import logging
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.mgmt.loganalytics import LogAnalyticsManagementClient
import uuid
from app.connections.pylogger import log_message
from app.utils.constants import DAYS_BACK, METRIC_NAMESPACE, METRIC_MAP, INSTANCE_TYPE, INSTANCE_NAME, MAX_MEM_USED, UTILIZATION, MAX_CPU, MAX_NW_BW, MAX_DISK_BW, MAX_IOPS, MAX_WORKERS, LevelType
from tqdm import tqdm
import concurrent.futures
from app.utils.telemetry_utils import get_average


# ------------------ CONFIGURATION ------------------
end_time = datetime.utcnow()
start_time = end_time - timedelta(days=DAYS_BACK)
timespan = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"

# ------------------ WORKSPACE DETECTION ------------------
def build_workspace_vm_map(workspaces, logs_client):
    workspace_vm_map = {}
    for workspace in tqdm(workspaces, desc="Mapping VMs to workspaces"):
        try:
            query = """
                Heartbeat
                | where TimeGenerated >= ago(1h)
                | summarize by Computer
            """
            response = logs_client.query_workspace(
                workspace_id=workspace.customer_id,
                query=query,
                timespan=(datetime.utcnow() - timedelta(hours=1), datetime.utcnow())
            )
            if response.tables and response.tables[0].rows:
                for row in response.tables[0].rows:
                    workspace_vm_map[row[0]] = workspace.customer_id
        except Exception as err:
            log_message(LevelType.ERROR, f"error in build_workspace_vm_map : {str(err)}", ErrorCode=-1)
            continue
    return workspace_vm_map

# ------------------ METRIC FETCHERS ------------------
def fetch_metric(resource_id, metric_name, monitor_client, average=False):
    result = monitor_client.metrics.list(
        resource_id,
        timespan=timespan,
        interval="PT5M",
        metricnames=metric_name,
        aggregation="Maximum",
        metricnamespace=METRIC_NAMESPACE
    )
    max_vals = [
        dp.maximum for m in result.value for ts in m.timeseries for dp in ts.data if dp.maximum is not None
    ]
    if average and metric_name == "Percentage CPU":
        return get_average(max_vals)
    return max(max_vals) if max_vals else 0.0

def fetch_memory_used_mb(vm_name, workspace_id, logs_client, total_memory_mb=8192):
    try:
        query = f"""
        Perf
        | where TimeGenerated >= ago(1h)
        | where ObjectName == "Memory"
        | where CounterName == "Available MBytes Memory"
        | where Computer == '{vm_name}'
        | summarize avg_available = avg(CounterValue)
        | extend mem_used_mb = round({total_memory_mb} - avg_available, 2)
        | project mem_used_mb
        """
        response = logs_client.query_workspace(
            workspace_id=workspace_id,
            query=query,
            timespan=(datetime.utcnow() - timedelta(hours=1), datetime.utcnow())
        )
        log_message(LevelType.INFO, f"fetch_memory_used_mb response: {response}", ErrorCode=1)
        if response.tables and response.tables[0].rows:
            return float(response.tables[0].rows[0][0])
        else:
            return None
    except Exception as err:
        log_message(LevelType.ERROR, f"error in fetch_memory_used_mb : {str(err)}", ErrorCode=-1)
        return None

def fetch_uptime_hours(vm_computer_name, workspace_id, logs_client):
    query = f"""
        Heartbeat
        | where TimeGenerated >= ago({DAYS_BACK}d)
        | where Computer == '{vm_computer_name}'
        | summarize heartbeat_count = count()
    """
    try:
        response = logs_client.query_workspace(
            workspace_id=workspace_id,
            query=query,
            timespan=(start_time, end_time)
        )
        if response.status != LogsQueryStatus.SUCCESS or not response.tables[0].rows:
            return 0
        heartbeat_count = int(response.tables[0].rows[0][0])
        uptime_seconds = heartbeat_count * 60
        return round(uptime_seconds / 3600, 2)
    except Exception as err:
        log_message(LevelType.ERROR, f"error in fetch_uptime_hours : {str(err)}", ErrorCode=-1)
        return 0
    
def update_cca_quantity_hours(cca_data, cca_entry, up_time):
    for idx, entry in enumerate(cca_data):
        if entry[INSTANCE_TYPE] == cca_entry[INSTANCE_TYPE] and entry['region'] == cca_entry["region"] and entry['instance_name'] == cca_entry['instance_name']:
            total_up_time = cca_entry['quantity'] * 730 if up_time > 730 * cca_entry['quantity'] or up_time <= 0 else up_time
            cca_data[idx]['quantity'] += 1
            cca_data[idx][UTILIZATION] += total_up_time
            cca_data[idx][UTILIZATION] = round(cca_data[idx][UTILIZATION], 2)
            return cca_data
    cca_data.append(cca_entry)
    return cca_data

# ------------------ METRIC AGGREGATOR ------------------
def collect_metrics_for_vm(vm, workspace_vm_map, monitor_client, logs_client, regions):
    if vm.location.lower() not in [region.lower() for region in regions]:
        return None

    workspace_id = workspace_vm_map.get(vm.name)
    if not workspace_id:
        return None
    
    resource_id = vm.id
    vm_name = vm.name
    pricing_model = "ondemand"
    raw_metrics = {label: fetch_metric(resource_id, metric, monitor_client) for metric, label in METRIC_MAP.items()}
    uavg, pavg, u95, p95 = fetch_metric(resource_id, "Percentage CPU", monitor_client, True)
    max_net_bw_mbps = round(((raw_metrics["net_in"] + raw_metrics["net_out"]) * 8) / 60 / 1_000_000, 2)
    max_disk_bw_mibps = round(((raw_metrics["disk_read_bytes"] + raw_metrics["disk_write_bytes"]) / 60) / (1024 * 1024), 2)
    max_disk_iops = round(raw_metrics["disk_read_ops"] + raw_metrics["disk_write_ops"], 2)

    mem_used_mb = fetch_memory_used_mb(vm_name, workspace_id, logs_client)
    uptime_hours = fetch_uptime_hours(vm_name, workspace_id, logs_client)
    
    if vm.priority and vm.priority.lower() == "spot":
        pricing_model = "spot"

    result = {
        "uuid": str(uuid.uuid4()),
        "cloud_csp": "AZURE",
        INSTANCE_TYPE: vm.hardware_profile.vm_size,
        INSTANCE_NAME: vm_name if vm_name else '',
        "region": vm.location,
        MAX_CPU: round(raw_metrics["max_cpu_percent"], 2),
        MAX_MEM_USED: round(mem_used_mb / 1024, 2),
        UTILIZATION: uptime_hours,
        MAX_NW_BW: max_net_bw_mbps,
        MAX_DISK_BW: max_disk_bw_mibps,
        MAX_IOPS: max_disk_iops,
        "pricingModel": pricing_model,
        "uavg": uavg,
        "u95": u95,
        "pavg": pavg,
        "p95": p95
    }
    
    return result


def check_azureinsights_connection(tenant_id, client_id, client_secret, subscription_id, regions):
    try:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        compute_client = ComputeManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        logs_client = LogsQueryClient(credential)
        log_analytics_client = LogAnalyticsManagementClient(credential, subscription_id)
        instances_summary_list = []
        all_workspaces = list(log_analytics_client.workspaces.list())
        workspace_vm_map = build_workspace_vm_map(all_workspaces, logs_client)
        vms = compute_client.virtual_machines.list_all()
        if not vms:
            log_message(LevelType.ERROR, "in check_azureinsights_connection vms is empty", ErrorCode=-1)
            return False, "Provided credentials are incorrect.", None, None
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(collect_metrics_for_vm, vm, workspace_vm_map, monitor_client, logs_client, regions): vm for vm in vms}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Collecting metrics"):
                try:
                    data = future.result()
                    if data:
                        results.append(data)
                except Exception as err:
                    log_message(LevelType.ERROR, f"Error in check_azureinsights_connection for data {data} :{str(err)}", ErrorCode=-1)
                    continue
        for metrics in  results:
            summary = {
                INSTANCE_TYPE: metrics.get(INSTANCE_TYPE, ""),
                INSTANCE_NAME: metrics.get(INSTANCE_NAME, ""),  # Note: "instnace" typo retained as per original, but ideally should be corrected.
                "region": metrics.get("region", ""),
                "pricingModel": metrics.get("pricingModel", "")
            }
            instances_summary_list.append(summary)

        if results:
            return True, "Azure Insights connection is successful.", instances_summary_list, results
        else:
            log_message(LevelType.ERROR, "Azure Insights connection failed: No running instances found", ErrorCode=-1)
            return False, "Unable to connect to Azure Insights: No running instances found.", None, None

    except Exception as err:
        log_message(LevelType.ERROR, f"Error in check_azureinsights_connection :{str(err)}", ErrorCode=-1)
        return False, "Unable to connect to Azure Insights", None, None


def separate_metrics(results, instances):
    eia_result = []
    cca_result = []

    if results:
        for result in results:
            instance_name = result.get(INSTANCE_NAME)
            if instance_name and instance_name in instances:
                eia_result.append({
                    "uuid": result.get("uuid"),
                    "cloud_csp": result.get("cloud_csp"),
                    INSTANCE_TYPE: result.get(INSTANCE_TYPE),
                    "instance name": result.get(INSTANCE_NAME),
                    "region": result.get("region"),
                    MAX_CPU: result.get(MAX_CPU),
                    MAX_MEM_USED: result.get(MAX_MEM_USED),
                    MAX_NW_BW: result.get(MAX_NW_BW),
                    MAX_DISK_BW: result.get(MAX_DISK_BW),
                    MAX_IOPS: result.get(MAX_IOPS),
                    'pricingModel': result.get("pricingModel"),
                    "uavg": result.get("uavg"),
                    "u95": result.get("u95"),
                    # "pavg": result.get("pavg"),
                    # "p95": result.get("p95")
                })

                cca_entry = {
                    'cloud_csp': result.get("cloud_csp"),
                    'region': result.get("region"),
                    INSTANCE_TYPE: result.get(INSTANCE_TYPE),
                    'quantity': 1,
                    UTILIZATION: result.get("monthly utilization (hourly)"),
                    'pricingModel': result.get("pricingModel"),
                    'instance name': result.get(INSTANCE_NAME)
                }
                cca_result = update_cca_quantity_hours(cca_result, cca_entry, result.get("monthly utilization (hourly)"))
            
    return eia_result, cca_result

