from google.cloud import monitoring_v3
from google.cloud import compute_v1
from google.oauth2 import service_account
from datetime import datetime, timedelta
import hashlib
import socket
import logging
from app.utils.constants import TOKEN_URI, DAYS_LOOKBACK, PERIOD, NAMESPACE_GCP, NAMESPACE_AGENT, INSTANCE_TYPE, INSTANCE_NAME, MAX_MEM_USED, UTILIZATION, MAX_CPU, MAX_NW_BW, MAX_DISK_BW, MAX_IOPS, LevelType
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor
from app.utils.telemetry_utils import replace_spaces_with_plus, get_average, list_reservations, is_instance_reserved
from app.utils.telemetry_utils import get_average
from app.connections.pylogger import log_message

# Helper Functions
def get_credentials(private_key, client_email):
    formatted_private_key = replace_spaces_with_plus(private_key)
    if not is_private_key_valid(formatted_private_key):
        raise ValueError("Malformed private key incorrect BEGIN/END lines or trailing characters.")
    credentials = service_account.Credentials.from_service_account_info({
        "private_key": formatted_private_key,
        "client_email": client_email,
        "token_uri": TOKEN_URI
    })
    return credentials

def is_private_key_valid(private_key: str) -> bool:
    private_key = private_key.strip()
    return (
        private_key.startswith("-----BEGIN PRIVATE KEY-----") and
        private_key.endswith("-----END PRIVATE KEY-----") and
        private_key.count("-----BEGIN PRIVATE KEY-----") == 1 and
        private_key.count("-----END PRIVATE KEY-----") == 1 and
        private_key.split("-----END PRIVATE KEY-----")[-1].strip() == ""
    )
 
def get_zones(project_id, credentials, region):
    """Return only zones that belong to the hardcoded region."""
    zones_client = compute_v1.ZonesClient(credentials=credentials)
    zones = []
    request = compute_v1.ListZonesRequest(project=project_id)
    for zone in zones_client.list(request):
        if zone.name.startswith(region):
            zones.append(zone.name)
    return zones
 
def check_ops_agent_installed(client, project_id, instance_id, start_time, end_time):
    """Check if Ops Agent is installed by querying memory metric availability."""
    try:
        interval = monitoring_v3.TimeInterval(
            end_time={"seconds": int(end_time.timestamp())},
            start_time={"seconds": int(start_time.timestamp())}
        )
        request = monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project_id}",
            filter=f'metric.type = "agent.googleapis.com/memory/bytes_used" AND resource.labels.instance_id = "{instance_id}"',
            interval=interval,
            aggregation=monitoring_v3.Aggregation(
                alignment_period={"seconds": PERIOD},
                per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MAX
            )
        )
        results = client.list_time_series(request)
        # If any time series data is returned, Ops Agent is installed
        for _ in results:
            return True
        return False
    except Exception as err:
        log_message(LevelType.ERROR, f"Error checking Ops Agent for instance {instance_id}: {str(err)}", ErrorCode=-1)
        return False
 
def get_instances(project_id, credentials, monitoring_client, start_time, end_time, region):
    """Fetch running instances with Ops Agent installed."""
    compute_client = compute_v1.InstancesClient(credentials=credentials)
    instances = []
    zones = get_zones(project_id, credentials, region)
 
    for zone in zones:
        request = compute_v1.ListInstancesRequest(project=project_id, zone=zone)
        try:
            for instance in compute_client.list(request):
                if instance.status == "RUNNING":
                    if check_ops_agent_installed(monitoring_client, project_id, instance.id, start_time, end_time):
                        instances.append(instance)
        except Exception as err:
            log_message(LevelType.ERROR, f"Error listing instances in zone {zone}: {str(err)}", ErrorCode=-1)
    return instances
 
def get_metric(client, project_id, instance_id, metric_type, start_time, end_time, average=False, aligner="ALIGN_MAX"):
    try:
        interval = monitoring_v3.TimeInterval(
            end_time={"seconds": int(end_time.timestamp())},
            start_time={"seconds": int(start_time.timestamp())}
        )

        request = monitoring_v3.ListTimeSeriesRequest(
            name=f"projects/{project_id}",
            filter=f'metric.type = "{metric_type}" AND resource.labels.instance_id = "{instance_id}"',
            interval=interval,
            aggregation=monitoring_v3.Aggregation(
                alignment_period={"seconds": PERIOD},
                per_series_aligner=getattr(monitoring_v3.Aggregation.Aligner, aligner),
                cross_series_reducer=monitoring_v3.Aggregation.Reducer.REDUCE_MAX
            )
        )

        results = client.list_time_series(request)
        values = []
        found = False
        if "uptime_total" in metric_type:
            for time_series in results:
                for point in time_series.points:
                    return point.value.double_value or point.value.int64_value or 0
        for time_series in results:
            for point in time_series.points:
                val = point.value
                if hasattr(val, "double_value"):
                    value = getattr(val, "double_value", 0.0)
                elif hasattr(val, "int64_value"):
                    value = getattr(val, "int64_value", 0)
                else:
                    value = 0
                values.append(value)
                found = True
        if not found:
            return 0
        if average:
            return get_average(values)
        return max(values)
    except Exception as err:
        log_message(LevelType.ERROR, f"Error fetching {metric_type} for {instance_id}: {str(err)}", ErrorCode=-1)
        return 0
 
def calculate_uptime(client, project_id, instance_id, start_time, end_time):
    """Calculate uptime based on instance status."""
    try:
        metric_type = "compute.googleapis.com/instance/uptime_total"
        uptime_seconds = get_metric(client, project_id, instance_id, metric_type, start_time, end_time)
        uptime_hours = round(uptime_seconds / 3600, 2)
        return min(uptime_hours, 730) if uptime_hours > 0 else 730
    except Exception as err:
        log_message(LevelType.ERROR, f"Error calculating uptime for {instance_id}: {str(err)}", ErrorCode=-1)
        return 730
 
def update_cca_quantity_hours(cca_data, found, cca_entry, up_time):
    """Update CCA data with quantity and hours."""
    for idx, entry in enumerate(cca_data):
        if entry[INSTANCE_TYPE] == cca_entry[INSTANCE_TYPE] and entry["region"] == cca_entry["region"]:
            total_up_time = cca_entry["quantity"] * 730 if up_time > 730 * cca_entry["quantity"] or up_time <= 0 else up_time
            cca_data[idx]["quantity"] += 1
            cca_data[idx][UTILIZATION] += total_up_time
            cca_data[idx][UTILIZATION] = round(cca_data[idx][UTILIZATION], 2)
            found = True
            break
    return cca_data, found
 
# Main Metric Collection Function

def fetch_instance_metrics(instance_args):
    instance, monitoring_client, project_id, start_time, end_time, reservations_per_zone = instance_args
    try:
        instance_id = instance.id
        instance_name = instance.name
        instance_type = instance.machine_type.split("/")[-1]
        zone = instance.zone.split("/")[-1]
        region = "-".join(zone.split("-")[:-1])
        cloud_provider = "GCP"
        reservations = reservations_per_zone.get(zone, [])
        lifecycle = "spot" if instance.scheduling.preemptible else "ondemand"
        if is_instance_reserved(instance, reservations):
            lifecycle = "reserved"

        # CPU Metrics
        max_cpu = get_metric(
            monitoring_client, project_id, instance_id, "agent.googleapis.com/cpu/utilization", start_time, end_time
        )
        uavg, pavg, u95, p95 = get_metric(
            monitoring_client, project_id, instance_id, "agent.googleapis.com/cpu/utilization", start_time, end_time, average=True
        )

        # Memory
        max_mem = get_metric(
            monitoring_client, project_id, instance_id, "agent.googleapis.com/memory/bytes_used", start_time, end_time
        ) / (1024 ** 3)

        # Network
        net_received = get_metric(monitoring_client, project_id, instance_id,
            "compute.googleapis.com/instance/network/received_bytes_count", start_time, end_time, aligner="ALIGN_RATE")
        net_sent = get_metric(monitoring_client, project_id, instance_id,
            "compute.googleapis.com/instance/network/sent_bytes_count", start_time, end_time, aligner="ALIGN_RATE")
        max_net_bw = ((net_received + net_sent) * 8) / 1_000_000

        # Disk Bandwidth
        disk_read_bytes = get_metric(monitoring_client, project_id, instance_id,
            "agent.googleapis.com/disk/read_bytes_count", start_time, end_time, aligner="ALIGN_RATE")
        disk_write_bytes = get_metric(monitoring_client, project_id, instance_id,
            "agent.googleapis.com/disk/write_bytes_count", start_time, end_time, aligner="ALIGN_RATE")
        max_disk_bw = ((disk_read_bytes + disk_write_bytes) / (1024 * 1024))

        # IOPS
        disk_read_ops = get_metric(monitoring_client, project_id, instance_id,
            "compute.googleapis.com/instance/disk/read_ops_count", start_time, end_time, aligner="ALIGN_RATE")
        disk_write_ops = get_metric(monitoring_client, project_id, instance_id,
            "compute.googleapis.com/instance/disk/write_ops_count", start_time, end_time, aligner="ALIGN_RATE")
        max_iops = disk_read_ops + disk_write_ops

        # Uptime
        up_time = calculate_uptime(monitoring_client, project_id, instance_id, start_time, end_time)

        # EIA Entry
        uuid = f"{hashlib.md5(socket.getfqdn().encode()).hexdigest()}_{instance_name}"
        eia_entry = {
            "uuid": uuid,
            "cloud_csp": cloud_provider,
            INSTANCE_TYPE: instance_type,
            "region": region,
            MAX_CPU: round(max_cpu, 2),
            MAX_MEM_USED: round(max_mem, 2),
            MAX_NW_BW: round(max_net_bw, 2),
            MAX_DISK_BW: round(max_disk_bw, 2),
            MAX_IOPS: round(max_iops, 2),
            INSTANCE_NAME: instance_name,
            "pricingModel": lifecycle,
            "uavg": uavg,
            "u95": u95,
            "pavg": pavg,
            "p95": p95
        }

        # CCA Entry
        cca_entry = {
            "cloud_csp": cloud_provider,
            "region": region,
            INSTANCE_TYPE: instance_type,
            "quantity": 1,
            UTILIZATION: up_time,
            "pricingModel": lifecycle,
            INSTANCE_NAME: instance_name
        }

        return eia_entry, cca_entry, up_time
    except Exception as err:
        log_message(LevelType.ERROR, f"[Instance: {instance.name}] Metric collection failed: {str(err)}", ErrorCode=-1)
        return None, None, 0

def collect_metrics(private_key, client_email, project_id, region):
    credentials = get_credentials(private_key, client_email)
    monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)
    compute_client = compute_v1.InstancesClient(credentials=credentials)
    reservations_client = compute_v1.ReservationsClient(credentials=credentials)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=DAYS_LOOKBACK)

    zones = get_zones(project_id, credentials, region)

    instances = []
    reservations_per_zone = {}

    # Fetch all reservations and instances per zone
    for zone in zones:
        reservations_per_zone[zone] = list_reservations(reservations_client, project_id, zone)
        request = compute_v1.ListInstancesRequest(project=project_id, zone=zone)
        for instance in compute_client.list(request):
            if instance.status == "RUNNING":
                if check_ops_agent_installed(monitoring_client, project_id, instance.id, start_time, end_time):
                    instances.append(instance)

    eia_result, cca_result = [], []

    # Parallel metric collection
    with ThreadPoolExecutor(max_workers=10) as executor:
        args_list = [(instance, monitoring_client, project_id, start_time, end_time, reservations_per_zone) for instance in instances]
        results = executor.map(fetch_instance_metrics, args_list)

        for eia_entry, cca_entry, up_time in results:
            if eia_entry:
                eia_result.append(eia_entry)

            if cca_entry:
                found = False
                if cca_result:
                    cca_result, found = update_cca_quantity_hours(cca_result, found, cca_entry, up_time)
                if not found:
                    cca_result.append(cca_entry)

    return eia_result, cca_result

def collect_for_region(private_key, client_email, project_id, region):
    try:
        eia_result, cca_result = collect_metrics(private_key, client_email, project_id, region)
        return region, eia_result, cca_result, None
    except Exception as err:
        log_message(LevelType.ERROR, f"[GCP] Error collecting metrics in region {region}: {str(err)}", ErrorCode=-1)
        return region, [], [], str(e)

def check_gcp_telemetry_connection(private_key, client_email, project_id, regions, app_name):
    try:
        credentials = get_credentials(private_key, client_email)
        is_valid, error = validate_gcp_credentials(project_id, credentials)
        if not is_valid:
            log_message(LevelType.ERROR, f"Invalid GCP credentials: {error}", ErrorCode=-1)
            return False, "Invalid GCP credentials.", [], [], []
        eia_data = []
        cca_data = []
        instances_summary_list = []

        with ThreadPoolExecutor(max_workers=min(len(regions), 5)) as executor:
            futures = {executor.submit(collect_for_region, private_key, client_email, project_id, region): region for region in regions}
            for future in as_completed(futures):
                region, eia_result, cca_result, error = future.result()
                if error:
                    log_message(LevelType.ERROR, f"[GCP] Skipped region {region} due to error: {error}", ErrorCode=-1)
                    continue

                eia_data.extend(eia_result)
                cca_data.extend(cca_result)

                if app_name.upper() == "CCA":
                    for metrics in cca_result:
                        instances_summary_list.append({
                            INSTANCE_TYPE: metrics.get(INSTANCE_TYPE, ""),
                            INSTANCE_NAME: metrics.get(INSTANCE_NAME, ""),
                            "region": metrics.get("region", ""),
                            "pricingModel": metrics.get("pricingModel", "")
                        })

                if app_name.upper() == "EIA":
                    for metrics in eia_result:
                        instances_summary_list.append({
                            INSTANCE_TYPE: metrics.get(INSTANCE_TYPE, ""),
                            INSTANCE_NAME: metrics.get(INSTANCE_NAME, ""),
                            "region": metrics.get("region", ""),
                            "pricingModel": metrics.get("pricingModel", "")
                        })

        if instances_summary_list:
            return True, "GCP Telemetry connection is successful.", instances_summary_list, eia_data, cca_data

        if not regions:
            return False, f"Valid credentials, but no regions provided for GCP project '{project_id}'.", [], [], []

        return False, "No valid GCP telemetry instances found in the provided regions.", [], [], []

    except Exception as err:
        log_message(LevelType.ERROR, f"[GCP] Failed to connect to telemetry for project '{project_id}': {str(err)}", ErrorCode=-1)
        return False, "Invalid GCP credentials", [], [], []

def validate_gcp_credentials(project_id, credentials):
    try:
        zones_client = compute_v1.ZonesClient(credentials=credentials)
        list(zones_client.list(project=project_id))  # Will raise error if credentials are invalid
        return True, None
    except Exception as e:
        return False, str(e)    

def selected_instances(eia_result, cca_result, instances):
    eia_data = []
    cca_data = []
    if eia_result:
        for result in eia_result:
            instance_name = result.get(INSTANCE_NAME)
            if instance_name and instance_name in instances:
                eia_data.append({
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
        for result in cca_result:
            instance_name = result.get(INSTANCE_NAME)
            if instance_name and instance_name in instances:
                cca_data.append({
                    'cloud_csp': result.get("cloud_csp"),
                    'region': result.get("region"),
                    INSTANCE_TYPE: result.get(INSTANCE_TYPE),
                    'quantity': 1,
                    UTILIZATION: result.get("monthly utilization (hourly)"),
                    'pricingModel': result.get("pricingModel"),
                    'instance name': result.get(INSTANCE_NAME)
                })
    return eia_data, cca_data