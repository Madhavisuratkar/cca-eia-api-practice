import socket
import time
import hashlib
from datetime import datetime, timezone, timedelta
from itertools import zip_longest
from app.utils.constants import AMAZON_WEB_SERVICES, HOST, INSTANCE_TYPE, MONTHLY_UTILIZATION, AWS_MAX_CPU, MEM_USED, AWS_MAX_NET_IN, AWS_MAX_NET_OUT, AWS_EBS_READ, AZURE_NET_IN, AZURE_NET_OUT, AZURE_DISK_READ_BYTES, GCP_CPU, GCP_NET_REC, GCP_DISK_READ, GCP_DISK_READ_BYTE, LevelType
from app.utils.constants import AWS_READ_WRITE, AWS_DISK_READ, AWS_DISK_WRITE, AWS_EBS_READ_BYTES, AWS_EBS_WRITE_BYTES, AWS_DISK_READ_BYTES, AWS_DISK_WRITE_BYTES, AZURE_CPU, AZURE_DISK_READ, AZURE_DISK_WRITE, AZURE_DISK_WRITE_BYTES, GCP_NET_SENT, GCP_DISK_WRITE, GCP_DISK_WRITE_BYTE
from datadog import initialize, api
from app.utils.telemetry_utils import get_average
from app.connections.pylogger import log_message


def check_datadog_connection(api_key, app_key, api_host, provider):
    """Test the Datadog connection by fetching basic host data."""
    try:
        options = {
            'api_key': api_key,
            'app_key': app_key,
            'api_host': api_host
        }
        initialize(**options)
        response = api.Hosts.search()
        if 'host_list' in list(response):
            return True, "Datadog connection is successful.", response['host_list']
        else:
            log_message(LevelType.ERROR, f"Unable to connect to Datadog: {response}", ErrorCode=-1)
            if 'status' in response:
                return False, "Provided credentials are incorrect", None
            return False, "Unable to connect to Datadog", None

    except Exception as err:
        log_message(LevelType.ERROR, f"unable to connect to Datadog: {str(err)}", ErrorCode=-1)
        return False, "Unable to connect to datadog", None

def get_datadog_aws_hosts(region, data):
    """Get the Datadog host data for aws."""
    region_tag = f'region:{region}' if region else None
    host_data = []
    for entry in data:
        tags_by_source = entry.get("tags_by_source", {})
        aws_tags = tags_by_source.get(AMAZON_WEB_SERVICES, [])

        if (region_tag is None or region_tag in aws_tags) and "Datadog" in tags_by_source and "agent" in entry['sources']:
            region_value = next((tag.split(":")[1] for tag in aws_tags if tag.startswith("region:")), None)
            instance_type_value = next((tag.split(":")[1] for tag in aws_tags if tag.startswith("instance-type:")), None)
            instance_name = next((tag.split(":")[1] for tag in aws_tags if tag.startswith("name:")), "")
            
            # --- Determine correct host_name ---
            host_name = None
            host_name_host = entry.get("host_name")
            host_name_name = entry.get("name")

            # Prefer the one that starts with "ip-"
            if isinstance(host_name_host, str) and host_name_host.startswith("ip-"):
                host_name = host_name_host
            elif isinstance(host_name_name, str) and host_name_name.startswith("ip-"):
                host_name = host_name_name

            # Skip if no valid host_name found
            if not host_name:
                continue
            # if region_value and instance_type_value and entry.get("host_name").startswith("ip-"):
            if region_value and instance_type_value:
                host_data.append({
                    "host_id": entry.get("aws_id"),
                    "host_name": host_name,
                    "region": region_value,
                    INSTANCE_TYPE: instance_type_value,
                    "instance name": instance_name
                })
    return host_data

def get_datadog_azure_hosts(region, data):
    """Get the Datadog host data for azure."""
    region_tag = f'region:{region}' if region else None
    host_data = []
    for entry in data:
        tags_by_source = entry.get("tags_by_source", {})
        azure_tags = tags_by_source.get("Azure", [])

        if (region_tag is None or region_tag in azure_tags) and "Azure" in tags_by_source and "agent" in entry["sources"]:
            region_value = next((tag.split(":", 1)[1] for tag in azure_tags if tag.startswith("region:")), None)
            instance_type_value = next((tag.split(":", 1)[1] for tag in azure_tags if tag.startswith("size:")), None)
            instance_name = next((tag.split(":", 1)[1] for tag in azure_tags if tag.startswith("name:")), "")

            if region_value and instance_type_value:
                host_data.append({
                    "host_id": entry.get("aliases")[1] if len(entry.get("aliases")) > 1 else entry.get("aliases")[0],
                    "host_name": entry.get("host_name"),
                    "region": region_value,
                    INSTANCE_TYPE: instance_type_value,
                    "instance name": instance_name
                })

    return host_data

def get_datadog_gcp_hosts(region, data):
    """Get the Datadog host data for gcp."""
    region_tag = f'region:{region}' if region else None
    host_data = []

    for entry in data:
        tags_by_source = entry.get("tags_by_source", {})
        gcp_tags = tags_by_source.get("Google Cloud Platform", [])

        if (region_tag is None or region_tag in gcp_tags) and "Google Cloud Platform" in tags_by_source and "agent" in entry['sources']:
            region_value = next((tag.split(":", 1)[1] for tag in gcp_tags if tag.startswith("region:")), None)
            instance_type_value = next((tag.split(":", 1)[1] for tag in gcp_tags if tag.startswith("instance-type:")), None)
            instance_name = next((tag.split(":", 1)[1] for tag in gcp_tags if tag.startswith("name:")), "")
            instance_id = next((tag.split(":", 1)[1] for tag in gcp_tags if tag.startswith("instance-id:")), None)
            
            if region_value and instance_type_value:
                host_data.append({
                    "host_id": instance_id,
                    "host_name": entry.get("host_name"),
                    "region": region_value,
                    INSTANCE_TYPE: instance_type_value,
                    "instance name": instance_name
                })

    return host_data

def datadog_hosts(data, regions, provider):
    """Commaon function to get the Datadog host data based on provider."""
    try:
        host_data = []
        provider_function_map = {
            'aws': get_datadog_aws_hosts,
            'azure': get_datadog_azure_hosts,
            'gcp': get_datadog_gcp_hosts
        }

        fetch_func = provider_function_map.get(provider.lower())
        if not fetch_func:
            return [], f"Unsupported provider: {provider}"

        region_list = regions or [None]
        for region in region_list:
            host_data.extend(fetch_func(region, data))

        if not host_data:
            return [], "No host data is available"
        return host_data, "Host data fetched successfully"

    except Exception as err:
        log_message(LevelType.ERROR, f"unable to connect to Datadog: {str(err)}", ErrorCode=-1)
        return [], "Unable to connect to Datadog"


def get_queries(host_tags, provider):
    """Commaon function to get the metrics queries based on provider."""
    queries = []
    for host_tag in host_tags:
        if provider.lower() == 'aws':
            queries.extend([
                f'{AWS_MAX_CPU}{{{host_tag}}}',
                f'{MEM_USED}{{{host_tag}}}',
                f'{AWS_MAX_NET_IN}{{{host_tag}}}',
                f'{AWS_MAX_NET_OUT}{{{host_tag}}}',
                f'{AWS_EBS_READ}{{{host_tag}}}',
                f'{AWS_READ_WRITE}{{{host_tag}}}',
                f'{AWS_DISK_READ}{{{host_tag}}}',
                f'{AWS_DISK_WRITE}{{{host_tag}}}',
                f'{AWS_EBS_READ_BYTES}{{{host_tag}}}',
                f'{AWS_EBS_WRITE_BYTES}{{{host_tag}}}',
                f'{AWS_DISK_READ_BYTES}{{{host_tag}}}',
                f'{AWS_DISK_WRITE_BYTES}{{{host_tag}}}'
            ])
        elif provider.lower() == 'azure':
            queries.extend([
                f'{AZURE_CPU}{{{host_tag}}}',
                f'{MEM_USED}{{{host_tag}}}',
                f'{AZURE_NET_IN}{{{host_tag}}}',
                f'{AZURE_NET_OUT}{{{host_tag}}}',
                f'{AZURE_DISK_READ}{{{host_tag}}}',
                f'{AZURE_DISK_WRITE}{{{host_tag}}}',
                f'{AZURE_DISK_READ_BYTES}{{{host_tag}}}',
                f'{AZURE_DISK_WRITE_BYTES}{{{host_tag}}}'
            ])
        else:
            queries.extend([
                f'{GCP_CPU}{{{host_tag}}}',
                f'{MEM_USED}{{{host_tag}}}',
                f'{GCP_NET_REC}{{{host_tag}}}',
                f'{GCP_NET_SENT}{{{host_tag}}}',
                f'{GCP_DISK_READ}{{{host_tag}}}',
                f'{GCP_DISK_WRITE}{{{host_tag}}}',
                f'{GCP_DISK_READ_BYTE}{{{host_tag}}}',
                f'{GCP_DISK_WRITE_BYTE}{{{host_tag}}}'
            ])
    return ",".join(queries)

def fetch_instance_metadata(host_details, provider):
    """Commaon function to get the Datadog instance metadata data based on provider."""
    if not host_details or not isinstance(host_details, dict):
        return None, None, None, None

    try:
        tags_by_source = host_details.get("tags_by_source", {})

        if provider.lower() == 'aws' and AMAZON_WEB_SERVICES in tags_by_source:
            tags = tags_by_source[AMAZON_WEB_SERVICES]
            tag_dict = dict(tag.split(":", 1) for tag in tags if ":" in tag)
            return (
                tag_dict.get("cloud_provider"),
                tag_dict.get("instance-type"),
                tag_dict.get("region"),
                tag_dict.get("name")
            )

        elif provider.lower() == 'azure' and "Azure" in tags_by_source:
            tags = tags_by_source["Azure"]
            tag_dict = dict(tag.split(":", 1) for tag in tags if ":" in tag)
            return (
                tag_dict.get("cloud_provider"),
                tag_dict.get("size"),
                tag_dict.get("region"),
                tag_dict.get("name")
            )

        elif provider.lower() == 'gcp' and "Google Cloud Platform" in tags_by_source:
            tags = tags_by_source["Google Cloud Platform"]
            tag_dict = dict(tag.split(":", 1) for tag in tags if ":" in tag)
            return (
                tag_dict.get("cloud_provider"),
                tag_dict.get("instance-type"),
                tag_dict.get("region"),
                tag_dict.get("name")
            )

    except (AttributeError, TypeError, ValueError) as err:
        log_message(LevelType.ERROR, f"Unable to fetch instance metadata for provider '{provider}': {str(err)}", ErrorCode=-1)
        return None, None, None, None

def aws_get_metrics(host_tags, start_time_, end_time_, provider):
    """Get the instance metrics data for aws."""
    retries = 15
    delay = 1
    chunk_size = 10
    chunks = [host_tags[i:i + chunk_size] for i in range(0, len(host_tags), chunk_size)]
    max_metrics = {}
    try:
        for chunk in chunks:
            for attempt in range(retries):
                metrics = [
                    AWS_MAX_CPU, MEM_USED,
                    AWS_MAX_NET_IN, AWS_MAX_NET_OUT,
                    AWS_EBS_READ, AWS_READ_WRITE,
                    AWS_DISK_READ, AWS_DISK_WRITE,
                    AWS_EBS_READ_BYTES, AWS_EBS_WRITE_BYTES,
                    AWS_DISK_READ_BYTES, AWS_DISK_WRITE_BYTES
                ]
                response = api.Metric.query(start=start_time_, end=end_time_, query=get_queries(chunk, provider), verify=False)
                if 'series' not in response:
                    if attempt < retries - 1:
                        time.sleep(delay)
                        continue
                    else:
                        return {}
                responses = parse_response(response, metrics)
                for host_tag in chunk:
                    max_metrics[host_tag] = calculate_aws_metrics(responses, host_tag)
                break
        return max_metrics            
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch instance metrics data. {str(err)}", ErrorCode=-1)
        return {}
    
def azure_get_metrics(host_tags, start_time_, end_time_, provider):
    """Get the instance metrics data for azure."""
    retries = 15
    delay = 1
    chunk_size = 13
    chunks = [host_tags[i:i + chunk_size] for i in range(0, len(host_tags), chunk_size)]
    max_metrics = {}
    metrics = [AZURE_CPU,
            MEM_USED,
            AZURE_NET_IN,
            AZURE_NET_OUT,
            AZURE_DISK_READ,
            AZURE_DISK_WRITE,
            AZURE_DISK_READ_BYTES,
            AZURE_DISK_WRITE_BYTES]
    for chunk in chunks:
        for attempt in range(retries):
            response = api.Metric.query(start=start_time_, end=end_time_, query=get_queries(chunk, provider), verify=False)
            if 'series' not in response:
                if attempt < retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    return None, None, None, None, None
            responses = {metric: {} for metric in metrics}
            for series in response["series"]:
                metric_name = series["metric"]
                scope = series["scope"]
                host_tag = scope
                if metric_name in responses:
                    if host_tag not in responses[metric_name]:
                        responses[metric_name][host_tag] = []
                    responses[metric_name][host_tag].extend([(t, v) for t, v in series["pointlist"]])
            for host_tag in chunk:
                max_metrics[host_tag] = calculate_azure_metrics(responses, host_tag)
            break
    return max_metrics

def gcp_get_metrics(host_tags, start_time_, end_time_, provider):
    """Get the instance metrics data for gcp."""
    retries = 15
    delay = 1
    chunk_size = 13
    chunks = [host_tags[i:i + chunk_size] for i in range(0, len(host_tags), chunk_size)]
    max_metrics = {}
    metrics = [GCP_CPU,
            MEM_USED,
            GCP_NET_REC,
            GCP_NET_SENT,
            GCP_DISK_READ,
            GCP_DISK_WRITE,
            GCP_DISK_READ_BYTE,
            GCP_DISK_WRITE_BYTE]
    for chunk in chunks:
        for attempt in range(retries):
            response = api.Metric.query(start=start_time_, end=end_time_, query=get_queries(chunk, provider), verify=False)
            if 'series' not in response:
                if attempt < retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    return None, None, None, None, None
            responses = {metric: {} for metric in metrics}
            for series in response["series"]:
                metric_name = series["metric"]
                scope = series["scope"]
                host_tag = scope
                if metric_name in responses:
                    if host_tag not in responses[metric_name]:
                        responses[metric_name][host_tag] = []
                    responses[metric_name][host_tag].extend([(t, v) for t, v in series["pointlist"]])
            for host_tag in chunk:
                max_metrics[host_tag] = calculate_gcp_metrics(responses, host_tag)
            break
    return max_metrics

def parse_response(response, metrics):
    responses = {metric: {} for metric in metrics}
    for series in response["series"]:
        metric_name = series["metric"]
        scope = series["scope"]
        host_tag = scope
        if metric_name in responses:
            if host_tag not in responses[metric_name]:
                responses[metric_name][host_tag] = []
            responses[metric_name][host_tag].extend([(t, v) for t, v in series["pointlist"]])
    return responses

def calculate_aws_metrics(responses, host_tag):
    """Calcutaion of instance metrics data for aws."""
    max_cpu_util = max((v for t, v in responses[AWS_MAX_CPU].get(host_tag, []) if v is not None), default=0)
    max_mem_used = max((v for t, v in responses[MEM_USED].get(host_tag, []) if v is not None), default=0)
    
    cpu_values = [v for t, v in responses[AWS_MAX_CPU].get(host_tag, []) if v is not None]
    
    uavg, _, u95, _ = get_average(cpu_values)
    max_net_bw = max(
        (
            (in_val or 0) + (out_val or 0)
            for (_, in_val), (_, out_val) in zip_longest(
                responses.get(AWS_MAX_NET_IN, {}).get(host_tag, []),
                responses.get(AWS_MAX_NET_OUT, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_iops = max(
        (
            ((ebs_read or 0)/300) + ((ebs_write or 0)/300) + ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, ebs_read), (_, ebs_write), (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(AWS_EBS_READ, {}).get(host_tag, []),
                responses.get(AWS_READ_WRITE, {}).get(host_tag, []),
                responses.get(AWS_DISK_READ, {}).get(host_tag, []),
                responses.get(AWS_DISK_WRITE, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_bw = max(
        (
            ((ebs_read or 0)/300) + ((ebs_write or 0)/300) + ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, ebs_read), (_, ebs_write), (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(AWS_EBS_READ_BYTES, {}).get(host_tag, []),
                responses.get(AWS_EBS_WRITE_BYTES, {}).get(host_tag, []),
                responses.get(AWS_DISK_READ_BYTES, {}).get(host_tag, []),
                responses.get(AWS_DISK_WRITE_BYTES, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )

    return {
        'max_cpu_util': max_cpu_util,
        'max_mem_used': max_mem_used,
        'max_net_bw': max_net_bw,
        'max_disk_iops': max_disk_iops,
        'max_disk_bw': max_disk_bw,
        'uavg': uavg,
        'u95': u95,
        # 'pavg': pavg,
        # 'p95': p95
    }
    
def calculate_azure_metrics(responses, host_tag):
    """Calcutaion of instance metrics data for azure."""
    max_cpu_util = max((v for t, v in responses[AZURE_CPU].get(host_tag, []) if v is not None), default=0)
    max_mem_used = max((v for t, v in responses[MEM_USED].get(host_tag, []) if v is not None), default=0)
    
    cpu_values = [v for t, v in responses[AZURE_CPU].get(host_tag, []) if v is not None]

    uavg, _, u95, _ = get_average(cpu_values)
    max_net_bw = max(
        (
            (in_val or 0) + (out_val or 0)
            for (_, in_val), (_, out_val) in zip_longest(
                responses.get(AZURE_NET_IN, {}).get(host_tag, []),
                responses.get(AZURE_NET_OUT, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_iops = max(
        (
            ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(AZURE_DISK_READ, {}).get(host_tag, []),
                responses.get(AZURE_DISK_WRITE, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_bw = max(
        (
            ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(AZURE_DISK_READ_BYTES, {}).get(host_tag, []),
                responses.get(AZURE_DISK_WRITE_BYTES, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    return {
            'max_cpu_util': max_cpu_util,
            'max_mem_used': max_mem_used,
            'max_net_bw': max_net_bw,
            'max_disk_iops': max_disk_iops,
            'max_disk_bw': max_disk_bw,
            'uavg': uavg,
            'u95': u95,
            # 'pavg': pavg,
            # 'p95': p95
        }
    
def calculate_gcp_metrics(responses, host_tag):
    """Calcutaion of instance metrics data for gcp."""
    max_cpu_util = max((v for t, v in responses[GCP_CPU].get(host_tag, []) if v is not None), default=0)
    max_mem_used = max((v for t, v in responses[MEM_USED].get(host_tag, []) if v is not None), default=0)
    
    cpu_values = [v for t, v in responses[GCP_CPU].get(host_tag, []) if v is not None]

    uavg, _, u95, _ = get_average(cpu_values)
    max_net_bw = max(
        (
            (in_val or 0) + (out_val or 0)
            for (_, in_val), (_, out_val) in zip_longest(
                responses.get(GCP_NET_REC, {}).get(host_tag, []),
                responses.get(GCP_NET_SENT, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_iops = max(
        (
            ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(GCP_DISK_READ, {}).get(host_tag, []),
                responses.get(GCP_DISK_WRITE, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    max_disk_bw = max(
        (
            ((disk_read or 0)/60) + ((disk_write or 0)/60)
            for (_, disk_read), (_, disk_write) in zip_longest(
                responses.get(GCP_DISK_READ_BYTE, {}).get(host_tag, []),
                responses.get(GCP_DISK_WRITE_BYTE, {}).get(host_tag, []),
                fillvalue=(0, 0)
            )
        ),
        default=0
    )
    return {
            'max_cpu_util': max_cpu_util,
            'max_mem_used': max_mem_used,
            'max_net_bw': max_net_bw,
            'max_disk_iops': max_disk_iops,
            'max_disk_bw': max_disk_bw,
            'uavg': uavg,
            'u95': u95,
            # 'pavg': pavg,
            # 'p95': p95
        }

def get_datadog_uptime(series, host_tag, uptime_values):
    host_series = [s for s in series if f"{host_tag}" in s["scope"]]
    if host_series and host_series[0]["pointlist"]:
        pointlist = host_series[0]["pointlist"]
        uptime_hours = [(timestamp, (value or 0) / 3600) for timestamp, value in pointlist]
        if not uptime_hours:
            uptime_values[host_tag] = 730
        elif len(uptime_hours) == 1:
            uptime_values[host_tag] = round(uptime_hours[0][1], 2) if uptime_hours[0][1] < 730 else 730
        else:
            uptime_values = get_uptime(uptime_hours, uptime_values, host_tag)
    else:
        uptime_values[host_tag] = None
    return uptime_values

def get_uptime(uptime_hours, uptime_values,host_tag):
    total_difference = 0
    first_value = uptime_hours[0][1]
    current_max = first_value
    for i in range(1, len(uptime_hours)):
        _, uptime = uptime_hours[i]
        _, prev_uptime = uptime_hours[i - 1]
        if uptime > prev_uptime:
            current_max = max(current_max, uptime)
        else:
            total_difference += current_max - first_value
            first_value = uptime
            current_max = uptime
    if current_max > first_value:
        total_difference += current_max - first_value
    uptime_values[host_tag] = round(total_difference, 2) if total_difference < 730 else 730
    return uptime_values

def get_uptime_values_in_hours(host_tags):
    """Get the uptime of datadog instance in hours."""
    retries = 15
    delay = 1
    chunk_size = 120
    chunks = [host_tags[i:i + chunk_size] for i in range(0, len(host_tags), chunk_size)]
    uptime_values = {}
    try:
        for chunk in chunks:
            for attempt in range(retries):
                queries = [f"system.uptime{{{host_tag}}}" for host_tag in chunk]
                from_time = int((datetime.now(timezone.utc) - timedelta(hours=730)).timestamp())
                to_time = int(datetime.now(timezone.utc).timestamp())
                response = api.Metric.query(start=from_time, end=to_time, query=",".join(queries))
                if 'series' not in response:
                    if attempt < retries - 1:
                        time.sleep(delay)
                        continue
                    else:
                        return {}
                series = response.get("series", [])
                for host_tag in chunk:
                    uptime_values = get_datadog_uptime(series, host_tag, uptime_values)
                break
        return uptime_values
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to get instance metrics data. {str(err)}", ErrorCode=-1)
        return {}


def fetch_data(app_name, host_tags, datadog_host_details, start_time_, end_time_, provider):
    """Main function to get the datadog instances for all providers."""
    cca_data, eia_data = [], []
    if provider == "azure" and app_name.lower() == 'eia':
        max_metrics = azure_get_metrics(host_tags, start_time_, end_time_, provider)
    elif provider == "aws" and app_name.lower() == 'eia':
        max_metrics = aws_get_metrics(host_tags, start_time_, end_time_, provider)
    elif provider == "gcp" and app_name.lower() == 'eia':
        max_metrics = gcp_get_metrics(host_tags, start_time_, end_time_, provider)
    else:
        uptime_values = get_uptime_values_in_hours(host_tags)
    for host_tag in host_tags:
        host_metadata = get_host_metadata(host_tag, datadog_host_details)
        if provider in ["azure", "aws", "gcp"]:
            cloud_provider, instance_type, region,instance_name = fetch_instance_metadata(host_metadata, provider)
        if app_name.lower() == 'eia':
            eia_data.append(collect_eia_data(host_tag, cloud_provider, instance_type, region, max_metrics, instance_name))
        else:
            up_time = uptime_values[host_tag] if uptime_values[host_tag] else 0
            cca_data = update_cca_data(cca_data, cloud_provider, instance_type, region, up_time, instance_name)

    return cca_data, eia_data

def get_host_metadata(host_tag, datadog_host_details):
    return next((h for h in datadog_host_details if h.get('name') == host_tag.removeprefix(HOST)), None)

def collect_eia_data(host_tag, cloud_provider, instance_type, region, max_metrics, instance_name):
    """Collect the eia datadog instances and formatting the response."""
    max_cpu_util = max_metrics[host_tag]['max_cpu_util']
    max_mem_used = max_metrics[host_tag]['max_mem_used']
    max_net_bw = max_metrics[host_tag]['max_net_bw']
    max_disk_iops = max_metrics[host_tag]['max_disk_iops']
    max_disk_bw = max_metrics[host_tag]['max_disk_bw']
    uavg = max_metrics[host_tag]['uavg']
    u95 = max_metrics[host_tag]['u95']
    return {
        'uuid': f"{hashlib.md5(socket.getfqdn().encode()).hexdigest()}_{host_tag.strip(HOST)}",
        'cloud_csp': cloud_provider.upper() if cloud_provider else None,
        INSTANCE_TYPE: instance_type if instance_type else None,
        'instance name': instance_name if instance_name else "",
        'region': region if region else None,
        'max cpu%': round(max_cpu_util, 2) if max_cpu_util is not None else None,
        'max mem used': round(float(max_mem_used) / (1024 ** 3), 2) if max_mem_used is not None else None,
        'max network bw': round(((float((max_net_bw * 8)) / 60) / 1_000_000), 2) if max_net_bw is not None else None,
        'max disk bw used': round((float(max_disk_bw)) / (1024 * 1024), 2) if max_disk_bw is not None else None,
        'max iops': round(float(max_disk_iops), 2) if max_disk_iops is not None else None,
        'pricingModel': 'ondemand',
        'uavg': uavg,
        'u95': u95,
        # 'pavg': pavg,
        # 'p95': p95
    }

def update_cca_quantity_hours(cca_data, found, cca_entry, up_time):
    for idx, entry in enumerate(cca_data):
        if entry['instance type'] == cca_entry['instance type'] and entry['region'] == cca_entry["region"]:
            total_up_time = cca_entry['quantity'] * 730 if up_time > 730 * cca_entry['quantity'] or up_time<=0 else up_time
            cca_data[idx]['quantity'] += 1
            cca_data[idx][MONTHLY_UTILIZATION] += total_up_time
            cca_data[idx][MONTHLY_UTILIZATION] = round(cca_data[idx][MONTHLY_UTILIZATION],2)
            found = True
            break
    return cca_data, found

def update_cca_data(cca_data, cloud_provider, instance_type, region, up_time, instance_name):
    """Collect the cca datadog instances and formatting the response."""
    cca_entry = {
        'cloud_csp': cloud_provider.upper() if cloud_provider else None,
        'region': region if region else None,
        INSTANCE_TYPE: instance_type if instance_type else None,
        'quantity': 1,
        'monthly utilization (hourly)': up_time,
        'pricingModel': 'ondemand',
        'instance name': instance_name if instance_name else ""
    }
    found = False
    if cca_entry:
        cca_data, found = update_cca_quantity_hours(cca_data, found, cca_entry, up_time)
    if not found:
        cca_data.append(cca_entry)
    return cca_data


def find_items_with_null_values(data):
    uuids_with_null = []
    for record in data:
        for key, value in record.items():
            if value is None:
                uuids_with_null.append(record["uuid"].split('_')[1])
                break
    return uuids_with_null