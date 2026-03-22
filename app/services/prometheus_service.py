import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from app.utils.constants import LevelType
from app.utils.telemetry_utils import get_average
from app.connections.pylogger import log_message
import uuid

def get_targets(resp, prometheus_job, regions):
    try:
        targets = resp.get("data", {}).get("activeTargets", [])
        if not targets:
            return False, "Invalid prometheus URL.", []

        # Match based on job name and instance state
        job_targets = [
            t for t in targets
            if t["discoveredLabels"].get("job") == prometheus_job and (
                (prometheus_job == "aws" and t["discoveredLabels"].get("__meta_ec2_instance_state") == "running") or
                (prometheus_job == "gcp" and t["discoveredLabels"].get("__meta_gce_instance_status") == "RUNNING") or
                (prometheus_job == "azure")
            )
        ]
       
        if not job_targets:
            return False, f"No running instances found for {prometheus_job.upper()}.", []

        up_targets = [t for t in job_targets if t.get("health") == "up"]
        if not up_targets:
            return False, f"No running instances found for {prometheus_job.upper()}.", []

        matched_targets = []
        for t in up_targets:
            labels = t["discoveredLabels"]

            if prometheus_job == "aws":
                region = labels.get("__meta_ec2_region", "unknown")
                if region in regions:
                    matched_targets.append({
                        "instance": labels.get("__meta_ec2_public_ip", labels.get("__meta_ec2_private_ip")),
                        "instance name": labels.get("__meta_ec2_tag_Name", "unknown"),
                        "instance type": labels.get("__meta_ec2_instance_type", "unknown"),
                        "region": region,
                        "pricingModel": labels.get("pricing_model", "ondemand")
                    })
            elif prometheus_job == "gcp":
                region = t["labels"].get("region", "").rsplit("-", 1)[0]
                if region in regions:
                    matched_targets.append({
                        "instance": labels.get("__meta_gce_public_ip", labels.get("__meta_gce_private_ip")),
                        "instance name": labels.get("__meta_gce_instance_name", "unknown"),
                        "instance type": labels.get("__meta_gce_machine_type", "").rsplit("/", 1)[-1],
                        "region": region,
                        "pricingModel": labels.get("pricing_model", "ondemand")
                    })
            elif prometheus_job == "azure":
                region = labels.get("__meta_azure_machine_location", "unknown")
                if region in regions:
                    matched_targets.append({
                        "instance": t["labels"].get("instance", labels.get("__address__")),
                        "instance name": labels.get("__meta_azure_machine_name", "unknown"),
                        "instance type": labels.get("__meta_azure_machine_size", "unknown"),
                        "region": region,
                        "pricingModel": labels.get("pricing_model", "ondemand")
                    })

        if not matched_targets:
            return False, f"No running instances found for {prometheus_job.upper()}.", []

        return True, "Prometheus connection is successful.", matched_targets

    except requests.exceptions.RequestException as err:
        log_message(LevelType.ERROR, f"Invalid prometheus URL {str(err)}", ErrorCode=-1)
        return False, "Invalid prometheus URL.", []
    except ValueError as err:
        log_message(LevelType.ERROR, f"Invalid prometheus URL {str(err)}", ErrorCode=-1)
        return False, "Invalid prometheus URL.", []
    except Exception as err:
        log_message(LevelType.ERROR, f"Invalid prometheus URL {str(err)}", ErrorCode=-1)
        return False, "Invalid prometheus URL.", []

def get_metrics(instance, prometheus_url, username, password):
    queries = {
        "cpu": f'(100 - rate(node_cpu_seconds_total{{mode="idle",instance="{instance}"}}[5m]) * 100)',
        "max mem used": f'max_over_time(((node_memory_MemTotal_bytes{{instance="{instance}"}} - node_memory_MemAvailable_bytes{{instance="{instance}"}}) / 1024 / 1024 / 1024)[24h:5m])',
        "max network bw": f'max_over_time(((rate(node_network_receive_bytes_total{{instance="{instance}"}}[5m]) + rate(node_network_transmit_bytes_total{{instance="{instance}"}}[5m])) * 8 / 1e6)[24h:5m])',
        "max disk bw used": f'max_over_time(((rate(node_disk_read_bytes_total{{instance="{instance}"}}[5m]) + rate(node_disk_written_bytes_total{{instance="{instance}"}}[5m])) / (1024 * 1024))[24h:5m])',
        "max iops": f'max_over_time((rate(node_disk_reads_completed_total{{instance="{instance}"}}[5m]) + rate(node_disk_writes_completed_total{{instance="{instance}"}}[5m]))[24h:5m])',
        "uptime_hours": f'(time() - node_boot_time_seconds{{instance="{instance}"}}) / 3600'
    }

    results = {}

    # 1. CPU max and average from query_range
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=24)
        params = {
            "query": queries["cpu"],
            "start": start.isoformat(),
            "end": end.isoformat(),
            "step": "300"
        }
        _, data = prom_get("/api/v1/query_range", prometheus_url, username, password, params=params, timeout=15)
        results_list = data.get("data", {}).get("result", [])
        if results_list:
            series = [float(v[1]) for v in results_list[0].get("values", [])]
        else:
            series = []
        if series:
            results["max cpu%"] = round(max(series), 2)
            results["uavg"], _, results["u95"], _ = get_average(series)
        else:
            results.update({"max cpu%": 0.0, "uavg": 0.0, "u95": 0.0})
    except Exception as err:
        log_message(LevelType.ERROR, f"Error in get_metrics msg as : {str(err)}", ErrorCode=-1)
        results.update({"max cpu%": 0.0, "uavg": 0.0, "u95": 0.0})

    # 2. All other queries (max only)
    for key in ["max mem used", "max network bw", "max disk bw used", "max iops", "uptime_hours"]:
        try:
            _, data = prom_get("/api/v1/query", prometheus_url, username, password, params=params, timeout=10)
            results_list = data.get("data", {}).get("result", [])
            if results_list:
                value = float(results_list[0].get("value", [0, 0])[1])
            else:
                value = 0.0
            results[key] = round(value, 2)
        except Exception as err:
            log_message(LevelType.ERROR, f"Error in get_metrics for key {key} msg as : {str(err)}", ErrorCode=-1)
            results[key] = 0.0

    return results

def collect(prometheus_url, region, provider, instances, username, password):
    _, response = prom_get("/api/v1/targets", prometheus_url, username, password, timeout=10)
    _, _, targets = get_targets(response, provider.lower(), region)
    if not targets:
        return [], []
    
    targets = [t for t in targets if t.get("instance name") in instances]

    eia_result = []
    cca_result = []

    def process_instance(t):
        metrics = get_metrics(t["instance"], prometheus_url, username, password)
        if provider.lower().startswith("gcp"):
            cloud_csp = "GCP"
        elif provider.lower().startswith("azure"):
            cloud_csp = "AZURE"
        else:
            cloud_csp = "AWS"
        
        capped_uptime = min(metrics["uptime_hours"], 730) if metrics["uptime_hours"] else 730
        metrics.pop("uptime_hours", None)
        uuid_val= f"{uuid.uuid4()}_{t['instance name']}"
        return {
            "eia": {
                "uuid": uuid_val,
                "cloud_csp": cloud_csp,
                "instance type": t["instance type"],
                "region": t["region"],
                **metrics,
                "pricingModel": t["pricingModel"],
                "instance name": t["instance name"]
            },
            "cca": {
                "instance type": t["instance type"],
                "region": t["region"],
                "up_time": capped_uptime,
                "pricingModel": t["pricingModel"],
                "instance name": t["instance name"]
            }
        }

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_instance, targets))

    for r in results:
        eia_result.append(r["eia"])
        matched = False
        if provider.lower().startswith("gcp"):
            cloud_csp = "GCP"
        elif provider.lower().startswith("azure"):
            cloud_csp = "AZURE"
        else:
            cloud_csp = "AWS"
        for entry in cca_result:
            if entry["instance type"] == r["cca"]["instance type"] and entry["region"] == r["cca"]["region"] and entry["instance name"] == r["cca"]["instance name"]:
                entry["quantity"] += 1
                entry["monthly utilization (hourly)"] += r["cca"]["up_time"]
                matched = True
                break
        if not matched:
            cca_result.append({
                "cloud_csp": cloud_csp,
                "region": r["cca"]["region"],
                "instance type": r["cca"]["instance type"],
                "quantity": 1,
                "monthly utilization (hourly)": r["cca"]["up_time"],
                "pricingModel": r["cca"]["pricingModel"],
                "instance name": r["cca"]["instance name"]
            })

    return eia_result, cca_result

def prom_get(path, prometheus_url, username, password, params=None, timeout=10):
    """Wrapper for Prometheus GET requests with Basic Auth."""
    try:
        resp = requests.get(
            f"{prometheus_url}{path}",
            params=params,
            auth=HTTPBasicAuth(username, password),
            timeout=timeout
        )
        resp.raise_for_status()
        return True, resp.json()
    except requests.exceptions.RequestException as e:
        log_message(LevelType.ERROR, f"Prometheus API error: {e}", ErrorCode=-1)
        return False, {}

def check_prometheus_connection(prometheus_url, region, provider, username, password):
    try:
        connection, response = prom_get("/api/v1/targets", prometheus_url, username, password, timeout=10)
        if not connection:
            return False, "Invalid credentials.", []
        conn, message, targets = get_targets(response, provider.lower(), region)
        if conn:
            return True, "Prometheus connection is successful.", targets
        else:
            log_message(LevelType.ERROR, message, ErrorCode=-1)
            return False, message, targets
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to connect to Prometheus {str(err)}", ErrorCode=-1)
        return False, "Unable to connect to Prometheus.", []