import boto3
from datetime import datetime, timezone, timedelta
import logging
from app.utils.constants import DAYS_LOOKBACK, PERIOD, NAMESPACE_EC2, NAMESPACE_CWAGENT, LevelType
import ast
import os
import json
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.utils.telemetry_utils import get_average
from app.connections.pylogger import log_message

logger = logging.getLogger(__name__)

def get_boto3_session(aws_access_key_id, aws_secret_access_key, region_name):
    return boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

def get_instances(ec2_client):
    instances = []
    paginator = ec2_client.get_paginator('describe_instances')
    for page in paginator.paginate():
        for reservation in page['Reservations']:
            for instance in reservation['Instances']:
                if instance['State']['Name'] == 'running':
                    instances.append(instance)
    return instances

def calculate_uptime(cloudwatch, instance_id, period=3600):
    start_time = int((datetime.now(timezone.utc) - timedelta(hours=730)).timestamp())
    end_time = int(datetime.now(timezone.utc).timestamp())
    uptime_seconds = 0
    try:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='StatusCheckFailed',
            Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=['Maximum']
        )
        datapoints = response.get('Datapoints', [])
        uptime_seconds = sum(period for dp in datapoints if dp.get("Maximum", 1) == 0.0)
    except Exception as e:
        log_message(LevelType.ERROR, f"Error getting uptime for {instance_id}: {str(e)}", ErrorCode=-1)
    return round(uptime_seconds / 3600, 2)

def get_metric(cloudwatch, instance_id, namespace, metric_name, statistic='Maximum', unit=None, start_time=None, end_time=None):
    try:
        params = {
            "Namespace": namespace,
            "MetricName": metric_name,
            "Dimensions": [{'Name': 'InstanceId', 'Value': instance_id}],
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": PERIOD,
            "Statistics": [statistic]
        }
        if unit:
            params["Unit"] = unit

        response = cloudwatch.get_metric_statistics(**params)
        datapoints = response.get('Datapoints', [])
        if datapoints:
            return max((dp[statistic] for dp in datapoints), default=0)
    except Exception as e:
        log_message(LevelType.ERROR, f"Error fetching {metric_name} for {instance_id}: {str(e)}", ErrorCode=-1)
    return 0

def get_average_metric(cloudwatch, instance_id, namespace, metric_name, statistic='Average', unit=None, start_time=None, end_time=None):
    try:
        cpu_values = []
        params = {
            "Namespace": namespace,
            "MetricName": metric_name,
            "Dimensions": [{'Name': 'InstanceId', 'Value': instance_id}],
            "StartTime": start_time,
            "EndTime": end_time,
            "Period": PERIOD,
            "Statistics": [statistic]
        }
        if unit:
            params["Unit"] = unit

        response = cloudwatch.get_metric_statistics(**params)
        datapoints = response.get('Datapoints', [])
        if datapoints:
            cpu_values = [dp[statistic] for dp in datapoints]
        if not cpu_values:
            return 0.0, 0.0
        # Calculate UAVG, PAVG, P95 and U95
        uavg, pavg, u95, p95 = get_average(cpu_values)
        return uavg, u95, pavg, p95
    except Exception as e:
        log_message(LevelType.ERROR, f"Error fetching {metric_name} for {instance_id}: {str(e)}", ErrorCode=-1)
    return 0

def update_cca_quantity_hours(cca_data, cca_entry, up_time):
    for idx, entry in enumerate(cca_data):
        if entry['instance type'] == cca_entry['instance type'] and entry['region'] == cca_entry["region"]:
            total_up_time = cca_entry['quantity'] * 730 if up_time > 730 * cca_entry['quantity'] or up_time <= 0 else up_time
            cca_data[idx]['quantity'] += 1
            cca_data[idx]['monthly utilization (hourly)'] += total_up_time
            cca_data[idx]['monthly utilization (hourly)'] = round(cca_data[idx]['monthly utilization (hourly)'], 2)
            return cca_data
    cca_data.append(cca_entry)
    return cca_data

def collect_metrics(instance_list, aws_access_key_id, aws_secret_access_key, regions):
    eia_result = []
    cca_result = []

    for region in regions:
        try:
            session = get_boto3_session(aws_access_key_id, aws_secret_access_key, region)
            cloudwatch = session.client('cloudwatch')
            ec2 = session.client('ec2')

            instances = get_instances(ec2)
            filtered_instances = [inst for inst in instances if inst['InstanceId'] in instance_list]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=DAYS_LOOKBACK)

            for inst in filtered_instances:
                lifecycle = inst.get('InstanceLifecycle','ondemand').lower()
                instance_id = inst['InstanceId']
                instance_type = inst.get('InstanceType', 'unknown')
                az = inst['Placement']['AvailabilityZone']
                region = az[:-1] if len(az) > 1 else az
                instance_name = ''
                for tag in inst.get('Tags', []):
                    if tag['Key'] == 'Name':
                        instance_name = tag['Value']
                        break

                max_cpu = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'CPUUtilization', unit='Percent', start_time=start_time, end_time=end_time)
                mem_used = get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_used', unit='Bytes', start_time=start_time, end_time=end_time)
                mem_cached = get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_cached', unit='Bytes', start_time=start_time, end_time=end_time)
                mem_buffered = get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_buffered', unit='Bytes', start_time=start_time, end_time=end_time)
                mem_slab = get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_slab', unit='Bytes', start_time=start_time, end_time=end_time)
                max_mem = (mem_used + mem_cached + mem_buffered + mem_slab) / (1024 * 1024 * 1024)

                net_in = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'NetworkIn', unit='Bytes', start_time=start_time, end_time=end_time)
                net_out = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'NetworkOut', unit='Bytes', start_time=start_time, end_time=end_time)
                max_net_bw = ((net_in + net_out) * 8) / (PERIOD * 1_000_000) if net_in or net_out else 0

                ebs_read_bytes = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'EBSReadBytes', unit='Bytes', start_time=start_time, end_time=end_time)
                ebs_write_bytes = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'EBSWriteBytes', unit='Bytes', start_time=start_time, end_time=end_time)
                disk_write_bytes = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'DiskWriteBytes', unit='Bytes', start_time=start_time, end_time=end_time)
                disk_read_bytes = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'DiskReadBytes', unit='Bytes', start_time=start_time, end_time=end_time)
                max_disk_bw = (ebs_read_bytes + ebs_write_bytes + disk_write_bytes + disk_read_bytes) / (1024 * 1024 * PERIOD)

                ebs_read_ops = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'EBSReadOps', unit='Count', start_time=start_time, end_time=end_time)
                ebs_write_ops = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'EBSWriteOps', unit='Count', start_time=start_time, end_time=end_time)
                disk_read_ops = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'DiskReadOps', unit='Count', start_time=start_time, end_time=end_time)
                disk_write_ops = get_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'DiskWriteOps', unit='Count', start_time=start_time, end_time=end_time)
                max_iops = (ebs_read_ops + ebs_write_ops + disk_read_ops + disk_write_ops) / PERIOD

                up_time = calculate_uptime(cloudwatch, instance_id)
                if int(up_time > 730) or (up_time < 0):
                    up_time = 730
                    
                uavg, u95, _,_ = get_average_metric(cloudwatch, instance_id, NAMESPACE_EC2, 'CPUUtilization', unit='Percent', start_time=start_time, end_time=end_time)

                if all([instance_id, instance_type, region, max_cpu, max_mem, max_net_bw, max_disk_bw, max_iops]):
                    eia_result.append({
                        "uuid": instance_id,
                        "cloud_csp": "AWS",
                        "instance type": instance_type,
                        "instance name": instance_name if instance_name else '',
                        "region": region,
                        "max cpu%": round(max_cpu, 2),
                        "max mem used": round(max_mem, 2),
                        "max network bw": round(max_net_bw, 2),
                        "max disk bw used": round(max_disk_bw, 2),
                        "max iops": round(max_iops, 2),
                        'pricingModel': lifecycle,
                        "uavg": uavg,
                        "u95": u95,
                        # "pavg": pavg,
                        # "p95": p95
                    })

                cca_entry = {
                    'cloud_csp': "AWS",
                    'region': region,
                    'instance type': instance_type,
                    'quantity': 1,
                    'monthly utilization (hourly)': up_time,
                    'pricingModel': lifecycle,
                    'instance name': instance_name
                }
                cca_result = update_cca_quantity_hours(cca_result, cca_entry, up_time)

        except ClientError as err:
            log_message(LevelType.ERROR, f"error in collect_metrics for ClientError : {str(err)}", ErrorCode=-1)

            continue
        except Exception as err:
            log_message(LevelType.ERROR, f"error in collect_metrics : {str(err)}", ErrorCode=-1)
            continue

    return eia_result, cca_result


def check_cloudwatch_connection(aws_access_key_id, aws_secret_access_key, regions):
    try:
        # First validate the credentials by making a simple AWS API call
        valid_credentials = False
        for region in regions:
            if validate_aws_credentials(aws_access_key_id, aws_secret_access_key, region):
                valid_credentials = True
                break

        if not valid_credentials:
            return False, "Provided credentials are incorrect.", None

        instances_list = []

        with ThreadPoolExecutor(max_workers=min(10, len(regions))) as executor:
            future_to_region = {executor.submit(fetch_instances, aws_access_key_id, aws_secret_access_key, region): region for region in regions}

            for future in as_completed(future_to_region):
                result = future.result()
                if result:
                    instances_list.extend(result)

        if instances_list:
            return True, "CloudWatch connection is successful.", instances_list
        else:
            log_message(LevelType.ERROR, f"CloudWatch connection failed: No running instances found {aws_access_key_id} : {regions}", ErrorCode=-1)
            return False, "Unable to connect to CloudWatch: No running instances found.", None

    except Exception as err:
        log_message(LevelType.ERROR, f"error in check_cloudwatch_connection : {str(err)}", ErrorCode=-1)
        return False, "Unable to connect to CloudWatch", None

def validate_aws_credentials(aws_access_key_id, aws_secret_access_key, region):
    """Validate AWS credentials by making a simple API call"""
    try:
        session = get_boto3_session(aws_access_key_id, aws_secret_access_key, region)
        sts_client = session.client('sts')
        sts_client.get_caller_identity()  # This will throw an exception if credentials are invalid
        return True
    except ClientError as e:
        log_message(LevelType.ERROR, f"error in validate_aws_credentials for  ClientError: {str(e)}", ErrorCode=-1)
        if e.response['Error']['Code'] in ('InvalidClientTokenId', 'SignatureDoesNotMatch', 'InvalidAccessKeyId'):
            return False
        return True
    except Exception as err:
        log_message(LevelType.ERROR, f"error in validate_aws_credentials: {str(err)}", ErrorCode=-1)
        return False

def fetch_instances(aws_access_key_id, aws_secret_access_key, region):
    try:
        session = get_boto3_session(aws_access_key_id, aws_secret_access_key, region)
        ec2 = session.client('ec2')
        instances = get_instances(ec2)
        return instances
    except ClientError as err:
        log_message(LevelType.ERROR, f"error in fetch_instances for ClientError: {str(err)}", ErrorCode=-1)
        return []
    except Exception as err:
        log_message(LevelType.ERROR, f"error in fetch_instances: {str(err)}", ErrorCode=-1)
        return []

def cloudwatch_instances(aws_access_key_id, aws_secret_access_key, regions, instances):
    valid_instances = []
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=DAYS_LOOKBACK)

    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []

            for region in regions:
                try:
                    session = get_boto3_session(aws_access_key_id, aws_secret_access_key, region)
                    cloudwatch = session.client('cloudwatch')
                    for inst in instances:
                        if inst.get("Placement", {}).get("AvailabilityZone", "").startswith(region):
                            futures.append(executor.submit(is_valid_instance, cloudwatch, inst, region, start_time, end_time))
                except Exception as e:
                    log_message(LevelType.ERROR, f"Skipping region {region} due to error: {e}", ErrorCode=-1)

                    continue

            for future in as_completed(futures):
                result = future.result()
                if result:
                    valid_instances.append(result)

        if valid_instances:
            log_message(LevelType.ERROR, f"Fetched {len(valid_instances)} valid instances from CloudWatch", ErrorCode=-1)
            return valid_instances, "Valid CloudWatch instances fetched successfully."

        return [], "No valid CloudWatch instances found in the provided regions."

    except Exception as e:
        log_message(LevelType.ERROR, f"Error filtering valid CloudWatch instances: {str(e)}", ErrorCode=-1)
        return [], "Error fetching valid CloudWatch instances"


def is_valid_instance(cloudwatch, instance, region, start_time, end_time):
    try:
        instance_id = instance['InstanceId']
        metrics = [
            get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_used', unit='Bytes', start_time=start_time, end_time=end_time),
            get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_cached', unit='Bytes', start_time=start_time, end_time=end_time),
            get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_buffered', unit='Bytes', start_time=start_time, end_time=end_time),
            get_metric(cloudwatch, instance_id, NAMESPACE_CWAGENT, 'mem_slab', unit='Bytes', start_time=start_time, end_time=end_time),
        ]
        if any(metric > 0 for metric in metrics):
            instance_name = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'), '')
            return {
                'region': region,
                'instance_id': instance_id,
                'instance type': instance.get('InstanceType', 'unknown'),
                'InstanceLifecycle': instance.get('InstanceLifecycle', 'ondemand').lower(),
                'instance name': instance_name
            }
    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to fetch metrics for instance {instance.get('InstanceId')} in {region}: {str(e)}", ErrorCode=-1)
    return None