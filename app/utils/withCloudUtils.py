
import json
import os
import tempfile
import time
import boto3
from app.utils.constants import TOKEN_URI, UNIVERSE_URI
from google.api_core.exceptions import GoogleAPICallError
from azure.core.exceptions import HttpResponseError, ClientAuthenticationError
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from google.auth.exceptions import GoogleAuthError
from azure.mgmt.compute import ComputeManagementClient
from azure.identity import ClientSecretCredential
from google.oauth2 import service_account
from google.cloud import compute_v1
from datetime import datetime, timezone, timedelta
from werkzeug.utils import secure_filename



def data_extract(private_key_pem,cloud_account_data,user_mail,api_endpoint):
    """"""
    private_key_pem = replace_spaces_with_plus(private_key_pem)
    
    filtered_data = {
        "project_id": cloud_account_data.get("project_id"),
        "private_key": private_key_pem,
        "client_email": cloud_account_data.get("client_email"),
        "client_id" : cloud_account_data.get("client_id"),
        "token_uri": TOKEN_URI,
        "universe_domain": UNIVERSE_URI
    }

    file_path = f"GCP_{get_user_data_name(user_mail)}_{int(time.time())}.json"
    filename = secure_filename(file_path)
    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, filename)

    with open(temp_file_path, "w") as temp_file:
        json.dump(filtered_data, temp_file, indent=2, ensure_ascii=False)

    cloud_account_data['service_account_key_file'] = temp_file_path
    if api_endpoint=="/cloud-accounts/{query_type}":
        cloud_account_data['service_account_key_data'] = filtered_data
        del cloud_account_data['private_key']
        del cloud_account_data['client_email']
        del cloud_account_data['client_id']
    return cloud_account_data


def replace_spaces_with_plus(key_pem):
    if "\\n" in key_pem:
        key_pem = key_pem.replace("\\n", "\n")
    key_pem = key_pem.replace(" ", "+")
    lines = key_pem.splitlines()
    return "\n".join(line.replace("+", " ") if "PRIVATE+KEY" in line else line for line in lines)


def get_user_data_name(email):
    if not isinstance(email, str) or '@' not in email or '.' not in email.split('@')[-1]:
        return str(email)  # Fallback: just return the email or its string version

    try:
        username = email.split('@')[0]
        domain = email.split('@')[1].split('.')[0]
        return f"{username}_{domain}"
    except Exception:
        return str(email)

def test_cloud_connection(cloud_account_data):
    providers = {'aws': connect_to_aws, 'azure': connect_to_azure, 'gcp': connect_to_gcp}
    provider = cloud_account_data.get('provider', '').lower()
    if provider in providers:
        return providers[provider](cloud_account_data)
    return False, "Unsupported cloud provider"

def get_credential_vm_obj(cloud_account_data, cloud_csp):
    if cloud_csp == 'AWS':
        session = boto3.Session(
            aws_access_key_id=cloud_account_data['awsAccessId'],
            aws_secret_access_key=cloud_account_data['awsAccessSecret'],
            region_name=cloud_account_data['region']
        )
        return session, session.client('ec2')
    if cloud_csp == 'AZURE':
        credential = ClientSecretCredential(
            tenant_id=cloud_account_data['azureTenantId'],
            client_id=cloud_account_data['azureClientId'],
            client_secret=cloud_account_data['azureClientSecret']
        )
        compute_client = ComputeManagementClient(credential, cloud_account_data['azureSubscriptionId'])
        return compute_client, compute_client.virtual_machines.list_all()
    if cloud_csp == 'GCP':
        credentials = service_account.Credentials.from_service_account_file(cloud_account_data['service_account_key_file'])
        vm_obj = compute_v1.InstancesClient(credentials=credentials)
        zone_client = compute_v1.ZonesClient(credentials=credentials)
        zone=list(zone_client.list(project=cloud_account_data['project_id']))
        region_zones = [zone.name for zone in zone if zone.name.startswith(cloud_account_data['region'])]
        return vm_obj, region_zones, zone


def connect_to_aws(cloud_account_data):
    try:
        session, _ = get_credential_vm_obj(cloud_account_data, 'AWS')
        s3_client = session.client('s3')
        response = s3_client.list_buckets()

        if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
            return True, 'Success'
        return False, 'Failed'

    except (NoCredentialsError, PartialCredentialsError):
        return False, 'Invalid AWS credentials'
    except ClientError as e:
        error_message = e.response['Error']['Message']
        if 'signature' in error_message or 'Access Key Id' in error_message:
            error_message = 'Provided AWS Account ID or Access Key is invalid'
        return False, error_message
    except Exception as e:
        error_message = 'Provided AWS Region is invalid' if 'Could not connect to the endpoint URL' in str(e) else str(e)
        return False, error_message


def connect_to_azure(cloud_account_data):
    try:
        _, vms = get_credential_vm_obj(cloud_account_data, 'AZURE')
        if list(vms):
            return True, 'Success'
        return False, 'Failed to retrieve virtual machines'

    except (HttpResponseError, ClientAuthenticationError) as e:
        error_message = "Authentication failed for Azure account. Check credentials or permissions." if isinstance(e, ClientAuthenticationError) else "Authorization error. Verify subscription ID or permissions."
        return False, error_message
    except Exception as e:
        error_message = 'Provided Azure region or endpoint URL is invalid' if 'Could not connect to the endpoint URL' in str(e) else str(e)
        return False, error_message
    
def connect_to_gcp(cloud_account_data):
    try:
        _, _, zones = get_credential_vm_obj(cloud_account_data, 'GCP')
        
        if zones:
            return True, "Success."
        else:
            return False, "Failed to retrieve virtual machines."
    
    except (GoogleAPICallError, GoogleAuthError) as e:
        return False, str(e)
    except Exception as e:
        error_message = 'Invalid service account key format. Please check the key data.' if 'Could not deserialize key data' in str(e) else str(e)
        return False, error_message
    
def extract_instance_info(data, region, pricing_model, cloud_csp):
    if cloud_csp == 'AWS':
        instance_type = data.get("InstanceType")
        tags = data.get("Tags") or []
        instance_name = ""
        for tag in tags:
            if tag.get("Key") == "Name":
                instance_name = tag.get("Value")
                break
        if not instance_name and tags:
            instance_name = tags[0].get("Value")
    elif cloud_csp == 'AZURE':
        instance_type = data.hardware_profile.vm_size
        instance_name = data.name        
    elif cloud_csp == 'GCP':
        instance_type = data.get("instance_type")
        instance_name = data.get("instance_name")
    else:
        instance_type = None
    
    return {
        "region": region,
        "instance type": instance_type,
        "quantity": 1,
        "monthly utilization (hourly)": 730,
        "pricingModel": pricing_model,
        "cloud_csp": cloud_csp,
        "instance_name": instance_name if instance_name else ""
    }


def get_aws_instances(cloud_account_data, compute=False, reserved=False, spot=False):
    """get aws instance function"""
    _, vm_obj = get_credential_vm_obj(cloud_account_data, 'AWS')
    portfolio_list = []

    if reserved:
        response = vm_obj.describe_reserved_instances(Filters=[{'Name': 'state', 'Values': ['active']}])
        portfolio_list.extend(
            extract_instance_info(reserved_instance, cloud_account_data['region'].lower(), 'reserved', 'AWS')
            for reserved_instance in response['ReservedInstances']
            if reserved_instance['State'] != 'retired'
        )
        return portfolio_list

    if compute or spot:
        response = vm_obj.describe_instances()
        for reservation in response['Reservations']:
            for instance in reservation.get('Instances', []):
                state = instance['State']['Name']
                if state not in ['running', 'stopped']:
                    continue
                
                launch_time = instance.get('LaunchTime')  # datetime in UTC
                current_time = datetime.now(timezone.utc)

                if launch_time:
                    hours_used = (current_time - launch_time).total_seconds() / 3600
                else:
                    hours_used = 0

                # Check for spot instance
                if spot and instance.get('InstanceLifecycle') == 'spot':
                    instance_info = extract_instance_info(instance, cloud_account_data['region'].lower(), 'spot', 'AWS')
                    
                    utilization = 730 if hours_used == 0 else round(min(hours_used, 730))
                    instance_info['monthly utilization (hourly)'] = utilization
                    portfolio_list.append(instance_info)

                # Check for ondemand instance
                elif compute and 'InstanceLifecycle' not in instance:
                    instance_info = extract_instance_info(
                    instance,
                    cloud_account_data['region'].lower(),
                    'ondemand',
                    'AWS'
                    )
                    utilization = 730 if hours_used == 0 else round(min(hours_used, 730))
                    instance_info['monthly utilization (hourly)'] = utilization
                    portfolio_list.append(instance_info)

        return portfolio_list
    
def get_azure_instances(cloud_account_data):
    compute_client, vm_obj = get_credential_vm_obj(cloud_account_data, 'AZURE')
    portfolio_list = []
    for vm in vm_obj:
        vm_details = compute_client.virtual_machines.get(vm.id.split('/')[4], vm.name, expand='instanceView')
        instance_view = vm_details.instance_view
        statuses = instance_view.statuses if instance_view else []
        status = vm_details.instance_view.statuses[1].display_status if len(vm_details.instance_view.statuses) > 1 else "Unknown"
        if status in ['VM running', 'VM deallocated']:
            instance_type = 'ondemand'
            if hasattr(vm_details, 'priority') and vm_details.priority and vm_details.priority.lower() == 'spot':
                instance_type = 'spot'
            launch_time = None

            for status in statuses:
                if 'provisioningstate/succeeded' in status.code.lower():
                    launch_time = status.time
                    break
            hours_used = 0
            
            if launch_time:
                current_time = datetime.now(timezone.utc)
                hours_used = (current_time - launch_time).total_seconds() / 3600
                
            instance_info = extract_instance_info(vm_details, vm_details.location, instance_type, 'AZURE')
            utilization = 730 if hours_used == 0 else round(min(hours_used, 730))
            instance_info['monthly utilization (hourly)'] = utilization
            portfolio_list.append(instance_info)

    return portfolio_list

def list_reservations(reservations_client, project, zone):
    """List all Compute Engine reservations for a zone"""
    request = compute_v1.ListReservationsRequest(
        project=project,
        zone=zone
    )
    return reservations_client.list(request)

def is_instance_reserved(instance, reservations):
    """Check if instance is covered by a reservation"""
    instance_type = instance.machine_type.split("/")[-1]
    for reservation in reservations:
        # Check if any specific reservation covers this instance type
        for specific_reservation in reservation.specific_reservations:
            if instance_type in [vm.machine_type for vm in specific_reservation.instance_properties]:
                return True
    return False

def get_gcp_instances(cloud_account_data):
    """get gcp instance info"""
    client, zones, _ = get_credential_vm_obj(cloud_account_data, 'GCP')
    project = cloud_account_data['project_id']
    portfolio_list = []
    info = {
        "client_email": cloud_account_data['service_account_key_data']['client_email'],
        "private_key": cloud_account_data['service_account_key_data']['private_key'],
        "token_uri": cloud_account_data['service_account_key_data']['token_uri']
    }
    credentials = service_account.Credentials.from_service_account_info(info)
    # Get GCP reservations
    _ = compute_v1.InstancesClient(credentials=credentials)
    reservations_client = compute_v1.ReservationsClient(credentials=credentials)
    for zone in zones:
        instance_list = client.list(project=project, zone=zone)
        # Get reservations for the zone
        zone_name = zone.split('/')[-1]
        reservations = list_reservations(reservations_client, project, zone_name)
        for instance in instance_list:
            if instance.status in ["RUNNING", "TERMINATED"]: 
                pricing_model = "spot" if instance.scheduling.preemptible else "ondemand"
                # Check if instance is covered by a reservation
                if is_instance_reserved(instance, reservations):
                    pricing_model = "reserved"
                region = "-".join(zone_name.split("-")[:2])
                instance_type = instance.machine_type.split("/")[-1]
                
                instance_info = extract_instance_info({"instance_type" : instance_type, "instance_name" : instance.name}, region, pricing_model, 'GCP')
                
                try:
                    creation_time = datetime.strptime(instance.creation_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
                except ValueError:
                    creation_time = datetime.strptime(instance.creation_timestamp, "%Y-%m-%dT%H:%M:%S%z")
                    
                now = datetime.now(timezone.utc)
                hours_used = (now - creation_time).total_seconds() / 3600
                utilization = 730 if hours_used == 0 else round(min(hours_used, 730))
                instance_info['monthly utilization (hourly)'] = utilization
                portfolio_list.append(instance_info)

    return portfolio_list

    
def sync_gcp_account(cloud_account_data, user_mail):
    """sync gcp account"""
    file_path = f"GCP_{get_user_data_name(user_mail)}_{int(time.time())}.json"
    key_data = cloud_account_data["service_account_key_data"]

    filename = secure_filename(file_path)   
    temp_dir = tempfile.gettempdir()        
    temp_file_path = os.path.join(temp_dir, filename)
    with open(temp_file_path, "w") as temp_file:        
        json.dump(key_data, temp_file, indent=2)
        
    cloud_account_data['service_account_key_file'] = temp_file_path   
    cloud_account_data['project_id']=cloud_account_data['service_account_key_data']['project_id']
    return cloud_account_data