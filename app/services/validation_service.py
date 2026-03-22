import math
import os
import re
from bson import ObjectId
import h5py
import pandas as pd
from app.connections.cloud_s3_connect import fetch_s3_file
from app.connections.custom_exceptions import CustomAPIException
from app.connections.mongodb import get_collection
from app.connections.pylogger import log_message
from app.utils.common_utils import BILLING_PARSERS
from app.utils.constants import CLOUD_PROVIDERS, CollectionNames, UNSUPPORTED_PROVIDERS, PRICING_MODEL, UNSUPPORTED_PRICING_MODEL, PRICING_DATABASE, NUMBER_VALIDATION, PIPE, PRICE_MODEL, PRICEMODEL, REQUERED_FIELD_ERROR, LevelType
from app.utils.constants import DATA_SUCCESS, INSTANCE_ERROR, UTILIZATION, MAX_CPU, MAX_MEM_USED, MAX_NW_BW, MAX_DISK_BW, INSTANCE_TYPE, MAX_IOPS, REGION_REQUIRED, HOURS_REQUIRED, NO_DATA_IN_FILE, FILE_VALIDATE_ERROR
import uuid
import datetime
from io import BytesIO
import asyncio
from pydantic import BaseModel

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # project root
h5_file_path = os.path.join(ROOT_DIR, 'database', PRICING_DATABASE)
with h5py.File(h5_file_path, 'r') as hdf:
    providers = list(hdf.keys())
    regions_map = {provider: list(hdf[provider].keys()) for provider in providers}
    instances_map = {
        (provider, region): set(hdf[f"{provider}/{region}"]['Instance'][:].astype(str))
        for provider, regions in regions_map.items()
        for region in regions
    }

azure_instance_pattern = r"^(standard|basic)_[A-Za-z]+\d+[a-z]*(_v\d+)?$"
aws_instance_pattern = fr'\b(?:r7iz{PIPE}g4dn{PIPE}c7a{PIPE}mac2{PIPE}r6id{PIPE}c8g{PIPE}c5{PIPE}c7i{PIPE}r5{PIPE}c3{PIPE}m6i{PIPE}gr6{PIPE}f1{PIPE}mac2-m2{PIPE}c6in{PIPE}x8g{PIPE}is4gen{PIPE}u-9tb1{PIPE}i3{PIPE}c6i{PIPE}t2{PIPE}d3{PIPE}i2{PIPE}r5ad{PIPE}u-18tb1{PIPE}c7gn{PIPE}r5dn{PIPE}c5n{PIPE}r3{PIPE}c6a{PIPE}m1{PIPE}r5a{PIPE}m5zn{PIPE}u-3tb1{PIPE}i7ie{PIPE}z1d{PIPE}m6g{PIPE}r5n{PIPE}r5d{PIPE}g6{PIPE}r6in{PIPE}mac2-m1ultra{PIPE}inf1{PIPE}dl1{PIPE}m6in{PIPE}hpc7g{PIPE}hpc6a{PIPE}hpc6id{PIPE}hpc7a{PIPE}t1{PIPE}m8g{PIPE}d3en{PIPE}m7a{PIPE}r6a{PIPE}r7gd{PIPE}c6gn{PIPE}i8g{PIPE}m5d{PIPE}r7i{PIPE}inf2{PIPE}c6id{PIPE}trn1{PIPE}m7i{PIPE}r7a{PIPE}c6g{PIPE}g5g{PIPE}r8g{PIPE}c7i-flex{PIPE}g5{PIPE}c5a{PIPE}u-6tb1{PIPE}t3a{PIPE}c7g{PIPE}r7an{PIPE}r6idn{PIPE}p4d{PIPE}r6g{PIPE}c4{PIPE}r7g{PIPE}x1{PIPE}d2{PIPE}i3en{PIPE}x2idn{PIPE}m6gd{PIPE}t3{PIPE}p5{PIPE}vt1{PIPE}t4g{PIPE}c7gd{PIPE}u7in-16tb{PIPE}c5ad{PIPE}m5ad{PIPE}x2iedn{PIPE}m7i-flex{PIPE}m6a{PIPE}i4i{PIPE}mac1{PIPE}mac2-m2pro{PIPE}m5n{PIPE}r5b{PIPE}trn1n{PIPE}m5dn{PIPE}h1{PIPE}p3dn{PIPE}a1{PIPE}c1{PIPE}r4{PIPE}r6gd{PIPE}u-24tb1{PIPE}c6gd{PIPE}g6e{PIPE}u7in-32tb{PIPE}m2{PIPE}x1e{PIPE}u-12tb1{PIPE}m5a{PIPE}u7in-24tb{PIPE}p2{PIPE}x2iezn{PIPE}c5d{PIPE}m7g{PIPE}m6id{PIPE}i4g{PIPE}m7gd{PIPE}m4{PIPE}u7i-12tb{PIPE}x2gd{PIPE}g4ad{PIPE}m6idn{PIPE}im4gn{PIPE}p3{PIPE}r6i{PIPE}m3{PIPE}m5)\.(?:large{PIPE}medium{PIPE}metal-48xl{PIPE}18xlarge{PIPE}9xlarge{PIPE}micro{PIPE}12xlarge{PIPE}96xlarge{PIPE}6xlarge{PIPE}metal{PIPE}nano{PIPE}10xlarge{PIPE}3xlarge{PIPE}metal-32xl{PIPE}metal-24xl{PIPE}xlarge{PIPE}112xlarge{PIPE}2xlarge{PIPE}224xlarge{PIPE}small{PIPE}56xlarge{PIPE}32xlarge{PIPE}4xlarge{PIPE}48xlarge{PIPE}16xlarge{PIPE}metal-16xl{PIPE}24xlarge{PIPE}8xlarge)\b'
gcp_instance_pattern = r"^(c4a|n4|a3|n2|z3|a2|m2|n2d|f1|g1|g2|c3d|m1|h3|c4|t2a|n1|c2d|c3|e2|m3|t2d|c2|c4d|n4d)-(ultragpu|megamem|standard|megagpu|highgpu|ultramem|medium|highcpu|hypermem|small|micro|highmem)(-(60|48|22|224|1g|416|208|44|96|56|4|72|128|16|112|80|90|180|16g|24|176|2|8|2g|32|30|160|1|192|12|40|8g|360|4g|64|88|384|144|288))?(-metal|-lssd)?$"

def get_all_instances_for_provider(provider):
    all_instances = set()
    for region in regions_map.get(provider, []):
        all_instances.update(instances_map.get((provider, region), set()))

    instance_type_data = sorted([str(item) for item in all_instances])
    log_message(LevelType.INFO, "All instances data fetched", ErrorCode=1)

    return instance_type_data


def value_format(value):
    try:
        value = remove_commas(value)
        value = float(value)
        if value.is_integer():
            return int(value)
        else:
            return math.ceil(value)
    except ValueError as e:
        log_message(LevelType.ERROR, f"Error : {str(e)}", ErrorCode=-1)
        return value


def remove_commas(value):
    if isinstance(value, str) and ',' in value:
        return value.replace(',', '')
    return value

async def file_args_validation(cloud_csp, app, udf_file, file):
    await asyncio.sleep(0)
    data = { "Provider": cloud_csp, "Data": "", "UDF": ""} 
    if not cloud_csp or not app:
        log_message(LevelType.ERROR, REQUERED_FIELD_ERROR,data=data, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR, data=data)
    if app.upper() not in ["CCA", "EIA"]:
        msg = "Invalid application provided."
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)
    if udf_file:
        udf_file_name = udf_file.filename
        if not udf_file_name.endswith('.xlsx'):
            msg = "Invalid own metrics file provided"
            log_message(LevelType.ERROR, msg, data=data, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=msg, data=data)
    if file:
        file_name = file.filename
        if not file_name.endswith('.xlsx'):
            msg = "Invalid file provided"
            log_message(LevelType.ERROR, msg, data=data, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=msg, data=data)
    if cloud_csp.upper() not in CLOUD_PROVIDERS:
        log_message(LevelType.ERROR, FILE_VALIDATE_ERROR, data=data, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=FILE_VALIDATE_ERROR, data=data)
    if cloud_csp.upper() in CLOUD_PROVIDERS and cloud_csp.upper() in UNSUPPORTED_PROVIDERS:
        msg = "Provider is unsupported"
        log_message(LevelType.ERROR, msg, data=data, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg, data=data)
    return None


async def eia_data_read(file, udf_file, cloud_csp):
    data, udf_data, flag, message, udf_message = "", {}, False, "Invalid data provided", ""
    if not file and not udf_file:
        data, udf_data = [], []
        message = "Input file not provided"
    if file:
        data, message, flag = await validate_file_input_data_eia(file, cloud_csp)
        if not flag:
            log_message(LevelType.INFO, message, ErrorCode=1)
            return {}, {}, message, "", False

    if udf_file:
        udf_data, udf_message = await read_udf_file_data(udf_file)
        if not udf_data:
            message = f"Own metrics data: {udf_message}"
            log_message(LevelType.INFO, f"{message} for file : {file}",ErrorCode=1)
        else:
            flag = True
    return data, udf_data, message, udf_message, flag

def cca_input_format(entry, cloud_csp, instance_type_data):
    entry = {k: (v.strip() if isinstance(v, str) else v) for k, v in entry.items()}
    entry['quantity'] = value_format(entry['quantity'])
    entry[UTILIZATION] = value_format(entry[UTILIZATION])
    if all(not x for x in [entry['cloud_csp'], entry['region'], entry[INSTANCE_TYPE], entry['quantity'], entry[UTILIZATION], entry['pricingModel']]):
        return None
    entry['Remarks'] = validate_fields_cca(entry['cloud_csp'], entry['region'], entry[INSTANCE_TYPE], entry['quantity'], entry[UTILIZATION], entry['pricingModel'], cloud_csp, instance_type_data, None)
    return entry


def eia_input_format(entry, cloud_csp):
    entry = {k: (v.strip() if isinstance(v, str) else v) for k, v in entry.items()}
    entry[MAX_CPU] = value_format(entry[MAX_CPU])
    entry[MAX_MEM_USED] = value_format(entry[MAX_MEM_USED])
    entry[MAX_NW_BW] = value_format(entry[MAX_NW_BW])
    entry[MAX_DISK_BW] = value_format(entry[MAX_DISK_BW])
    entry[MAX_IOPS] = value_format(entry[MAX_IOPS])
    if all(not x for x in [entry["uuid"], entry['cloud_csp'], entry[INSTANCE_TYPE], entry['region'], entry[MAX_CPU], entry[MAX_MEM_USED], entry[MAX_NW_BW], entry[MAX_DISK_BW], entry[MAX_IOPS], entry[PRICE_MODEL]]):
        return None
    primary_remarks = validate_fields_eia(entry["uuid"], entry['cloud_csp'], entry[INSTANCE_TYPE], entry['region'], entry[MAX_CPU], entry[MAX_MEM_USED], entry[MAX_NW_BW], entry[MAX_DISK_BW], entry[MAX_IOPS], cloud_csp, entry['pricingModel'], None)
    metric_remarks = validate_utilization_metrics_fields(entry)
    entry['Remarks'] = primary_remarks + metric_remarks
    return entry

def cca_validate_input_data(input_data, cloud_csp,):
    try:
        if not input_data:
            return {}
        output_data = []
        for item in input_data:
            if not item['uuid'] or 'uuid' not in item or not item['uuid']:
                item['uuid'] = str(uuid.uuid4())
        instance_type_data = get_all_instances_for_provider(cloud_csp)
        for entry in input_data:
            entry = cca_input_format(entry, cloud_csp, instance_type_data)
            if not entry:
                continue
            output_data.append(entry)
        log_message(LevelType.INFO, f"output_data : {output_data} for cloud_csp : {cloud_csp}",ErrorCode=1)
        return output_data
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in cca_validate_input_data for cloud_csp : {cloud_csp}: {str(e)}",ErrorCode=-1)
        return {}


async def validate_input_data(input_data, cloud_csp, app, udf_data, request):
    app=app.strip()
    try:
        if not input_data and not udf_data:
            log_message(LevelType.ERROR, f"No data provided in input for {cloud_csp} : {app}",ErrorCode=-1)
            return {}, {}, "No data provided in input"
        output_data = []
        udf_errors = []
        field_alias_map_eia = {
                "max_cpu_percent": "max cpu%",
                "instance_type": "instance type",
                "max_mem_used": "max mem used",
                "max_network_bw":"max network bw",
                "max_disk_bw_used":"max disk bw used",
                "max_iops":"max iops"
            }
        field_alias_map_cca = {
                "instance_type": "instance type",
                "monthly_utilization_hourly": "monthly utilization (hourly)"
            }
        if app == "CCA":
            instance_type_data = get_all_instances_for_provider(cloud_csp)
            for entry in input_data:
                if isinstance(entry, BaseModel):
                    entry = entry.dict()

                for alias_key, actual_key in field_alias_map_cca.items():
                    if alias_key in entry:
                        entry[actual_key] = entry.pop(alias_key)
                if 'uuid' not in entry or not entry['uuid']:
                    entry['uuid'] = str(uuid.uuid4())
                entry = cca_input_format(entry, cloud_csp, instance_type_data)
                if not entry:
                    log_message(LevelType.ERROR, f"skipping due no entry for {cloud_csp} : {app}",ErrorCode=-1)
                    continue
                output_data.append(entry)
        else:
            for entry in input_data:
                if not isinstance(entry, dict):
                    entry = entry.model_dump()
                
                for alias_key, actual_key in field_alias_map_eia.items():
                    if alias_key in entry:
                        entry[actual_key] = entry.pop(alias_key)
                if 'uuid' not in entry or not entry['uuid']:
                    entry['uuid'] = str(uuid.uuid4())
                entry = eia_input_format(entry, cloud_csp)
                if not entry:
                    log_message(LevelType.ERROR, f"skipping due no entry for {cloud_csp} : {app}",ErrorCode=-1)
                    continue
                output_data.append(entry)
        return output_data, udf_errors, DATA_SUCCESS, None
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in validate_input_data: {cloud_csp} : {app}: {str(e)}",ErrorCode=-1)
        return {}, {}, "Unable to process provided data", None


def region_instance_validate(region, instance, remark_list):
    if not region:
        remark_list.append({"Field": "region", "Message": REGION_REQUIRED})
    if not instance:
        remark_list.append({"Field": INSTANCE_TYPE, "Message": "Size is required"})
    return remark_list

def regex_validation(instance,instance_type_data, cloud_csp, remark_list):
    if not instance:
        remark_list.append({"Field": INSTANCE_TYPE, "Message": INSTANCE_ERROR})
    elif not isinstance(instance, str):
        remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
    elif instance.lower() in instance_type_data:
        if not ((cloud_csp == "AZURE" and re.match(azure_instance_pattern, instance.lower())) or (
                cloud_csp == "AWS" and re.match(aws_instance_pattern, instance.lower())) or 
                cloud_csp == "GCP" and re.match(gcp_instance_pattern, instance.lower())):
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is unsupported"})
    else:
        remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
    return remark_list

def region_instance_cloud_data(region, cloud_csp, instance, remark_list, instance_type_data, flag=None):
    if not region:
        remark_list.append({"Field": "region", "Message": REGION_REQUIRED})
    elif not isinstance(region, str):
        remark_list.append({"Field": "region", "Message": f"{region} is invalid"})
    elif region.lower() not in regions_map[cloud_csp]:
        remark_list.append({"Field": "region", "Message": f"{region} is invalid"})
    if not flag:
        remark_list = regex_validation(instance,instance_type_data, cloud_csp, remark_list)
    else:
        region_instance_type_data = sorted([str(item) for item in instances_map.get((cloud_csp, region), set())])
        if not instance:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": INSTANCE_ERROR})
        if not isinstance(instance, str):
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
        elif instance.lower() not in region_instance_type_data and instance.lower() in instance_type_data:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is unsupported"})
        elif instance.lower() not in instance_type_data:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
    return remark_list


def validate_cloud_data(provider, region, instance, remark_list, cloud_csp, instance_type_data, flag):
    if not provider:
        remark_list.append({"Field": "cloud_csp", "Message": "Cloud is required"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif not isinstance(provider, str):
        remark_list.append({"Field": "cloud_csp", "Message": f"{provider} is invalid "})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif cloud_csp != provider.upper() and provider.upper() in CLOUD_PROVIDERS:
        remark_list.append({"Field": "cloud_csp", "Message": "Cloud input should be same as Cloud Service Provider"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif provider.upper() in CLOUD_PROVIDERS and provider.upper() in UNSUPPORTED_PROVIDERS:
        remark_list.append({"Field": "cloud_csp", "Message": f"{provider} is unsupported"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif provider.upper() not in CLOUD_PROVIDERS or provider.upper() != cloud_csp:
        remark_list.append({"Field": "cloud_csp", "Message": f"{provider} is invalid"})
        remark_list = region_instance_validate(region, instance, remark_list)
    else:
        remark_list = region_instance_cloud_data(region, cloud_csp, instance, remark_list, instance_type_data, flag)
    return remark_list


def hours_validation_cca(max_hours, remark_list, total_hours):
    if not total_hours:
        remark_list.append({"Field": UTILIZATION, "Message": HOURS_REQUIRED})
    else:
        if isinstance(total_hours, (int, float)) and total_hours > 0:
            total_hours = math.ceil(total_hours)
            if not (0 < total_hours <= max_hours):
                remark_list.append(
                    {"Field": UTILIZATION, "Message": f"Hours should not exceed {max_hours}"})
        else:
            remark_list.append(
                {"Field": UTILIZATION, "Message": "Hours must be a positive number"})
    return remark_list


def quantity_hours_cca_validation(quantity, total_hours, remark_list):
    if not quantity:
        remark_list.append({"Field": "quantity", "Message": "Quantity is required"})
        if not total_hours:
            remark_list.append({"Field": UTILIZATION, "Message": HOURS_REQUIRED})
    elif isinstance(quantity, (int, float)) and quantity > 0:
        quantity = math.ceil(quantity)
        max_hours = quantity * 730
        remark_list = hours_validation_cca(max_hours, remark_list, total_hours)
    else:
        remark_list.append({"Field": "quantity", "Message": "Quantity must be a positive number"})
        if not total_hours:
            remark_list.append({"Field": UTILIZATION, "Message": HOURS_REQUIRED})
    return remark_list


def validate_fields_cca(provider, region, instance, quantity, total_hours, pricing_model, cloud_csp, instance_type_data, flag):
    remark_list = []
    remark_list = validate_cloud_data(provider, region, instance, remark_list, cloud_csp, instance_type_data, flag)
    remark_list = quantity_hours_cca_validation(quantity, total_hours, remark_list)
    if not pricing_model:
        remark_list.append({"Field": "pricingModel", "Message": "Pricing Model is required"})
    elif pricing_model.lower() in PRICING_MODEL and pricing_model.lower() in UNSUPPORTED_PRICING_MODEL:
        remark_list.append({"Field": "pricingModel", "Message": f"{pricing_model} pricing model is unsupported"})
    elif pricing_model.lower() not in PRICING_MODEL:
        remark_list.append({"Field": "pricingModel", "Message": f"{pricing_model} pricing model is invalid"})
    return remark_list


def disk_fields_validation(disk_max, iops_max, remark_list):
    if not disk_max:
        remark_list.append({"Field": MAX_DISK_BW, "Message": "Max Disk BW used is required"})
    elif not isinstance(disk_max, (int, float)) or disk_max <= 0:
        remark_list.append({"Field": MAX_DISK_BW, "Message": "Max Disk BW used " + NUMBER_VALIDATION})
    if not iops_max:
        remark_list.append({"Field": MAX_IOPS, "Message": "Max IOPS is required"})
    elif not isinstance(iops_max, (int, float)) or iops_max <= 0:
        remark_list.append({"Field": MAX_IOPS, "Message": "Max IOPS " + NUMBER_VALIDATION})
    return remark_list


def validate_fields_eia(uuid, provider, instance, region, cpu_max, mem_max, net_max, disk_max, iops_max, cloud_csp, pricing_model, flag):
    remark_list = []
    instance_type_data = get_all_instances_for_provider(cloud_csp)
    remark_list = validate_cloud_data(provider, region, instance, remark_list, cloud_csp, instance_type_data, flag)
    if not cpu_max:
        remark_list.append({"Field": "max cpu%", "Message": "Max CPU is required"})
    elif not isinstance(cpu_max, (int, float)) or not (0 < cpu_max <= 100):
        remark_list.append(
            {"Field": "max cpu%", "Message": "Max CPU must be a positive number and range between 1 to 100"})
    if not mem_max:
        remark_list.append({"Field": MAX_MEM_USED, "Message": "Max Mem used is required"})
    elif not isinstance(mem_max, (int, float)) or mem_max <= 0:
        remark_list.append({"Field": MAX_MEM_USED, "Message": "Max Mem used " + NUMBER_VALIDATION})
    if not net_max:
        remark_list.append({"Field": MAX_NW_BW, "Message": "Max Network BW is required"})
    elif not isinstance(net_max, (int, float)) or net_max <= 0:
        remark_list.append({"Field": MAX_NW_BW, "Message": "Max Network BW " + NUMBER_VALIDATION})
    remark_list = disk_fields_validation(disk_max, iops_max, remark_list)
    uuid = str(uuid).strip()
    if not uuid:
        remark_list.append({"Field": "uuid", "Message": "UUID is required"})
    if not pricing_model:
        remark_list.append({"Field": "pricingModel", "Message": "Pricing Model is required"})
    elif pricing_model.lower() in PRICING_MODEL and pricing_model.lower() in UNSUPPORTED_PRICING_MODEL:
        remark_list.append({"Field": "pricingModel", "Message": f"{pricing_model} pricing model is unsupported"})
    elif pricing_model.lower() not in PRICING_MODEL:
        remark_list.append({"Field": "pricingModel", "Message": f"{pricing_model} pricing model is invalid"})
    return remark_list


def remarks_input_cca(data, cloud_csp):
    remarks = []
    corrected_data = []
    try:
        instance_type_data = get_all_instances_for_provider(cloud_csp)
        data['quantity'] = data['quantity'].apply(value_format)
        data[UTILIZATION] = data[UTILIZATION].apply(value_format)
        for index, row in data.iterrows():
            row_dict = row.to_dict()
            corrected_entry = auto_correct_validation_data_cca(row_dict, cloud_csp)
            remarks_list = validate_fields_cca(corrected_entry['cloud_csp'], corrected_entry['region'], corrected_entry[INSTANCE_TYPE], corrected_entry['quantity'], corrected_entry[UTILIZATION], corrected_entry['pricingModel'], cloud_csp, instance_type_data, None)
            remarks.append(remarks_list)
            corrected_data.append(corrected_entry)
        corrected_data_df = pd.DataFrame(corrected_data)
        return remarks, corrected_data_df
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in remarks_input_cca: {cloud_csp} : {str(e)}",ErrorCode=-1)
        return "Failed", None
    
def validate_utilization_metrics_fields(entry):
    """
    Validates pavg, uavg, p95, and u95 fields to ensure they are numbers (0-100).
    Returns a list of remarks.
    """
    remarks = []
    numeric_fields = ['pavg', 'uavg', 'p95', 'u95']

    for field in numeric_fields:
        value = entry.get(field, '')
        if value != '':
            # Disallow datetime or timestamp types
            if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
                remarks.append({
                    "Field": field,
                    "Message": f"Timestamps or dates are not allowed in {field}. Must be a number between 0 and 100."
                })
                continue
            try:
                float_val = float(value)
                if not (0 <= float_val <= 100):
                    remarks.append({
                        "Field": field,
                        "Message": f"Invalid value '{value}' in {field}. Must be a number between 0 and 100."
                    })
            except (ValueError, TypeError) as err:
                log_message(LevelType.ERROR, f"Invalid value '{value}' in {field}. Must be a number between 0 and 100. : {str(err)}",ErrorCode=-1)
                remarks.append({
                    "Field": field,
                    "Message": f"Invalid value '{value}' in {field}. Must be a number between 0 and 100."
                })

    return remarks


def remarks_input_eia(data, cloud_csp):
    remarks = []
    auto_corrected_data = []
    try:
        for _, row in data.iterrows():
            row_dict = row.to_dict()
            corrected_entry = auto_correct_validation_data_eia(row_dict, cloud_csp)
            uuid = corrected_entry['uuid']
            provider = corrected_entry['cloud_csp']
            instance = corrected_entry[INSTANCE_TYPE]
            region = corrected_entry['region']
            cpu_max = corrected_entry[MAX_CPU]
            mem_max = corrected_entry['max mem used']
            net_max = corrected_entry['max network bw']
            disk_max = corrected_entry['max disk bw used']
            iops_max = corrected_entry['max iops']
            remarks_list = validate_fields_eia(uuid, provider, instance, region, cpu_max, mem_max, net_max, disk_max, iops_max, cloud_csp, corrected_entry[PRICE_MODEL], None)
            # Additional validation for pavg, uavg, p95, u95
            metric_remarks = validate_utilization_metrics_fields(corrected_entry)
            combined_remarks = remarks_list + metric_remarks
            remarks.append(combined_remarks)
            auto_corrected_data.append(corrected_entry)
        auto_corrected_data_df = pd.DataFrame(auto_corrected_data)
        return remarks, auto_corrected_data_df
    except Exception as e:
        log_message(LevelType.ERROR, f"Error for remarks_input_eia : {str(e)}",ErrorCode=-1)
        return "Failed", None


def auto_correct_validation_data_cca(entry, provider):
    if not entry['cloud_csp'] or not isinstance(entry['cloud_csp'],str) or entry['cloud_csp'].upper() not in CLOUD_PROVIDERS or entry['cloud_csp'].upper() != provider:
        entry['cloud_csp'] = provider
    quantity, _ = value_format_auto_correction(entry['quantity'], entry[UTILIZATION], "quantity")
    if not quantity:
        entry['quantity'] = value_format(entry['quantity'])
    else:
        entry['quantity'] = quantity
    if not entry[UTILIZATION]:
        entry[UTILIZATION] = 730 * value_format(entry['quantity'])
    hours, _ = value_format_auto_correction(entry['quantity'], entry[UTILIZATION], "hours")
    if not hours:
        entry[UTILIZATION] = 730 * value_format(entry[UTILIZATION])
    else:
        entry[UTILIZATION] = hours
    if not entry.get('pricingModel') or (str(entry['pricingModel']).lower() not in PRICING_MODEL):
        entry['pricingModel'] = 'ondemand'
    return entry


def auto_correct_validation_data_eia(entry, provider):
    if not entry['cloud_csp'] or not isinstance(entry['cloud_csp'],str) or entry['cloud_csp'].upper() not in CLOUD_PROVIDERS or entry['cloud_csp'].upper() != provider:
        entry['cloud_csp'] = provider
    if PRICE_MODEL not in entry or (entry[PRICE_MODEL].lower() not in PRICING_MODEL):
        entry[PRICE_MODEL] = 'ondemand'
    return entry


def cca_eia_headers_validation(data, required_headers):
    missing_headers = [header for header in required_headers if header not in data.columns]
    if missing_headers:
        log_message(LevelType.ERROR, f"Missing headers: [{', '.join(missing_headers)}]. Please download the template to get exact headers.",ErrorCode=-1)
        return {}, f"Missing headers: [{', '.join(missing_headers)}]. Please download the template to get exact headers."

def uuid_validation(data):
    if "uuid" not in data.columns:
        data['uuid'] = [str(uuid.uuid4()) for _ in range(len(data))]
    else:
        data['uuid'] = data['uuid'].apply(lambda x: str(uuid.uuid4()) if pd.isnull(x) or x == "" else x)
    return data

async def validate_file_input_data_cca(file, provider, portfolio_id, is_billing_data=False):
    try:
        content = await file.read()  # This returns bytes
        excel_data = BytesIO(content)
        data = pd.read_excel(excel_data)
        if data.empty:
            log_message(LevelType.ERROR, "Data is empty in excel",ErrorCode=-1)
            return {}, NO_DATA_IN_FILE, False

        data.columns = data.columns.astype(str)
        data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        data = data.fillna('')
        data.columns = data.columns.str.strip().str.lower()

        data = data.replace('', '')
        data = data.loc[~(data == '').all(axis=1)].reset_index(drop=True)
        if data.shape[0] > 3000:
            log_message(LevelType.ERROR, "Data is too large to process",ErrorCode=-1)
            return {}, "Data is too large to process", False
        
        if is_billing_data:
            parser_func = BILLING_PARSERS[provider]
            data = parser_func(data, provider, portfolio_id , regions_map)

            # ✅ Check if billing data is empty
            if data.empty:
                log_message(LevelType.ERROR, "No instance SKUs found from billing", ErrorCode=-1)
                return {}, "No instance SKUs found from billing", False
            

        required_headers = ["cloud", "region", "size", "quantity", "total number of hours per month", PRICEMODEL]

        if not all(header in data.columns for header in required_headers):
            val, message = cca_eia_headers_validation(data, required_headers)
            if not val:
                log_message(LevelType.ERROR, f"message : {message}",ErrorCode=-1)
                return {}, message, False

        data = uuid_validation(data)
            
        data = data.rename(columns={
            "cloud": "cloud_csp",
            "region": "region",
            "size": INSTANCE_TYPE,
            "quantity": "quantity",
            "total number of hours per month": UTILIZATION,
            PRICEMODEL: "pricingModel"
        })

        if "cloud_csp" in data.columns:
            data["cloud_csp"] = data["cloud_csp"].apply(
                lambda x: x.upper().strip() if isinstance(x, str) and x else ""  # leave blanks for null/empty
            )

        columns_to_clean = ['quantity', UTILIZATION]
        for column in columns_to_clean:
            if column in data.columns:
                data[column] = (
                    data[column]
                    .astype(str)
                    .str.replace(',', '', regex=True)
                    .replace('', '')
                    .astype(str)
                    .str.replace("nan", "")
                    .apply(lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() and '.' in x and float(x).is_integer() else x)
                )
        remarks, data = remarks_input_cca(data, provider)
        if remarks == "Failed":
            log_message(LevelType.ERROR, "Unable to validate data",ErrorCode=-1)
            return {}, "Unable to validate data", False
        data['Remarks'] = remarks
        filter_columns = ['cloud_csp', 'region', INSTANCE_TYPE, 'quantity', UTILIZATION, 'pricingModel', 'Remarks']
        data = data[~data[filter_columns].apply(lambda row: all(x in ["", []] for x in row), axis=1)]
        output_dict = data.to_dict(orient='records')
        return output_dict, DATA_SUCCESS, True
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in validate_file_input_data_cca: {str(e)}",ErrorCode=-1)
        return {}, "Unable to process provided file", False

# Detect datetime values in any cell
def contains_datetime_objects(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (pd.Timestamp, datetime.datetime, datetime.date))).any():
            return True, col
    return False, None

async def validate_file_input_data_eia(file, provider):
    try:
        content = await file.read()  
        excel_data = BytesIO(content)
        data = pd.read_excel(excel_data)
        if data.empty:
            return {}, NO_DATA_IN_FILE, False
        data.columns = data.columns.astype(str)
        data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        data = data.fillna('')
        data.columns = data.columns.str.strip().str.lower()
        
        is_datetime_present, datetime_column = contains_datetime_objects(data)
        if is_datetime_present:
            return {}, f"Column '{datetime_column}' contains unsupported datetime values.", False

        data = data.replace('', '')
        data = data.loc[~(data == '').all(axis=1)].reset_index(drop=True)
        if data.shape[0] > 3000:
            return {}, "Data is too large to process", False

        required_headers = ["cloud_csp", INSTANCE_TYPE, "region", MAX_CPU, MAX_MEM_USED, MAX_NW_BW, MAX_DISK_BW, MAX_IOPS, PRICEMODEL, 'uavg', 'u95']
        if not all(header in data.columns for header in required_headers):
            val, message = cca_eia_headers_validation(data, required_headers)
            if not val:
                return {}, message, False
            
        if "uuid" not in data.columns:
            data['uuid'] = [str(uuid.uuid4()) for _ in range(len(data))]
        else:
            data['uuid'] = data['uuid'].apply(lambda x: str(uuid.uuid4()) if pd.isnull(x) or x == "" else x)   
            
        data = data.rename(columns={
            PRICEMODEL: "pricingModel"
        })

        if "cloud_csp" in data.columns:
            data["cloud_csp"] = data["cloud_csp"].apply(
                lambda x: x.upper().strip() if isinstance(x, str) and x else ""
            )
            
        remarks, data = remarks_input_eia(data, provider)
        if remarks == "Failed":
            return {}, "Unable to validate data", False
        data['Remarks'] = remarks
        output_dict = data.to_dict(orient='records')
        return output_dict, 'Uploaded file has some errors. Please click on VIEW DETAILS button to know more', True
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in validate_file_input_data_eia: {str(e)}",ErrorCode=-1)
        return {}, "Unable to process provided file", False


def quantity_validate(quantity):
    if not quantity:
        return 1, True
    if isinstance(quantity, int):
        if quantity <= 0:
            return 1, True
        return int(quantity), False
    elif isinstance(quantity, float):
        if quantity <= 0:
            return 1, True
        return math.ceil(quantity), True
    else:
        return 1, True


def hours_validate(quantity, hours):
    if not hours:
        return (730 * int(quantity)), True
    elif isinstance(hours, int):
        if hours <= 0:
            return int(730 * int(quantity)), True
        if hours > (730 * int(quantity)):
            return int(730 * int(quantity)), True
        return int(hours), False
    elif isinstance(hours, float):
        if hours <= 0:
            return int(730 * int(quantity)), True
        if math.ceil(hours) > (730 * int(quantity)):
            return int(730 * quantity), True
        else:
            return math.ceil(hours), True
    else:
        return (730 * int(quantity)), True


def value_format_auto_correction(quantity, hours, field):
    try:
        quantity = remove_commas(quantity)
        hours = remove_commas(hours)
        try:
            quantity = float(quantity)
        except Exception:
            pass
        try:
            hours = float(hours)
        except Exception:
            pass
        if field == "quantity":
            val, flag = quantity_validate(quantity)
            return val, flag
        elif field == "hours":
            val, flag = hours_validate(quantity, hours)
            return val, flag
        else:
            return None, False
    except ValueError as e:
        log_message(LevelType.ERROR, f"Error in value_format_auto_correction: {str(e)}",ErrorCode=-1)
        return None, False


def auto_correction_fileds(entry, instance_type, region, pricing_model):
    if instance_type:
        if instance_type["from"].lower().strip() == entry[INSTANCE_TYPE].lower().strip():
            entry[INSTANCE_TYPE] = instance_type["to"]
            entry["adjusted"] = True
    if region:
        if region["from"].lower().strip() == entry["region"].lower().strip():
            entry["region"] = region["to"]
            entry["adjusted"] = True
    if pricing_model:
        if pricing_model["from"].lower().strip() == entry["pricingModel"].lower().strip():
            entry["pricingModel"] = pricing_model["to"]
            entry["adjusted"] = True
    return entry


def region_instance_input_cloud_data_correct(region, instance, cloud_csp, remark_list, instance_type_data, regions_map):
    all_instance_data = get_all_instances_for_provider(cloud_csp)
    if not region:
        remark_list.append({"Field": "region", "Message": REGION_REQUIRED})
        if not instance:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": INSTANCE_ERROR})
    elif not isinstance(region, str) or (region.lower() not in regions_map[cloud_csp]):
        remark_list.append({"Field": "region", "Message": f"{region} is invalid"})
        if not instance:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": INSTANCE_ERROR})
    else:
        if not instance:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": INSTANCE_ERROR})
        if not isinstance(instance, str):
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
        elif instance.lower() not in instance_type_data and instance.lower() in all_instance_data:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is unsupported"})
        elif instance.lower() not in all_instance_data:
            remark_list.append({"Field": INSTANCE_TYPE, "Message": f"{instance} is invalid"})
    return remark_list


def input_cloud_data_correct(provider, region, instance, remark_list, regions_map, cloud_csp, instance_type_data):
    if not provider:
        remark_list.append({"Field": "cloud_csp", "Message": "Cloud is required"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif not isinstance(provider, str):
        remark_list.append({"Field": "cloud_csp", "Message": f"{provider} is invalid"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif cloud_csp != provider.upper() and provider.upper() in CLOUD_PROVIDERS:
        remark_list.append({"Field": "cloud_csp", "Message": "Cloud input should be same as Cloud Service Provider"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif provider.upper() in CLOUD_PROVIDERS and provider.upper() in UNSUPPORTED_PROVIDERS:
        remark_list.append({"Field": "cloud_csp", "Message": f"{provider} is unsupported"})
        remark_list = region_instance_validate(region, instance, remark_list)
    elif provider.upper() not in CLOUD_PROVIDERS or provider.upper() != cloud_csp:
        remark_list = region_instance_validate(region, instance, remark_list.append(
            {"Field": "cloud_csp", "Message": f"{provider} is invalid"}))
    else:
        remark_list = region_instance_input_cloud_data_correct(region, instance, cloud_csp, remark_list, instance_type_data, regions_map)
    return remark_list


def cca_input_data_correction(entry, instance_type, region, pricing_model, provider):
    all_instance_data = get_all_instances_for_provider(provider)
    entry["Remarks"] = []
    entry = {k: (v.strip() if isinstance(v, str) else v) for k, v in entry.items()}
    entry['quantity'] = value_format(entry['quantity'])
    entry[UTILIZATION] = value_format(entry[UTILIZATION])
    entry['adjusted'] = False
    entry = auto_correction_fileds(entry, instance_type, region, pricing_model)
    entry['Remarks'] = validate_fields_cca(entry['cloud_csp'], entry['region'], entry[INSTANCE_TYPE], entry['quantity'], entry[UTILIZATION], entry['pricingModel'], provider, all_instance_data, "inputcorrect")
    return entry


def eia_input_data_correction(entry, instance_type, region, pricing_model, provider):
    entry["Remarks"] = []
    entry = {k: (v.strip() if isinstance(v, str) else v) for k, v in entry.items()}
    entry['adjusted'] = False
    if instance_type:
        if instance_type["from"].lower().strip() == entry[INSTANCE_TYPE].lower().strip():
            entry[INSTANCE_TYPE] = instance_type["to"]
            entry["adjusted"] = True
    if region:
        if region["from"].lower().strip() == entry["region"].lower().strip():
            entry["region"] = region["to"]
            entry["adjusted"] = True
    if pricing_model:
        if pricing_model["from"].lower().strip() == entry["pricingModel"].lower().strip():
            entry["pricingModel"] = pricing_model["to"]
            entry["adjusted"] = True
    entry['Remarks'] = validate_fields_eia(entry["uuid"], entry['cloud_csp'], entry[INSTANCE_TYPE], entry['region'], entry[MAX_CPU], entry[MAX_MEM_USED], entry[MAX_NW_BW], entry[MAX_DISK_BW], entry[MAX_IOPS], provider, entry[PRICE_MODEL], "inputcorrect")
    return entry


def input_data_correction(provider, instance_type, region, pricing_model, input_data, app_name):
    try:
        if not input_data:
            return {}, NO_DATA_IN_FILE
        if provider not in CLOUD_PROVIDERS:
            return {}, "Invalid Cloud Service Provider"
        elif provider.upper() in CLOUD_PROVIDERS and provider.upper() in UNSUPPORTED_PROVIDERS:
            return {}, f"{provider} is unsupported"
        field_alias_map_eia = {
                "max_cpu_percent": "max cpu%",
                "instance_type": "instance type",
                "max_mem_used": "max mem used",
                "max_network_bw":"max network bw",
                "max_disk_bw_used":"max disk bw used",
                "max_iops":"max iops"
            }
        field_alias_map_cca = {
                "instance_type": "instance type",
                "monthly_utilization_hourly": "monthly utilization (hourly)"
            }
        if app_name == "CCA":
            output_data = []
            for entry in input_data:
                entry=entry.model_dump()
                for alias_key, actual_key in field_alias_map_cca.items():
                    if alias_key in entry:
                        entry[actual_key] = entry.pop(alias_key)
                entry = cca_input_data_correction(entry, instance_type, region, pricing_model, provider)
                output_data.append(entry)
        elif app_name == 'EIA':
            output_data = []
            for entry in input_data:
                entry=entry.model_dump()
                for alias_key, actual_key in field_alias_map_eia.items():
                    if alias_key in entry:
                        entry[actual_key] = entry.pop(alias_key)
                entry = eia_input_data_correction(entry, instance_type, region, pricing_model, provider)
                output_data.append(entry)
        else:
            return {}, "Invalid application selected"
        return output_data, DATA_SUCCESS
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in input_data_correction: {str(e)}",ErrorCode=-1)
        return {}, "Unable to process provided data"


async def read_udf_file_data(input_data):
    try:
        if isinstance(input_data, list):
            data_dict = input_data
        else:
            content = await input_data.read()
            excel_data = BytesIO(content)
            data = pd.read_excel(excel_data, engine='openpyxl')
            data.columns = [col if 'Unnamed' not in col else '' for col in data.columns]
            data = data.fillna('')
            data_dict = data.to_dict(orient='records')
        return data_dict, 'Empty file uploaded for Self Perf Assessment'
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in read_udf_file_data: {str(e)}",ErrorCode=-1)
        return {}, "Failed to validate data"


async def file_upload_validate_service(request, params, app_name):
    """
    Service: Validate input file from S3 and return structured validation response.
    """
    try:
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        portfolio = await portfolio_collection.find_one({
            "_id": ObjectId(params.portfolio_id),
            "app_name": app_name.upper()
        })
        if not portfolio:
            raise CustomAPIException(status_code=404, message="Portfolio not found")

        cloud_csp = portfolio.get("cloud_provider", "").strip().upper()
        app = portfolio.get("app_name")
        s3_key = portfolio.get("s3_key")
        udf_key = portfolio.get("udf_key", None)
        is_billing_data = portfolio.get("is_billing_data", False)

        udf_file = None

        instance_data_file = fetch_s3_file(s3_key)
        if udf_key:
            udf_file = fetch_s3_file(udf_key)

        # Validate arguments
        validation_response = await file_args_validation(cloud_csp, app, udf_file, instance_data_file)
        if validation_response:
            return validation_response

        udf_data = ""

        # Validate based on app type
        if app == "CCA":
            if not instance_data_file:
                log_message(LevelType.ERROR, "Input file not found in S3", request=request, ErrorCode=-1)
                raise CustomAPIException(status_code=400, message="Input file not found in S3", error_code=-1)

            data, message, flag = await validate_file_input_data_cca(instance_data_file, cloud_csp, params.portfolio_id, is_billing_data)

        elif app == "EIA":
            data, udf_data, message, _, flag = await eia_data_read(instance_data_file, udf_file, cloud_csp)

        else:
            log_message(LevelType.ERROR, "Invalid application provided", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid application provided", error_code=-1)

        if not data:
                log_message(LevelType.ERROR, f"Validation failed: {message}", request=request, ErrorCode=-1)
                raise CustomAPIException(status_code=400, message=message, error_code=-1)
        if udf_file and not udf_data:
                log_message(LevelType.ERROR, f"Validation failed: {message}", request=request, ErrorCode=-1)
                raise CustomAPIException(status_code=400, message=message, error_code=-1)
        
        if not flag:
            log_message(LevelType.ERROR, f"Validation failed: {message}", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Input validation failed", error_code=-1)

        # Construct response
        if app == "CCA":
            return {
                "Message": "Data validated successfully",
                "Provider": cloud_csp,
                "Data": data,
                "ErrorCode": 1,
            }
        else:
            return {
                "Message": "Data validated successfully",
                "Provider": cloud_csp,
                "Data": data,
                "UDF": udf_data,
                "ErrorCode": 1,
            }
    except CustomAPIException:
        raise

    except Exception as e:
        log_message(LevelType.ERROR, f"Unable to validate input data {str(e)}", ErrorCode=-1)
        raise CustomAPIException(
            status_code=500,
            message=f"Unable to validate input data: {str(e)}"
        )

async def read_udf_file_data_for_large(input_data):
    try:
        if isinstance(input_data, list):
            data_dict = input_data
        else:
            content = await input_data.read()
            excel_data = BytesIO(content)
            data = pd.read_excel(excel_data, engine='openpyxl')

            # Replace unnamed columns with blanks
            data.columns = [col if 'Unnamed' not in col else '' for col in data.columns]

            # Replace NaNs with empty strings
            data = data.fillna('')

            # Clean whitespace-only cells and drop fully empty rows
            data = data.replace(r'^\s*$', pd.NA, regex=True)
            data = data.dropna(how="all")

            # --- Validation 1: Empty or only headers ---
            if data.empty or data.shape[0] == 0:
                error_msg = "Unable to validate input data: Uploaded file contains only headers or no data rows"
                log_message(LevelType.ERROR, error_msg, ErrorCode=-1)
                raise CustomAPIException(status_code=500, message=error_msg)

            # --- Validation 2: Must have at least 2 columns ---
            if data.shape[1] < 2:
                error_msg = "Unable to validate input data: File must contain at least 2 columns"
                log_message(LevelType.ERROR, error_msg, ErrorCode=-1)
                raise CustomAPIException(status_code=500, message=error_msg)

            # --- Combined Validation: column data types ---
            first_col_name = data.columns[0]
            second_col_name = data.columns[1]
            first_col = data.iloc[:, 0]
            second_col = data.iloc[:, 1]

            invalid_first_col = not first_col.map(lambda x: isinstance(x, str)).all()
            invalid_second_col = not pd.to_numeric(second_col, errors='coerce').notna().all()

            if invalid_first_col or invalid_second_col:
                error_parts = []
                if invalid_first_col:
                    error_parts.append(f"column '{first_col_name}' must contain only string values")
                if invalid_second_col:
                    error_parts.append(f"column '{second_col_name}' must contain only numeric values")

                error_msg = "Unable to validate input data: " + " and ".join(error_parts)
                log_message(LevelType.ERROR, error_msg, ErrorCode=-1)
                raise CustomAPIException(status_code=500, message=error_msg)

            # --- Keep only first 2 columns ---
            data_dict = data.iloc[:, :2].to_dict(orient='records')

        return data_dict, 'Empty file uploaded for Self Perf Assessment'

    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in read_udf_file_data_for_large: {str(e)}", ErrorCode=-1)
        return {}, "Failed to validate data"