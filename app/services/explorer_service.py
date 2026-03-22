import os
import subprocess
import pandas as pd
import asyncio
import h5py
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import (
    FAILED_REGION, FAILED_INSTANCE, INVALID_APP,
    REQUERED_FIELD_ERROR, PRICING_DATABASE, CLOUD_DATABASE,
    EXPLORER_FILE, CCA_DEFICIENT_FILE, CLOUD_PROVIDERS, UNSUPPORTED_PROVIDERS,
    LevelType, VALID_APPS
)
from app.utils.explore_utils import (
    check_hdf5_regions, check_hdf5_instance, get_all_instances_for_provider
)
from app.utils.application_validation_utils import validate_app_name_db
from app.connections.pylogger import log_message
from sqlalchemy.orm import Session

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATABASE_PATH = os.path.join(ROOT_DIR, 'database')


async def get_regions_service(provider: str, app_name: str):
    try:
        await asyncio.sleep(0)
        if not provider or not app_name:
            log_message(LevelType.ERROR, REQUERED_FIELD_ERROR, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR)
        if app_name not in VALID_APPS:
            log_message(LevelType.ERROR, INVALID_APP, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=INVALID_APP)
        h5_file_path = os.path.join(DATABASE_PATH, PRICING_DATABASE)
        if not os.path.exists(h5_file_path):
            log_message(LevelType.ERROR, FAILED_REGION, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=FAILED_REGION)
        with h5py.File(h5_file_path, "r") as hdf:
            valid, _ = check_hdf5_regions(hdf, provider)
            if not valid:
                log_message(LevelType.ERROR, f"Provider '{provider}' not found", ErrorCode=-1)
                raise CustomAPIException(status_code=404, message=f"Provider '{provider}' not found")
            regions = list(hdf[provider].keys())
        log_message(LevelType.INFO, "Regions fetched successfully", ErrorCode=1)
        return {
            "Message": "Regions data fetched successfully",
            "ErrorCode": 1,
            "cloud_provider": provider,
            "regions": regions
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error: {e}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=FAILED_REGION)


async def get_instance_sizes_service(provider: str, region: str, app_name: str):
    try:
        await asyncio.sleep(0)
        if not provider or not region or not app_name:
            log_message(LevelType.ERROR, f"Error: {REQUERED_FIELD_ERROR}", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR)
        if app_name not in VALID_APPS:
            log_message(LevelType.ERROR, f"Error: {INVALID_APP}", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=INVALID_APP)
        h5_file_path = os.path.join(DATABASE_PATH, PRICING_DATABASE)
        if not os.path.exists(h5_file_path):
            log_message(LevelType.ERROR, f"Error: {FAILED_INSTANCE}", ErrorCode=-1)
            raise CustomAPIException(status_code=500, message=FAILED_INSTANCE)
        with h5py.File(h5_file_path, 'r') as hdf:
            if provider not in hdf:
                log_message(LevelType.ERROR, f"Provider '{provider}' not found in HDF5 file.", ErrorCode=-1)
                raise CustomAPIException(status_code=404, message=f"Provider '{provider}' not found")
            if region not in hdf[provider]:
                log_message(LevelType.ERROR, f"Region '{region}' not found under provider '{provider}'.", ErrorCode=-1)
                raise CustomAPIException(status_code=404, message=f"Region '{region}' not found")
            region_group = hdf[provider][region]
            if 'Instance' in region_group:
                instances = list(region_group['Instance'][:].astype(str))
                return {"Message": "Instance data fetched successfully", "ErrorCode": 1, "cloud_provider": provider, "region": region, "instances": instances}
            else:
                log_message(LevelType.ERROR, f" No instances found for the specified region {region}", ErrorCode=-1)
                raise CustomAPIException(status_code=404, message=f"No instances found for the specified region {region}")
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error: {e}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=FAILED_INSTANCE)


async def get_explorer_service(provider: str, region: str,app_name: str, db: Session):
    try:
        await asyncio.sleep(0)
        if not provider or not app_name:
            log_message(LevelType.ERROR, REQUERED_FIELD_ERROR, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR)
        
        if not validate_app_name_db(app_name, db):
            log_message(LevelType.ERROR, f"Application name '{app_name}' not found", ErrorCode=-1)
            raise CustomAPIException(status_code=404, message=f"Application name '{app_name}' not found")

        region_arg = region.lower() if region else ""
        command = (
            f"bash run.sh {os.path.join(str(ROOT_DIR), str(CCA_DEFICIENT_FILE))} "
            f"{os.path.join(str(ROOT_DIR), str(EXPLORER_FILE))} EXP {provider} {region_arg}"
        )
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.stdout and "HDF5-DIAG: Error" not in result.stdout:
            if os.path.exists(EXPLORER_FILE):
                df = pd.read_csv(EXPLORER_FILE)
                df.columns = [
                    "region", "instance", "vCPU", "memory(GB)",
                    "instancePricingOndemand", "instancePricingReserved",
                    "Instance_Pricing_Spot", "CPU_Generation"
                ]
                log_message(LevelType.INFO, "Explorer data fetched successfully", ErrorCode=1)
                return {
                    "Message": "Explorer data fetched successfully",
                    "ErrorCode": 1,
                    "cloud_provider": provider,
                    "region": region.lower() if region else "All regions",
                    "Data": df.to_dict(orient="records")
                }
        log_message(LevelType.ERROR, "Failed to fetch explorer data", ErrorCode=-1)
        raise CustomAPIException(status_code=404, message="Failed to fetch explorer data")
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error: {e}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to get explorer instances")


async def get_instance_summary_service(provider: str, app_name: str):
    try:
        await asyncio.sleep(0)
        if not provider or not app_name:
            log_message(LevelType.ERROR, REQUERED_FIELD_ERROR, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR)
        if app_name not in VALID_APPS:
            log_message(LevelType.ERROR, INVALID_APP, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=INVALID_APP)
        if provider not in CLOUD_PROVIDERS:
            log_message(LevelType.ERROR, "Invalid cloud provider", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid cloud provider")
        if provider in UNSUPPORTED_PROVIDERS:
            log_message(LevelType.ERROR, "Unsupported cloud provider", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Unsupported cloud provider")

        h5_file_path = os.path.join(DATABASE_PATH, CLOUD_DATABASE)
        if not os.path.exists(h5_file_path):
            log_message(LevelType.ERROR, FAILED_INSTANCE, ErrorCode=-1)
            raise CustomAPIException(status_code=500, message=FAILED_INSTANCE)

        data = {}
        with h5py.File(h5_file_path, "r") as hdf:
            valid, _ = check_hdf5_regions(hdf, provider)
            if not valid:
                log_message(LevelType.ERROR, f"Provider '{provider}' not found", ErrorCode=-1)
                raise CustomAPIException(status_code=404, message=f"Provider '{provider}' not found")

            for region in hdf[provider].keys():
                _, _ = check_hdf5_instance(hdf, provider, region)
                instances = list(hdf[f"{provider}/{region}"]["Instance"][:].astype(str)) if "Instance" in hdf[f"{provider}/{region}"] else []
                data[region] = instances

        log_message(LevelType.INFO, "Cloud instances summary fetched successfully", ErrorCode=1)
        return {
            "Message": "Cloud instances data fetched successfully",
            "ErrorCode": 1,
            "cloud_provider": provider,
            "Data": data
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error: {e}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=FAILED_INSTANCE)


async def get_cloud_instances_service(provider: str, app_name: str):
    try:
        await asyncio.sleep(0)
        if not provider or not app_name:
            log_message(LevelType.ERROR, REQUERED_FIELD_ERROR, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=REQUERED_FIELD_ERROR)
        if app_name not in VALID_APPS:
            log_message(LevelType.ERROR, INVALID_APP, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=INVALID_APP)
        instances = get_all_instances_for_provider(provider)
        if instances:
            log_message(LevelType.INFO, "Cloud instances fetched successfully", ErrorCode=1)
            return {
                "Message": "Instance data fetched successfully",
                "ErrorCode": 1,
                "cloud_provider": provider,
                "instances": instances
            }
        log_message(LevelType.ERROR, "No instances found for the specified provider", ErrorCode=-1)
        raise CustomAPIException(status_code=404, message="No instances found for the specified provider")
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error: {e}", ErrorCode=-1)
        CustomAPIException(status_code=500, message=FAILED_INSTANCE)