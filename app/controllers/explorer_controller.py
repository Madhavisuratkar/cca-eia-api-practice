from app.utils.cs_database import get_db
from fastapi import APIRouter, Request, HTTPException, Depends
from app.utils.constants import (
    ApplicationEndpoints,
    ApplicationModuleTag,
    FAILED_REGION,
    LevelType,
)
from app.schema.explorer_schema import (
    InstanceSizesQueryModel,
    InstanceSummaryQueryModel,
    CloudInstancesQueryModel,
    ExplorerQueryModel,
    RegionsQueryModel,
    regions_query,
    cloud_instances_query,
    instance_sizes_query,
    explorer_query,
    instance_summary_query
)
from app.services.explorer_service import (
    get_regions_service,
    get_instance_sizes_service,
    get_explorer_service,
    get_instance_summary_service,
    get_cloud_instances_service,
)

from app.connections.custom_exceptions import CustomAPIException
from app.connections.pylogger import log_message
from sqlalchemy.orm import Session

explorer_router = APIRouter()

@explorer_router.get(ApplicationEndpoints.GET_REGIONS, tags=[ApplicationModuleTag.EXPLORER])
async def get_regions_endpoint(
    validated: RegionsQueryModel = Depends(regions_query),
    request: Request = None,
):
    """
    Method to get all the regions data of provided cloud provider
    """
    try:
        return await get_regions_service(
            provider=validated.provider.upper(),
            app_name=request.state.app_name
        )
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to get regions data", error_code=-1)

@explorer_router.get(ApplicationEndpoints.GET_INSTANCE_SIZES, tags=[ApplicationModuleTag.EXPLORER])
async def get_instance_sizes_endpoint(
    validated: InstanceSizesQueryModel = Depends(instance_sizes_query),
    request: Request = None,
):
    """
    method to get all instances avaialbel in the particular cloud provider and region
    """
    try:
        return await get_instance_sizes_service(
            provider=validated.provider.upper(),
            region=validated.region.lower(),
            app_name=request.state.app_name,
        )
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=FAILED_REGION, error_code=-1)

@explorer_router.get(ApplicationEndpoints.EXPLORER, tags=[ApplicationModuleTag.EXPLORER])
async def get_explorer_endpoint(
    validated: ExplorerQueryModel = Depends(explorer_query),
    request: Request = None,
    db: Session = Depends(get_db)
):
    """
    Method to get all the available AMD instances for particular provider and region
    """
    try:
        return await get_explorer_service(
            provider=validated.provider.upper(),
            region=validated.region.lower() if validated.region else None,
            app_name=request.state.app_name,
            db=db
        )
    except HTTPException as err:
        raise err
    except Exception as err:
        log_message(LevelType.ERROR, f"Error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to get explorer instances", error_code=-1)

@explorer_router.get(ApplicationEndpoints.INSTANCE_SUMMARY, tags=[ApplicationModuleTag.EXPLORER])
async def get_instance_summary_endpoint(
    validated: InstanceSummaryQueryModel = Depends(instance_summary_query),
    request: Request = None,
):
    """
    Method to get all the regions and instances available under the region for particular cloud provider
    """
    try:
        return await get_instance_summary_service(
            provider=validated.provider.upper(),
            app_name=request.state.app_name,
        )
    except HTTPException as err:
        raise err
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch instance summary data: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch instance summary data", error_code=-1)

@explorer_router.get(ApplicationEndpoints.GET_CLOUD_INSTANCES, tags=[ApplicationModuleTag.EXPLORER])
async def get_cloud_instances_endpoint(
    validated: CloudInstancesQueryModel = Depends(cloud_instances_query),
    request: Request = None,
):
    """
    Method to get all the instances available for the provider
    """
    try:
        return await get_cloud_instances_service(
            provider=validated.provider.upper(),
            app_name=request.state.app_name,
        )
    except HTTPException as err:
        raise err
    except Exception as err:
        log_message(LevelType.ERROR, f"Error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch instance summary data", error_code=-1)