from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional
from app.connections.custom_exceptions import CustomAPIException
from app.services.etl_service import get_features_endpoint_summary_count_data, fetch_jenkins_data, fetch_sonar_data, get_organisation_summary_data, get_savings_data, get_metrics_data
from app.utils.constants import (
    ApplicationEndpoints,
    ApplicationModuleTag,
    LevelType)
from app.connections.pylogger import log_message

etl_router = APIRouter()

@etl_router.get(ApplicationEndpoints.SAVINGS, tags=[ApplicationModuleTag.ETL_OPERATIONS])
async def get_savings(
    request: Request,
    date_filter: Optional[str] = Query(None,description="Date range filter to specify the time period of the data to retrieve.",example="All")
):
    """
    Method to fetch ETL savings data
    """
    try:
        app_name = request.headers.get("Appname").upper()
        return await get_savings_data(app_name, date_filter)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch Savings data: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch Savings data", error_code=-1)
    

@etl_router.get(ApplicationEndpoints.MATRICS, tags=[ApplicationModuleTag.ETL_OPERATIONS])
async def get_metrics(
    request: Request,
    date_filter: Optional[str] = Query(None,description="Date range filter to specify the time period of the data to retrieve.",example="All")
):
    """
    Method to fetch ETL metrics data
    """
    try:
        app_name = request.headers.get("Appname", "").upper()
        return await get_metrics_data(app_name, date_filter)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch Metrics data: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch Metrics data", error_code=-1)
    


@etl_router.get(ApplicationEndpoints.ORGANIZATION, tags=[ApplicationModuleTag.ETL_OPERATIONS])
async def get_organisation(
    request: Request,
    date_filter: Optional[str] = Query(None, description="Date range filter: 30, 60, 90, or All", example="All")
):
    """
    Method to fetch organization summary data from org_user_summary collection
    """
    try:
        app_name = request.headers.get("Appname", "").upper()
        if not app_name:
            log_message(LevelType.ERROR, "Appname header missing.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Appname header missing", error_code=-1)

        response = await get_organisation_summary_data(app_name, date_filter)
        return response

    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch organisation data: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch organisation data", error_code=-1)


@etl_router.get(ApplicationEndpoints.FEATURES_COUNT, tags=[ApplicationModuleTag.ETL_OPERATIONS])
async def get_features_count(
    request: Request,
    date_filter: Optional[str] = Query(None,description="Date range filter to specify the time period of the data to retrieve.",example="All")
):
    """
    Method to fetch feature counts cunsumed
    """
    try:
        app_name = request.headers.get("Appname", "").upper()
        return await get_features_endpoint_summary_count_data(app_name, date_filter.strip())
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch feature count data: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to fetch feature count data", error_code=-1)
    

@etl_router.get(ApplicationEndpoints.JENKINS_DATA, tags=[ApplicationModuleTag.ETL_OPERATIONS])
def get_jenkins_data(
    request: Request,
    job_name: str,
    tree: Optional[str] = Query(None),
):
    """"
    Method to fetch jenkins information
    """
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            log_message(LevelType.ERROR, "Authorization header is required.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Authorization header is required", error_code=-1)
        if not tree:
            log_message(LevelType.ERROR, "Tree parameter is required.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Tree parameter is required", error_code=-1)

        return fetch_jenkins_data(job_name, tree, auth_header)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, str(err), ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=str(err), error_code=-1)
    

@etl_router.get(ApplicationEndpoints.SONAR_DATA, tags=[ApplicationModuleTag.ETL_OPERATIONS])
def get_sonar_data(
    request: Request,
    component: str = Query(None),
    metrics: str = Query(None),
    from_date: str = Query(None, alias="from"),
    to_date: str = Query(None, alias="to"),
):
    """
    Method to fetch sonar data
    """
    try:
        token = request.headers.get("Authorization")
        if not component or not metrics or not from_date or not to_date:
            log_message(LevelType.ERROR, "Missing required parameters.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Missing required parameters.", error_code=-1)
        if not token:
            log_message(LevelType.ERROR, "Authorization header is required.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Authorization header is required.", error_code=-1)

        return fetch_sonar_data("featuresCount", component, metrics, from_date, to_date, token)

    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, str(err), ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=str(err), error_code=-1)