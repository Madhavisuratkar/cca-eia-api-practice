from fastapi import APIRouter, Depends, Request, Query
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import ApplicationEndpoints, LevelType
from app.connections.pylogger import log_message
from app.services.insights_service import get_insights_data_service, get_dashboard_analytics_service, get_dashboard_summary_service
from app.schema.insights_schema import InsightsRequest

insights_router = APIRouter()

@insights_router.get(ApplicationEndpoints.INSIGHTS, tags=["Insights"])
async def get_insights_data(request: Request, params: InsightsRequest = Depends()):
    """
    Fetch insights data
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        token = request.headers.get("Authorization")
        return await get_insights_data_service(
            params.insight, 
            params.get_filter_list(), 
            app_name,
            user_email,
            token
        )
    except ValueError as val_err:
        log_message(LevelType.ERROR, f"Insights Validation Failed: {str(val_err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=422, message=str(val_err), error_code=-1)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Insights Fetch Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Insights Fetch Failed", error_code=-1)

@insights_router.get(ApplicationEndpoints.DASHBOARD_ANALYTICS, tags=["Dashboard"])
async def get_dashboard_analytics(
    request: Request
):
    """
    Fetch aggregated dashboard analytics data
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        return await get_dashboard_analytics_service(app_name, user_email)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Dashboard Analytics Fetch Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Dashboard Analytics Fetch Failed", error_code=-1)

@insights_router.get(ApplicationEndpoints.DASHBOARD_CLIENT_SUMMARY, tags=["Dashboard"])
async def get_dashboard_summary(request: Request):
    """
    Dashboard summary: portfolio count, instances count, client count, active clients
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        return await get_dashboard_summary_service(user_email, app_name)  
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Dashboard Summary Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Dashboard summary fetch failed", error_code=-1)