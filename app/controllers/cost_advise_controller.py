"""
This File will perform costadvice operations for user requests
"""
from fastapi.params import Query
from app.schema.portfolio_model_without_cloud import SavePortfolioRequest
from fastapi import APIRouter, Request, Depends
from app.connections.custom_exceptions import CustomAPIException
from app.schema.cost_advise_schema import CostAdviseRequest, GetRecommendationsRequest
from app.services.cost_advise_service import process_cost_advise, process_get_recommendations, process_last_cost_advise
from app.utils.constants import ApplicationEndpoints, ApplicationModuleTag, LevelType
from app.connections.pylogger import log_message
from sqlalchemy.orm import Session
from app.utils.cs_database import get_db
from typing import Optional

cost_advise_router = APIRouter()

@cost_advise_router.post(ApplicationEndpoints.COST_ADVISE, tags=[ApplicationModuleTag.COST_ADVICE])
async def cost_advise(
    request: Request,
    payload: CostAdviseRequest,
    db: Session = Depends(get_db),
):
    """
    This method will provide the costadvice for user provided requests
    """
    try:
        user_email = payload.user_email or request.state.user_email
        app_name = request.state.app_name
        app_name = app_name.upper()
        return await process_cost_advise(db, user_email, app_name, payload.dict(by_alias=True))
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to fetch cost advise data: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to fetch cost advise data: {str(e)}", error_code=-1)

@cost_advise_router.post(ApplicationEndpoints.RECOMMENDATIONS, tags=[ApplicationModuleTag.COST_ADVICE])
async def get_recommendations(
    request: Request,
    payload: SavePortfolioRequest,
    db: Session = Depends(get_db),
):
    """
    This method will provide the recommendations for the user provided metrics
    """
    try:
        app_name = request.state.app_name
        app_name = app_name.upper()
        user_email = request.state.user_email
        ipaddr = request.client.host
        return await process_get_recommendations(user_email, app_name, payload.dict(by_alias=True), ipaddr, db)
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to fetch recommendations: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to fetch recommendations: {str(e)}", error_code=-1)


@cost_advise_router.get(ApplicationEndpoints.LAST_RECOMMENDATIONS, tags=[ApplicationModuleTag.LAST_RECOMMENDATIONS])
async def latest_cost_advise(
    request: Request,
    portfolio_id: str,
    page: int = 1,
    page_size: int = 10,
    user_email: str = Query(..., description="User email to override header/middleware value"),
    is_chart_value: Optional[bool] = False,
):
    """
    This method will provide the costadvice for user provided requests
    """
    try:
        final_user_email  = user_email or request.state.user_email
        app_name = request.state.app_name
        app_name = app_name.upper()
        return await process_last_cost_advise(final_user_email , app_name, portfolio_id, page, page_size, is_chart_value)
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to fetch cost advise data: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to fetch cost advise data: {str(e)}", error_code=-1)