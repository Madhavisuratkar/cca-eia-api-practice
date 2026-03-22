from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from app.connections.custom_exceptions import CustomAPIException
from app.schema.bulk_upload_schema import BulkUploadQuery, CostAdviceRequestSchema
from app.services.bulk_upload_service import get_excel_row_count, get_recommendation_progress_service, process_generate_upload_url, start_cost_advice_recommendation
from app.utils.constants import ApplicationEndpoints, LevelType
from app.connections.pylogger import log_message
from app.utils.cs_database import get_db

bulk_router = APIRouter()


@bulk_router.get(ApplicationEndpoints.GENERATE_UPLOAD_URL, tags=["Bulk Upload"])
async def generate_upload_url(
    request: Request,
    query: BulkUploadQuery = Depends(),
    db: Session = Depends(get_db)
):
    """
    Generate a pre-signed S3 upload URL and create or update portfolio metadata.
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        ipaddr = request.client.host

        # Delegate to service
        response = await process_generate_upload_url(app_name, user_email, ipaddr, query, db)

        return response

    except CustomAPIException:
        raise
    except Exception as e:
        log_message(
            LevelType.ERROR,
            f"{ApplicationEndpoints.GENERATE_UPLOAD_URL} unexpected error: {str(e)}",
            ErrorCode=-1
        )
        raise CustomAPIException(status_code=500, message="Internal Server Error", error_code=-1)


@bulk_router.post(ApplicationEndpoints.PORTFOLIO_COST_ADVICE, tags=["Bulk Upload"])
async def portfolio_cost_advice(
    request: Request,
    request_data: CostAdviceRequestSchema
):
    try:
        portfolio_id = request_data.portfolio_id
        app_name = request.state.app_name
        return await start_cost_advice_recommendation(
            portfolio_id=portfolio_id,
            app_name=app_name,
        )
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"{ApplicationEndpoints.PORTFOLIO_COST_ADVICE} unexpected error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Internal Server Error", error_code=-1)


@bulk_router.get(ApplicationEndpoints.GET_EXCEL_ROWCOUNT, tags=["Bulk Upload"]) # fetch-file-size
async def get_row_count_endpoint(request: Request, portfolio_id: str):
    """
    Returns the total row count of an Excel file stored in S3 quickly.
    """
    try:
        app_name = request.state.app_name

        result = await get_excel_row_count(portfolio_id, app_name)
        return result

    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Row count failed for {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=str(err))


@bulk_router.get(ApplicationEndpoints.JOB_STATUS, tags=["Bulk Upload"])
async def get_recommendation_progress(
    request: Request,
    portfolio_id: str
):
    """
    Get recommendation progress for a given portfolio.
    """
    try:
        app_name = request.state.app_name
        response = await get_recommendation_progress_service(portfolio_id, app_name.upper())
        return response
    
    except CustomAPIException as e:
        raise e
    except Exception as e:
        log_message(LevelType.ERROR, f"get_recommendation_progress error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Internal Server Error")