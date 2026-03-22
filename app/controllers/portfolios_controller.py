from app.connections.custom_exceptions import CustomAPIException
from fastapi import APIRouter, Request, Depends, Query, Path, Body
from sqlalchemy.orm import Session
from typing import Optional
from app.utils.constants import ApplicationEndpoints, ApplicationModuleTag, LevelType
from app.schema.portfolio_model_without_cloud import PortfolioLockRequest, SavePortfolioRequest, PortfolioFilter
from app.schema.portfolio_with_cloud_schema import CloudAccountSchema, QueryTypeCloudCred
from app.services.portfolios_service import (
    delete_current_instance_data,
    get_portfolios_common_data,
    handle_cloud_account_service,
    save_portfolio_data,
    patch_portfolio_data,
    delete_portfolio_data,
    add_cloud_account_service,
    list_portfolios_service,
    process_lock_unlock_portfolio
)
from app.connections.pylogger import log_message
from app.utils.cs_database import get_db

portfolios_router = APIRouter()

@portfolios_router.post(
    ApplicationEndpoints.SAVE_PORTFOLIO,
    tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS]
)
async def save_portfolio_endpoint(
    request: Request,
    payload: SavePortfolioRequest,
    db: Session = Depends(get_db)
):
    """
    Method allow to save the portfolio of the user
    """
    try:
        app_name = request.state.app_name
        user_email = request.state.user_email
        ipaddr = request.client.host
        return await save_portfolio_data(db, payload, app_name, ipaddr, user_email)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Portfolio create failed due to {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=str(err))


@portfolios_router.get(
    ApplicationEndpoints.GET_PORTFOLIO,
    tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS]
)
async def get_all_portfolios_endpoint(
    request: Request,
    provider: Optional[str] = Query(None,description="Cloud provider",example="AWS"),
    id: Optional[str] = Query(None,description="Portfolio ID",example="507f1f77bcf86cd799439011"),
    cloud_csp: Optional[str] = Query(None,description="Cloud provider",example="AWS"),
    list_all: Optional[bool] = Query(None,description="List all the portfolios",example=False),
    is_billing_data: bool = Query(False, description="Is billing data request?", example=False)
):
    """
    Fetch all portfolios for the given user, provider, and cloud CSP.
    """
    try:
        user_email = request.query_params.get("user_email") or request.state.user_email  # Set by auth middleware
        app_name = request.state.app_name
        token = request.headers.get("Authorization")
        # store in request.state
        if request:
            request.state.portfolio_id = id
        return await get_portfolios_common_data(provider, id, cloud_csp, list_all, app_name, user_email, token, is_billing_data)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to fetch portfolios due to {str(err)}", ErrorCode=-1, portfolio_id=id)
        raise CustomAPIException(status_code=500, message="Unable to fetch portfolios")

@portfolios_router.patch(
    ApplicationEndpoints.UPDATE_PORTFOLIO,
    tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS]
)
async def rename_portfolio_endpoint(request: Request, id: str = Query(...,description="Portfolio ID to update", example="507f1f77bcf86cd799439011"), db: Session = Depends(get_db)):
    """
    Rename an existing portfolio.
    """
    try:
        app_name = request.state.app_name
        user_email = request.state.user_email
        if request:
            request.state.portfolio_id = id
        if not app_name:
            raise CustomAPIException(status_code=400, message="Missing Appname in headers")
        request_body = await request.json()
        return await patch_portfolio_data(request_body, id, app_name, db, user_email)
    except CustomAPIException:
        raise 
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to update portfolios due to {str(err)}", ErrorCode=-1, portfolio_id=id)
        raise CustomAPIException(status_code=500, message="Unable to update portfolio")


@portfolios_router.delete(
    ApplicationEndpoints.DELETE_PORTFOLIO,
    tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS]
)
async def delete_portfolio_endpoint(request: Request, id: str = Query(...,description="Portfolio ID to delete", example="507f1f77bcf86cd799439011")):
    """
    Delete a specific portfolio by name.
    """
    try:
        if request:
            request.state.portfolio_id = id
        app_name = request.state.app_name
        user_email = request.state.user_email
        return await delete_portfolio_data(_id=id, app_name=app_name, user_email=user_email)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to delete portfolios due to {str(err)}", ErrorCode=-1, portfolio_id=id)
        raise CustomAPIException(status_code=500, message="Unable to delete portfolio")

@portfolios_router.delete(
    ApplicationEndpoints.DELETE_INSTANCE,
    tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS]
)
async def delete_current_instances(
    ids: list[str] = Query(..., description="List of Current Instance IDs to delete", example=["64ef1f77bcf86cd799439012", "64ef1f77bcf86cd799439013"])
):
    """
    Delete one or multiple current instances by IDs.
    """
    try:
        return await delete_current_instance_data(ids)
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to delete current instances due to {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to delete current instances")

@portfolios_router.post(
    ApplicationEndpoints.PORTFOLIO_WITH_CRED_ACCOUNT,
    tags=[ApplicationModuleTag.PORTFOLIO_WITH_CLOUD_CONTROLLERS]
)
async def add_cloud_account(payload: CloudAccountSchema, request: Request, db: Session = Depends(get_db)):
    """
    Method to add cloud account for getting recommendations
    """
    ipaddr = request.client.host
    user_email = payload.user_email or request.state.user_email
    app_name = request.state.app_name

    try:
        return await add_cloud_account_service(db, payload, user_email, app_name, ipaddr, ApplicationEndpoints.PORTFOLIO_WITH_CRED)
    except CustomAPIException as e:
        raise e
    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to add cloud account: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=str(e))

@portfolios_router.get(
    ApplicationEndpoints.PORTFOLIO_WITH_CRED,
    tags=[ApplicationModuleTag.PORTFOLIO_WITH_CLOUD_CONTROLLERS]
)
async def handle_cloud_account_actions(
    request: Request,
    query_type: QueryTypeCloudCred = Path(..., description="Telemetry source type"),
    provider: Optional[str] = Query(None, description="Cloud provider", example="AWS"),
    user_email_param: Optional[str] = Query(None, description="User email",example="user@example.com"),
    id: Optional[str] = Query(None, description="Account portfolio ID",example="507f1f77bcf86cd799439011"),
):
    """
    Method to allow cloud related instances data save in portfolio
    """
    ipaddr = request.client.host
    user_email = user_email_param or request.query_params.get("user_email") or request.state.user_email
    app_name = request.state.app_name

    try:
        if request:
            request.state.portfolio_id = id
        return await handle_cloud_account_service(
            query_type=query_type,
            provider=provider,
            user_email=user_email,
            app_name=app_name,
            ipaddr=ipaddr,
            request=request,
            _id=id
        )
    except CustomAPIException as e:
        raise e
    except Exception as err:
        log_message(LevelType.ERROR, f"Cloud account action failed: {str(err)}", ErrorCode=-1, portfolio_id=id)
        raise CustomAPIException(status_code=500, message=str(err))


@portfolios_router.get(ApplicationEndpoints.LIST_PORTFOLIOS,tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS])
async def list_portfolios(request: Request, filters: PortfolioFilter = Depends()):
    """
    List portfolios with dynamic filters.
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        token = request.headers.get("Authorization")


        return await list_portfolios_service(token, filters, user_email, app_name)

    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Failed to list portfolios: {err}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to list portfolios")


@portfolios_router.patch(ApplicationEndpoints.LOCK_UNLOCK_PORTFOLIO,tags=[ApplicationModuleTag.PORTFOLIO_WITHOUT_CLOUD_CONTROLLERS])
async def lock_unlock_portfolio(request: Request,payload: PortfolioLockRequest, portfolio_id: str = Query(..., description="Portfolio ID to lock/unlock")):
    """
    Lock or unlock portfolio (is_locked = true/false)
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name.upper()

        request.state.portfolio_id = portfolio_id
        return await process_lock_unlock_portfolio(portfolio_id=portfolio_id, user_email=user_email, app_name=app_name, is_locked=payload.is_locked)
    except CustomAPIException:
        raise

    except Exception as e:
        log_message(LevelType.ERROR, f"Failed to update lock state: {str(e)}", ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(status_code=500, message=f"Failed to update lock state: {str(e)}", error_code=-1)
