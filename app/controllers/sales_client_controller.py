from fastapi import APIRouter, Request
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import ApplicationEndpoints, LevelType
from app.connections.pylogger import log_message
from app.services.sales_client_service import (
    list_sales_clients_service,
    add_sales_client_service,
    delete_sales_client_service, toggle_favorite_sales_client_service,
    list_unique_portfolio_users_service
)
from app.schema.sales_client_schema import AddSalesClientSchema 

sales_client_router = APIRouter()


# ============================
# LIST SALES CLIENTS
# ============================
@sales_client_router.get(ApplicationEndpoints.SALES_CLIENT_LIST, tags=["Sales Client"])
async def list_sales_clients(request: Request, page: int = 1, page_size: int = 10, client_name: str | None = None, unselected: bool = False):
    """
    List sales clients including user's favorites
    """
    try:    
        user_email = request.state.user_email
        app_name = request.state.app_name
        token = request.headers.get("Authorization")

        return await list_sales_clients_service(user_email, page, page_size, client_name, unselected, token)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Sales Client List Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to fetch sales clients", error_code=-1)


# ============================
# ADD SALES CLIENT
# ============================
@sales_client_router.post(ApplicationEndpoints.SALES_CLIENT_ADD, tags=["Sales Client"])
async def add_sales_client(request: Request, payload: AddSalesClientSchema):
    """
    Add a new sales client
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        token = request.headers.get("Authorization")
        return await add_sales_client_service(payload, user_email, app_name, token)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Sales Client Add Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to add client", error_code=-1)


# ============================
# DELETE SALES CLIENT
# ============================
@sales_client_router.delete(ApplicationEndpoints.SALES_CLIENT_DELETE, tags=["Sales Client"])
async def delete_sales_client(request: Request,client_id: str):
    """
    Delete a sales client
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        return await delete_sales_client_service(client_id, user_email)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Sales Client Delete Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to delete client", error_code=-1)

# ============================
# TOGGLE FAVORITE SALES CLIENT
# ============================
@sales_client_router.put(ApplicationEndpoints.SALES_CLIENT_FAVORITE, tags=["Sales Client"])
async def toggle_favorite_sales_client(request: Request,client_id: str, favorite: bool):
    """
    Mark or unmark a sales client as favorite
    """
    try:
        user_email = request.state.user_email
        app_name = request.state.app_name
        return await toggle_favorite_sales_client_service(client_id, favorite, user_email)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Toggle Favorite Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to update favorite status", error_code=-1)


# ============================
# LIST UNIQUE PORTFOLIO USERS
# ============================
@sales_client_router.get(ApplicationEndpoints.UNIQUE_PORTFOLIO_USERS, tags=["Sales Client"])
async def list_unique_portfolio_users(request: Request):
    """
    List unique users from portfolio for a sales client's organizations
    """
    try:    
        user_email = request.state.user_email
        app_name = request.state.app_name   
        token = request.headers.get("Authorization")
        return await list_unique_portfolio_users_service(user_email, app_name, token)

    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Unique Portfolio Users Fetch Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to fetch unique portfolio users", error_code=-1)

