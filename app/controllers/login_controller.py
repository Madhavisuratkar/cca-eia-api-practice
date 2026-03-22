from fastapi import APIRouter, HTTPException
from app.connections.custom_exceptions import CustomAPIException
from app.services.login_service import login_user_service
from app.schema.login_schema import LoginRequest
from app.utils.constants import ApplicationEndpoints,LevelType
from app.connections.pylogger import log_message

login_router = APIRouter()
    
@login_router.post(ApplicationEndpoints.LOGIN)
def login_user(login_data: LoginRequest):
    """
    Method allow to login for testing the application
    """
    try:
        return login_user_service(login_data.dict())
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Login Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Login Failed", error_code=-1)
