from app.connections.custom_exceptions import CustomAPIException
from app.models.application import Application
from app.connections.pylogger import log_message
from app.utils.constants import LevelType
from sqlalchemy.orm import Session

def validate_app_name_db(app_name: str, db: Session) -> bool:
    """
    Validates if the provided app_name exists in the database.
    
    Args:
        app_name: Application name to validate
        
    Returns:
        True if app_name exists in database, False if not found or empty.
        Raises database exceptions for connection/query errors.
    """

    try:
        exists = db.query(Application).filter(Application.name == app_name.upper()).first()
        if not exists:
            log_message(LevelType.ERROR, f"Application '{app_name}' not found", ErrorCode=-1)
            return False
        log_message(LevelType.INFO, f"Application '{app_name}' validated successfully", ErrorCode=1)
        return True
    except Exception as e:
        log_message(LevelType.ERROR, f"Error during application validation: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Error during application validation", error_code=-1)