from fastapi import APIRouter, Request, Query
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import ApplicationEndpoints, LevelType
from app.connections.pylogger import log_message
from typing import List
from bson import ObjectId
from app.services.notification_service import (
    get_notification_dtls_service,
    delete_notifications_service,
    mark_notification_as_seen
)

notifications_router = APIRouter()

@notifications_router.get(ApplicationEndpoints.NOTIFICATIONS, tags=["Notification Router"])
async def get_notification_details(
    request: Request,
    user_email: str = Query(..., description="Email of the user"),
    app_name: str = Query(..., description="Application name, e.g., CCA or EIA"),
    notification_id: str = Query(None, description="Notification ID to fetch a specific notification"),
    portfolio_id: str = Query(None),
    portfolio_name: str = Query(None),
    purpose: str = Query(None),
    is_seen: bool = Query(None),
    title: str = Query(None, description="Title of the notification")
):
    """
    Fetch notification details using query parameters.
    """
    try:
        filters = {
            "_id": notification_id,
            "user_email": user_email,
            "app_name": app_name,
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio_name,
            "purpose": purpose,
            "is_seen": is_seen,
            "title": title         # Added title here
        }
        filters = {k: v for k, v in filters.items() if v is not None}
        response = await get_notification_dtls_service(filters)
        log_message(LevelType.INFO, f"Notifications fetched successfully for user: {user_email}. Filters: {filters}", ErrorCode=1)
        return response
    except CustomAPIException as e:
        raise e
    except Exception as e:
        log_message(LevelType.ERROR, f"get_notification_details error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to fetch notifications: {str(e)}")


@notifications_router.patch(ApplicationEndpoints.NOTIFICATIONS_READ, tags=["Notification Router"])
async def mark_notification_seen_endpoint(
    notification_id: str = Query(..., description="Notification ID to mark as seen")
):
    notification_id = notification_id.strip().replace('"', '')
    
    try:
        return await mark_notification_as_seen(notification_id)
    except CustomAPIException as e:
        raise e
    except Exception as e:
        log_message(LevelType.ERROR, f"mark_notification_seen_endpoint error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to update notification: {str(e)}")



@notifications_router.delete(ApplicationEndpoints.NOTIFICATIONS_DEL, tags=["Notification Router"])
async def delete_notifications(
    request: Request,
    user_email: str = Query(..., description="Email of the user"),
    app_name: str = Query(..., description="Application name, e.g., CCA or EIA"),
    notification_id: str = Query(None, description="Notification ID to delete a specific notification"),
    portfolio_id: str = Query(None),
    portfolio_name: str = Query(None),
    purpose: str = Query(None),
    is_seen: bool = Query(None),
    title: str = Query(None, description="Title of the notification")  # Added here
):
    """
    Delete notifications based on filters.
    """
    try:
        filters = {
            "_id": notification_id,
            "user_email": user_email,
            "app_name": app_name,
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio_name,
            "purpose": purpose,
            "is_seen": is_seen,
            "title": title         # Added title here
        }
        filters = {k: v for k, v in filters.items() if v is not None}
        _, message = await delete_notifications_service(filters)
        log_message(LevelType.INFO, f"{message} Filters: {filters}", ErrorCode=1)
        return {"message": message}
    except CustomAPIException as e:
        raise e
    except Exception as e:
        log_message(LevelType.ERROR, f"delete_notifications error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to delete notifications: {str(e)}")
