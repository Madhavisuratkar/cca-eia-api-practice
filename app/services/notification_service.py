from app.connections.mongodb import get_collection
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import LevelType
from app.connections.pylogger import log_message
from app.utils.constants import CollectionNames
from bson import ObjectId
from datetime import datetime

async def get_notification_dtls_service(filters: dict):
    try:
        notification_collection = get_collection(CollectionNames.NOTIFICATIONS)
        query = {}
        # Convert _id to ObjectId if present
        if "_id" in filters and filters["_id"]:
            try:
                query["_id"] = ObjectId(filters["_id"])
            except Exception:
                log_message(LevelType.ERROR, f"Invalid _id format: {filters['_id']}", ErrorCode=-1)
                raise CustomAPIException(status_code=400, message=f"Invalid _id format: {filters['_id']}")
        # Add remaining filters
        for k, v in filters.items():
            if k != "_id" and v not in [None, ""]:
                query[k] = v

        cursor = notification_collection.find(query).sort("created_at", -1)
        notifications = await cursor.to_list(length=None)
        # Convert ObjectId and datetime fields to JSON serializable
        for n in notifications:
            if "_id" in n:
                n["_id"] = str(n["_id"])
            if "created_at" in n and isinstance(n["created_at"], datetime):
                n["created_at"] = n["created_at"].isoformat()
            if "updated_at" in n and isinstance(n["updated_at"], datetime):
                n["updated_at"] = n["updated_at"].isoformat()

         # ✅ Count only unseen notifications
        unseen_count = sum(1 for n in notifications if not n.get("is_seen", False))

        if notifications:
            message = "Notifications data fetched successfully"
            log_message(
                LevelType.INFO,
                f"{len(notifications)} notifications fetched for filters: {filters}",
                ErrorCode=1
            )
        else:
            message = "No notifications found"
            log_message(
                LevelType.INFO,
                f"No notifications found for filters: {filters}",
                ErrorCode=1
            )

        return {
            "Data": notifications,
            "Count": unseen_count,
            "Message": message
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"get_notification_dtls_service error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to fetch notifications: {str(e)}")

async def mark_notification_as_seen(notification_id: str) -> dict:
    """
    Mark a single notification's is_seen field to True.
    Returns a response dict on success.
    Raises CustomAPIException on failure.
    """
    try:
        notification_collection = get_collection(CollectionNames.NOTIFICATIONS)
        notification = await notification_collection.find_one({"_id": ObjectId(notification_id)})
        if not notification:
            raise CustomAPIException(
                status_code=404,
                message=f"Notification with id {notification_id} not found",
                error_code=-1
            )
        if notification.get("is_seen") is True:
            raise CustomAPIException(
                status_code=400,
                message=f"Notification with id {notification_id} is already marked as seen",
                error_code=-1
            )
        update = {
            "$set": {
                "is_seen": True,
                "updated_at": datetime.utcnow()
            }
        }
        result = await notification_collection.update_one({"_id": ObjectId(notification_id)}, update)
        if result.modified_count == 0:
            raise CustomAPIException(
                status_code=500,
                message="Failed to mark notification as seen",
                error_code=-1
            )
        log_message(
            LevelType.INFO,
            f"Marked notification {notification_id} as seen",
            ErrorCode=1
        )
        # Return a dict as the response
        return {
            "notification_id": notification_id,
            "is_seen": True,
            "message": f"Notification {notification_id} marked as seen."
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"mark_notification_as_seen error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to update notification: {str(e)}")


async def delete_notifications_service(filters: dict) -> tuple[int, str]:
    """
    Delete notifications based on provided filters.
    Returns a tuple of (deleted_count, message)
    """
    try:
        collection = get_collection(CollectionNames.NOTIFICATIONS)
        query = {}
        if "_id" in filters and filters["_id"]:
            try:
                query["_id"] = ObjectId(filters["_id"])
            except Exception:
                log_message(LevelType.ERROR, f"Invalid _id format: {filters['_id']}", ErrorCode=-1)
                raise CustomAPIException(status_code=400, message=f"Invalid _id format: {filters['_id']}")
        for k, v in filters.items():
            if k != "_id" and v not in [None, ""]:
                query[k] = v
        result = await collection.delete_many(query)
        if result.deleted_count == 0:
            message = "No notifications found to delete."
            log_message(
                LevelType.INFO,
                f"No notifications found to delete for filters: {filters}",
                ErrorCode=1
            )
        else:
            message = f"Deleted {result.deleted_count} notifications."
            log_message(
                LevelType.INFO,
                f"Deleted {result.deleted_count} notifications for filters: {filters}",
                ErrorCode=1
            )
        return result.deleted_count, message
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"delete_notifications_service error: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Failed to delete notifications: {str(e)}")
