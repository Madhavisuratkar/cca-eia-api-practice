from app.connections.env_config import DATABASE_NAME, MONGO_URI, COLLECTION_NAME
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from app.connections.pylogger import log_message
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import LevelType
mongo_client: AsyncIOMotorClient = None

async def connect_to_mongo():
    global mongo_client
    await asyncio.sleep(0)
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    print(f"Connected to MongoDB for MONGO_URI : {MONGO_URI}")

async def close_mongo_connection():
    global mongo_client
    await asyncio.sleep(0)
    if mongo_client:
        mongo_client.close()
        print("🛑 MongoDB connection closed")

def get_database():
    if mongo_client is None:
        log_message(LevelType.ERROR, "MongoDB client is not initialized.", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="MongoDB client is not initialized.", error_code=-1)
    return mongo_client[DATABASE_NAME]

def get_collection(collection_name: str):
    if mongo_client is None:
        log_message(LevelType.ERROR, "MongoDB client is not initialized. Call connect_to_mongo() first.", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="MongoDB client is not initialized. Call connect_to_mongo() first.", error_code=-1)
    return mongo_client[DATABASE_NAME][collection_name]


def get_user_data_collection():
    "user data collection"
    return get_collection(COLLECTION_NAME)

async def check_mongo_status() -> bool:
    """
    Check if MongoDB connection is alive.
    Returns True if accessible, else False.
    """
    if mongo_client is None:
        return False

    try:
        db = get_database()
        result = await db.command("ping")
        return result.get("ok", 0) == 1
    except Exception:
        return False