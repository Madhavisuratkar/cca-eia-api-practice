from app.connections.custom_exceptions import CustomAPIException
from app.connections.mongodb import get_collection
from app.utils.constants import CollectionNames
from app.schema.sales_client_schema import AddSalesClientSchema
from fastapi import HTTPException
from bson import ObjectId
from app.connections.env_config import CS_URL
import httpx
from datetime import datetime
from app.connections.pylogger import log_message
from app.utils.constants import LevelType
from app.utils.common_utils import get_user_emailscope

# ============================
# LIST SERVICE (WITH PAGINATION)
# ============================

async def list_sales_clients_service(user_email: str, page: int = 1, page_size: int = 10, client_name: str | None = None, unselected: bool = False, token: str | None = None):

    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)
    organization_data_collection = get_collection(CollectionNames.ORGANIZATION_DATA)

    org_list = None

    if unselected:
        updated_url = f"{CS_URL.rsplit('/', 1)[0]}/organizations"
        async with httpx.AsyncClient(timeout=10.0, verify=False) as client:
            response = await client.get(updated_url,headers={"Authorization": f"Bearer {token}"})

        if response.status_code == 200:
            org_list = response.json().get("Data", [])
        else:
            raise CustomAPIException(
                status_code=500,
                message="Failed to fetch organizations",
                error_code=-1
            )


    # 1️⃣ Fetch all sales clients for this user
    query = {"user_email": user_email}
    # optional partial match filter
    if client_name:
        query["client_name"] = {
            "$regex": client_name,
            "$options": "i"   # case insensitive
        }

    clients_cursor = sales_client_collection.find(query).sort([("is_favorite", -1), ("client_name", 1)])
    clients = await clients_cursor.to_list(length=None)

    # -----------------------------------
    # 2️⃣ If unselected → filter only those NOT in org_list
    # -----------------------------------
    if unselected and org_list:

        # Extract existing client names
        client_names = {c.get("client_name") for c in clients}

        # Organizations not yet selected
        unselected_orgs = [org for org in org_list if org not in client_names]

        # Build response like client objects
        clients = [
            {
                "_id": None,
                "client_name": org,
                "is_favorite": False
            }
            for org in unselected_orgs
        ]


    total_clients = len(clients)
    total_pages = (total_clients + page_size - 1) // page_size

    # 2️⃣ Pagination
    start = (page - 1) * page_size
    end = start + page_size
    paginated_clients = clients[start:end]

    enriched_data = []

    # 3️⃣ Join each client with organization_data to fetch portfolio_count
    for client in paginated_clients:
        client_name = client.get("client_name")

        org_docs = await organization_data_collection.find({"organization": client_name}).to_list(length=None)

        # Sum portfolio_count across rows
        portfolio_count = sum(doc.get("portfolio_count", 0) for doc in org_docs)

        enriched_data.append({
            "_id": str(client["_id"]),
            "client_name": client_name,
            "portfolio_count": portfolio_count,
            "is_favorite": client.get("is_favorite", False)
        })

    # 4️⃣ Response
    return {
        "total_clients": total_clients,
        "total_pages": total_pages,
        "current_page": page,
        "page_size": page_size,
        "Data": enriched_data,
        "ErrorCode": 1
    }


# ============================
# ADD SERVICE (MOCK)
# ============================

async def add_sales_client_service(payload: AddSalesClientSchema, email: str, app_name: str, token: str | None = None):
    """Add a new sales client"""

    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)
    organization_data_collection = get_collection(CollectionNames.ORGANIZATION_DATA)

    user_email = payload.user_email

    if user_email != email:
        raise CustomAPIException(
            status_code=400,
            message="User email does not match",
            error_code=-1
        )

    # 🔥 Convert all names to lowercase once
    client_names = [c.strip().lower() for c in payload.client_names]

    updated_url = f"{CS_URL.rsplit('/', 1)[0]}/organizations"
    async with httpx.AsyncClient(timeout=20.0, verify=False) as client:
        response = await client.get(updated_url,headers={"Authorization": f"Bearer {token}"})

    if response.status_code == 200:
        org_list = [o.lower() for o in response.json().get("Data", [])]
    else:
        raise CustomAPIException(
            status_code=500,
            message="Failed to fetch organizations",
            error_code=-1
        )

        # -------------------------------------------------------------
    # 2️⃣ Validate all client names BEFORE adding anything
    # -------------------------------------------------------------
    for name in client_names:

        # ❌ Check if organization exists in org_list
        if name not in org_list:
            raise CustomAPIException(
                status_code=400,
                message=f"Organization '{name}' not found",
                error_code=-1
            )

        # ❌ Check duplicates for this user
        exists = await sales_client_collection.find_one({
            "client_name": name,
            "user_email": user_email
        })

        if exists:
            raise CustomAPIException(
                status_code=400,
                message=f"Client '{name}' already exists for user '{user_email}'",
                error_code=-1
            )

    # -------------------------------------------------------------
    # 3️⃣ After all validations → Insert safely
    # -------------------------------------------------------------
    created_clients = []

    for name in client_names:
        new_doc = {
            "client_name": name,
            "user_email": user_email,
            "is_favorite": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        inserted = await sales_client_collection.insert_one(new_doc)
        new_doc["_id"] = str(inserted.inserted_id)

        created_clients.append(new_doc)

    # -------------------------------------------------------------
    # 4️⃣ Response
    # -------------------------------------------------------------
    return {
        "Message": "Sales clients added successfully",
        "ErrorCode": 1,
        "Data": created_clients
    }


# ============================
# DELETE SERVICE (REAL)
# ============================
async def delete_sales_client_service(_id: str, user_email: str):
    """Delete a sales client""" 
    if not ObjectId.is_valid(_id):  # format: 24-char hex or 12-byte input
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid client id '{_id}' Must be a valid 24-character hex string."
            ) 
    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)

    # 1️⃣ Validate record
    existing = await sales_client_collection.find_one({
        "_id": ObjectId(_id),
        "user_email": user_email
    })

    if not existing:
        raise CustomAPIException(
            status_code=404,
            message="Client not found for this user",
            error_code=-1
        )

    # 2️⃣ Delete
    await sales_client_collection.delete_one({"_id": ObjectId(_id)})

    return {
        "Message": "Sales client deleted successfully",
        "ErrorCode": 1,
        "_id": _id,
        "deleted_for_user": user_email
    }


# ============================
# TOGGLE FAVORITE SERVICE (REAL)
# ============================
async def toggle_favorite_sales_client_service(_id: str, favorite: bool, user_email: str):
    """
    Toggle a sales client's favorite status by _id
    """
    if not ObjectId.is_valid(_id):  # format: 24-char hex or 12-byte input
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid client id '{_id}' Must be a valid 24-character hex string."
            ) 
    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)

    # 1️⃣ Validate record
    existing = await sales_client_collection.find_one({
        "_id": ObjectId(_id),
        "user_email": user_email
    })

    if not existing:
        raise CustomAPIException(
            status_code=404,
            message="Client not found for this user",
            error_code=-1
        )

    # 2️⃣ Update favorite
    await sales_client_collection.update_one(
        {"_id": ObjectId(_id)},
        {"$set": {"is_favorite": favorite, "updated_at": datetime.now()}}
    )

    return {
        "Message": f"Client {'marked as favorite' if favorite else 'unmarked as favorite'} successfully",
        "ErrorCode": 1,
        "_id": _id,
        "is_favorite": favorite
    }


async def list_unique_portfolio_users_service(user_email: str, app_name: str, token: str):
    # 1. Extract organization from user email

    # 2. Fetch client names from SALES_CLIENT collection
    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)


    client_docs = await sales_client_collection.find({"user_email": user_email}).to_list(None)
    client_names = [c.get("client_name") for c in client_docs]
    

    # Safety: if no clients found → return empty list
    if not client_names:
        log_message(LevelType.INFO, f"No clients found for user '{user_email}'", ErrorCode=1)
        user_email_list = get_user_emailscope(token, user_email, app_name.upper())
        return {
            "Message": "Unique users fetched successfully",
            "ErrorCode": 1,
            "count": len(user_email_list),
            "Data": user_email_list
        }

    log_message(LevelType.INFO, f"Clients found for user '{user_email}'", ErrorCode=1)

    # 2. Build OR-regex for client names
    # Example: (infy|tcs|wipro)
    client_pattern = "(" + "|".join(client_names) + ")"

    # 3. Query portfolio.user_email USING regex of client names
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)


    portfolio_query = {
        "user_email": {"$regex": client_pattern, "$options": "i"},
        "app_name": app_name
    }

    # 4. Fetch unique user emails
    unique_users = await portfolio_collection.distinct("user_email", portfolio_query)

    # Remove logged-in user's email if present
    if user_email in unique_users:
        unique_users.remove(user_email)


    return {
        "Message": "Unique users fetched successfully",
        "ErrorCode": 1,
        "count": len(unique_users),
        "Data": unique_users
    }
