from datetime import datetime
from app.connections.custom_exceptions import CustomAPIException
from app.connections.pylogger import log_message
from app.middleware.request_context import get_request
from app.schema.portfolio_model_without_cloud import PortfolioFilter, SavePortfolioRequest
from app.utils.constants import CLOUD_PROVIDERS, RecommendationStatus, UNSUPPORTED_PROVIDERS, LevelType, field_mappings, AppName, CollectionNames
import uuid
from app.utils.encrypt_decrypt import decrypt_dict, encrypt_dict
import asyncio
from app.utils.withCloudUtils import (data_extract, test_cloud_connection, get_aws_instances, 
                                      sync_gcp_account, get_azure_instances, get_gcp_instances)
from fastapi import Query, Request, HTTPException
from bson import ObjectId
from app.schema.portfolio_with_cloud_schema import AWSAccount, AzureAccount, GCPAccount
from typing import Dict
from app.utils.common_utils import save_portfolio_sanity_check, get_user_emailscope, extract_organization_from_email
from app.connections.mongodb import get_collection
from pydantic import TypeAdapter
from pymongo import DESCENDING
from pymongo.results import DeleteResult
from sqlalchemy.orm import Session
from app.models.policy_engine import PolicyEngine
from sqlalchemy import func, or_

async def save_portfolio_data(db: Session, payload: SavePortfolioRequest, app_name: str, ipaddr: str, user_email, is_cloud_cred : bool = False, is_billing_data : bool = False):
    """save portfolio function"""

    portfolio_name = payload.portfolioName.strip().replace(" ", "_")
    provider = payload.provider.strip().upper()
    policy_engine = payload.policy_engine if app_name.upper()=='CCA' else None
    headroom = payload.headroom or 20
    records = payload.data
    log_message(LevelType.INFO, f"Received save request for portfolio '{portfolio_name}' by {user_email} , {app_name}", ErrorCode=1)
    try:
        if app_name.upper()=="CCA" and policy_engine:
            exists = (
                db.query(PolicyEngine.id)
                .filter(
                    or_(PolicyEngine.user_email == user_email, PolicyEngine.user_email == ""),
                    func.lower(PolicyEngine.provider) == provider.lower(),
                    func.lower(PolicyEngine.policy_name) == policy_engine.strip().lower(),
                )
                .limit(1)
                .first()
            )
            if not exists:
                raise CustomAPIException(
                    message=(
                        f"Policy engine not found with name '{policy_engine}' "
                        f"for provider '{provider}'"
                    ),
                    status_code=409,
                )
        
        save_portfolio_sanity_check(provider, portfolio_name, records, headroom, app_name)
        # check if a portfolio already exists
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        query = {
            "name": portfolio_name,
            "user_email": user_email,
            "cloud_provider": provider,
            "app_name": app_name.upper(),
        }

        if is_billing_data:
            query["is_billing_data"] = True
        else:
            query["$or"] = [
                {"is_billing_data": False},
                {"is_billing_data": {"$exists": False}}
            ]

        # if cloud_cred exists, check against its provider
        if payload.cloud_cred and payload.cloud_cred.get("provider"):
            cred_provider = payload.cloud_cred.get("provider")
            query["cloud_cred.provider"] = {"$regex": f"^{cred_provider}$", "$options": "i"}

        # ✅ get current request and update state
        request = get_request()

        existing = await portfolio_collection.find_one(query)
        if existing:
            portfolio_id = str(existing.get("_id"))
            if request:
                request.state.portfolio_id = portfolio_id
            log_message(LevelType.ERROR, f"Portfolio '{portfolio_name}'already exiting for app_name : {app_name} : {provider}", ErrorCode=-1, portfolio_id=portfolio_id)
            raise CustomAPIException(status_code=400, message=f"Portfolio '{portfolio_name}'already exiting for app_name : {app_name} : {provider}")

        now = datetime.utcnow()
        doc = {
            "user_email": user_email,
            "name": portfolio_name,
            "cloud_provider": provider,
            "headroom": headroom,
            "app_name": app_name.upper(),
            "created_at": now,
            "uploaded_date": now,
            "status": "Passed",
            "submittedForRecommendations":False,
            "is_cloud_cred" : is_cloud_cred,
            "ip": ipaddr,
            "current_instances_count": len(records) if records else 0
        }

        if payload.cloud_cred:
            encrypted_cred = encrypt_dict(payload.cloud_cred)
            doc["cloud_cred"] = encrypted_cred
            doc["is_cloud_cred"] = True
        
        if app_name.upper() == "CCA":
            doc["policy_engine"] = policy_engine
        
        if app_name.upper()=="EIA" and payload.udf:
            doc["udf"] = payload.udf

        if payload.created_for:
            # 1️⃣ Validate client_name exists in organization_data_collection
            organization_data_collection = get_collection(CollectionNames.ORGANIZATION_DATA)
            org_doc = await organization_data_collection.find_one(
                {"organization": payload.created_for, "app_name": app_name.upper()}
            )

            if not org_doc:
                raise CustomAPIException(
                    status_code=400,
                    message=f"Organization '{payload.created_for}' not found",
                    error_code=-1
                )
            doc["created_for"] = payload.created_for.strip().lower()
        else:
            org = extract_organization_from_email(user_email)
            doc["created_for"] = org


        result = await portfolio_collection.insert_one(doc)
        inserted_id = str(result.inserted_id)

        log_message(LevelType.INFO, f"Portfolio '{portfolio_name}' saved successfully", ErrorCode=1, portfolio_id=inserted_id)
        
        if request:
            request.state.portfolio_id = inserted_id
        
        instance_docs = [
            {
                **record.model_dump(by_alias=True),
                "uuid": record.uuid or str(uuid.uuid4()),
                "portfolio_id": inserted_id,
                "created_at": now,
                "uploaded_date": now
            }
            for record in records
        ]

        if instance_docs:
            current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)
            await current_instance_collection.insert_many(instance_docs)
            log_message(LevelType.INFO, "Portfolio instance saved successfully", ErrorCode=1, portfolio_id=inserted_id)

            inc_result = await update_organization_instance_count(
                portfolio_id=inserted_id,
                new_current_instance_count=len(instance_docs),
                app_name=app_name.upper()
            )

            # Log it
            log_message(LevelType.INFO, f"[ORG_UPDATE] {inc_result['message']}", ErrorCode=1)

            
        response = {"message": f"Portfolio '{portfolio_name}' saved successfully", "portfolio_id": inserted_id,"ErrorCode": 1}
        return response
    except HTTPException as http_err:
        log_message(LevelType.ERROR, f"HTTPException: {http_err.detail}", ErrorCode=http_err.status_code)
        raise http_err
    except Exception as ex:
        log_message(LevelType.ERROR, f"Unhandled error: {str(ex)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Failed to save portfolio due to server error")
    

async def get_all_portfolios_data(user_mail, app_name, provider, cloud_csp, token, is_billing_data):
    """"""
    log_message(LevelType.INFO, f"Fetching portfolios for user: {user_mail}, provider: {provider}", ErrorCode=1)

    # Validate input
    if not provider or not app_name:
        msg = "Missing required fields"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)

    if provider.upper() not in CLOUD_PROVIDERS:
        msg = "Invalid cloud provider"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)

    if provider.upper() in UNSUPPORTED_PROVIDERS:
        msg = "Unsupported cloud provider"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)
    
    # Fetch from MongoDB
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    user_email_list = get_user_emailscope(token, user_mail, app_name.upper())

    query = {
        "app_name": app_name.upper(),
        "cloud_provider": provider.upper(),
        "user_email": {"$in": user_email_list},
    }

    if is_billing_data:
        query["is_billing_data"] = True
    else:
        # Consider missing key as False
        query["$or"] = [
            {"is_billing_data": False},
            {"is_billing_data": {"$exists": False}}
        ]

    if cloud_csp:
        query["cloud_cred.provider"] = {
            "$regex": f"^{cloud_csp}$",
            "$options": "i"  # case-insensitive match
        }

    cursor = portfolio_collection.find(query).sort("created_at", DESCENDING)
    docs = await cursor.to_list(length=None)

    if not docs:
        msg = f"Portfolios not found for provider {provider}"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=404, message=msg, error_code=1)

    # Build response
    response_data = []
    for doc in docs:
        # Extra condition when cloud_csp is not provided
        if not cloud_csp and doc.get("is_cloud_cred", False):
            cloud_cred = doc.get("cloud_cred", {})
            if cloud_cred.get("provider", "").lower() != provider.lower():
                continue  # skip this doc
        recommendation_percentage = doc.get("recommendation_percentage", 0)
        if doc.get("recommendation_status", RecommendationStatus.COMPLETED) == RecommendationStatus.COMPLETED:
            recommendation_percentage = 100

        res = {
            doc["name"]: doc.get("status", "Unknown"),
            "user": doc.get("user_email", "unknown_user"),
            "_id": str(doc.get("_id")),
            "is_cloud_cred": doc.get("is_cloud_cred", False),
            "is_recommendation_available": doc.get("submittedForRecommendations", False),
            "recommendation_status": doc.get("recommendation_status", RecommendationStatus.COMPLETED),
            "recommendation_percentage": recommendation_percentage,
            "is_large_data": doc.get("is_large_data", False),
            "is_billing_data": doc.get("is_billing_data", False)
        }

        response_data.append(res)


    if not response_data:
        msg = f"Portfolios not found for provider {provider}"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=404, message=msg)

    # Partition into own and team
    own_portfolios = [x for x in response_data if x.get("user", "").lower() == user_mail.lower()]
    team_portfolios = [x for x in response_data if x.get("user", "").lower() != user_mail.lower()]

    log_message(LevelType.INFO, f"Portfolios fetched successfully for {user_mail}", ErrorCode=2001)
    return {
        "Message": "Portfolio status fetched successfully",
        "own_portfolios": own_portfolios,
        "team_portfolios": team_portfolios,
        "ErrorCode": 1
    }


async def get_portfolio_data(user_mail, _id):
    """get portfolio logic function"""
    if not _id:
        log_message(LevelType.ERROR, "Missing required fields", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Required fields missing")
    
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)

    portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(_id)})
    if not portfolio_doc:
        log_message(LevelType.ERROR, "Portfolio not found", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=404, message="Portfolio not found", error_code=-1)
    
    portfolio_doc.pop("_id")  # Remove internal ID if not needed

    # Fetch all related instances
    instance_cursor = current_instance_collection.find({"portfolio_id": _id})
    instances = await instance_cursor.to_list(length=None)

    # Convert ObjectIds for JSON safety
    for inst in instances:
        inst["_id"] = str(inst["_id"])
        inst["portfolio_id"] = str(inst["portfolio_id"])

    portfolio_doc["input_data"] = instances

    log_message(LevelType.INFO, f"Portfolio fetched for {user_mail}", ErrorCode=1, portfolio_id=_id)
    return {
        "Message": "Portfolio fetched",
        "Data": portfolio_doc,
        "ErrorCode": 1
    }

    

async def patch_portfolio_data(
    update_fields: dict,
    _id: str,
    app_name: str,
    db: Session,
    user_email: str
):
    try:
        """Logic to rename a saved portfolio"""
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)

        # 1. Check if update_fields is empty
        if not update_fields:
            raise CustomAPIException(status_code=400, message="No fields provided for update")

        # 2. Check if all provided values are empty/whitespace
        cleaned_fields = {
            k: v for k, v in update_fields.items()
            if not (isinstance(v, str) and v.strip() == "")
        }
        if not cleaned_fields:
            raise CustomAPIException(status_code=400, message="All provided fields are empty")

        # Map aliases to real field names
        field_alias_map = {
            "portfolioName": "name",
            "provider": "cloud_provider"
        }
        for alias_key, actual_key in field_alias_map.items():
            if alias_key in update_fields:
                update_fields[actual_key] = update_fields.pop(alias_key)

        existing = await portfolio_collection.find_one({
            "_id": ObjectId(_id)
        })
        if not existing:
            log_message(LevelType.ERROR, f"Portfolio not found for {user_email}", ErrorCode=-1, portfolio_id=_id)
            raise CustomAPIException(status_code=404, message="Portfolio not found")
        if existing.get("is_locked") and existing.get("user_email") != user_email:
            log_message(LevelType.ERROR, f"Unauthorized access: {existing.get('user_email')} ≠ {user_email}", ErrorCode=-1, portfolio_id=_id)
            raise CustomAPIException(status_code=400, message="You are not authorized to update this portfolio", error_code=-1)
        if app_name.upper() == "CCA" and update_fields.get("policy_engine"):
            exists = (
                db.query(PolicyEngine.id)
                .filter(
                    or_(PolicyEngine.user_email == existing.get("user_email"), PolicyEngine.user_email == ""),
                    func.lower(PolicyEngine.provider) == existing.get("cloud_provider").lower(),
                    func.lower(PolicyEngine.policy_name) == update_fields.get("policy_engine").lower(),
                )
                .limit(1)
                .first()
            )
            if not exists:
                raise CustomAPIException(
                    message=(
                        f"Policy engine not found with name '{update_fields.get('policy_engine')}' "
                        f"for provider '{existing['cloud_provider']}'"
                    ),
                    status_code=409,
                )
        now = datetime.utcnow()
        if update_fields.get("data"):
            current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)
            instance_docs = [
                {
                    **record,
                    "uuid": record.get('uuid') or str(uuid.uuid4()),
                    "portfolio_id": _id,
                    "created_at": now,
                    "uploaded_date": now
                }
                for record in update_fields.pop("data")
            ]
            if instance_docs:
                # Delete old instances for this portfolio
                await current_instance_collection.delete_many({"portfolio_id": _id})

                # Insert new instances
                await current_instance_collection.insert_many(instance_docs)
                log_message(LevelType.INFO, "Portfolio instance saved successfully", ErrorCode=1, portfolio_id=_id)
                update_fields["current_instances_count"] = len(instance_docs)
                inc_result = await update_organization_instance_count(
                    portfolio_id=_id,
                    new_current_instance_count=len(instance_docs),
                    app_name=app_name.upper()
                )

                # Log it
                log_message(LevelType.INFO, f"[ORG_UPDATE] {inc_result['message']}", ErrorCode=1)
        
        if update_fields.get("cloud_cred"):
            encrypted_cred = encrypt_dict(update_fields.get("cloud_cred"))
            update_fields["cloud_cred"] = encrypted_cred

        update_fields["uploaded_date"] = now
        await portfolio_collection.update_one(
                {"_id": ObjectId(_id)},
                {"$set": update_fields}
            )
        log_message(LevelType.INFO, f"Portfolio {_id} updated ", portfolio_id=_id)
        return {"Message": "Portfolio updated successfully", "ErrorCode": 1}
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Unhandled error in patch_portfolio_data: {str(err)}", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=500, message="Failed to update portfolio due to server error")


async def delete_portfolio_data(_id: str, app_name: str, user_email: str):
    """Delete portfolio document from MongoDB"""
    try:
        portfolio_obj_id = ObjectId(_id)
        # Get collections
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
        current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)
        recommended_instance_collection = get_collection(CollectionNames.RECOMMENDED_INSTANCES)
        recommended_tracking_collection = get_collection(CollectionNames.RECOMMENDATION_TRACKING)
        recommendation_analytics_collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
        org_collection = get_collection(CollectionNames.ORGANIZATION_DATA)
        notification_collection = get_collection(CollectionNames.NOTIFICATIONS)

        # Check if portfolio exists
        portfolio = await portfolio_collection.find_one({"_id": portfolio_obj_id})
        if not portfolio:
            raise CustomAPIException(
                status_code=404,
                message=f"Portfolio not found for ID {_id}",
                error_code=-1
            )

        if portfolio.get("is_locked") and portfolio.get("user_email") != user_email:
            log_message(LevelType.ERROR, f"Unauthorized access: {portfolio.get('user_email')} ≠ {user_email}", ErrorCode=-1, portfolio_id=_id)
            raise CustomAPIException(
                status_code=400,
                message="You are not authorized to delete this portfolio",
                error_code=-1
            )

        created_for = portfolio.get("created_for")
        current_instances = portfolio.get("current_instances_count", 0)
        is_increment_added = portfolio.get("is_increment_added", False)

        # --------------------------------------------------
        # 1️⃣ IF increment was added earlier → decrement now
        # --------------------------------------------------
        if is_increment_added:
            await org_collection.update_one(
                {"organization": created_for, "app_name": app_name},
                {
                    "$inc": {
                        "portfolio_count": -1,
                        "current_instance_count": -current_instances
                    }
                }
            )
            
        # Step 1: Delete from recommendation_tracking
        await recommended_tracking_collection.delete_many({"portfolio_id": _id})

        # Step 2: Delete from recommendation_analytics
        await recommendation_analytics_collection.delete_many({"portfolio_id": _id})

        # Step 3: Delete from recommended_instances
        await recommended_instance_collection.delete_many({"portfolio_id": _id})

        # Step 4: Delete from current_instance
        await current_instance_collection.delete_many({"portfolio_id": _id})

        # Step 5: Delete from notifications
        await notification_collection.delete_many({"portfolio_id": _id})

        # Step 6: Delete from portfolios
        result = await portfolio_collection.delete_one({"_id": portfolio_obj_id})
        if result.deleted_count == 0:
            raise CustomAPIException(
                status_code=500,
                message="Failed to delete portfolio",
                error_code=-1
            )

        log_message(LevelType.INFO, f"Deleted portfolio and related data for ID {_id}", portfolio_id=_id)
        return {
            "Message": f"Portfolio : '{portfolio.get("name")}' deleted successfully",
            "ErrorCode": 1
        }
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"err in delete_portfolio_data : {str(err)}", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=400, message="unable to delete portfolio", error_code=-1)


async def delete_current_instance_data(ids: list[str]) -> dict:
    """
    Delete one or multiple current instances by their IDs.

    Args:
        ids (list[str]): List of current instance IDs (as strings).

    Returns:
        dict: Response containing:
            - status (str): success or partial
            - deleted_count (int): number of documents deleted
            - not_found_ids (list[str]): IDs that were not found in the collection

    Raises:
        CustomAPIException: If no IDs are provided or no matching documents found.
    """
    log_message(LevelType.ERROR, f"given ids : {ids}", ErrorCode=-1)
    if not ids:
        log_message(LevelType.ERROR, "Unable to delete instance - no IDs provided", ErrorCode=-1)
        raise CustomAPIException(
            status_code=500,
            message="Unable to delete instance - no IDs provided"
        )

    # Convert to ObjectId
    object_ids = [ObjectId(i) for i in ids]

    # Pre-check existing documents
    current_instances = get_collection(CollectionNames.CURRENT_INSTANCES)
    existing_docs = await current_instances.find({"_id": {"$in": object_ids}}).to_list(length=None)
    if not existing_docs:
        log_message(LevelType.ERROR, "Unable to delete portfolio - no matching IDs found", ErrorCode=-1)
        raise CustomAPIException(
            status_code=500,
            message="Unable to delete portfolio - no matching IDs found"
        )

    existing_ids = {str(doc["_id"]) for doc in existing_docs}
    not_found_ids = [i for i in ids if i not in existing_ids]

    # Delete only existing IDs
    result: DeleteResult = await current_instances.delete_many({"_id": {"$in": [ObjectId(e) for e in existing_ids]}})

    log_message(LevelType.INFO, f"Successfully deleted {result.deleted_count} instance(s).", ErrorCode=1)

    return {
        "Message": (
            f"Successfully deleted {result.deleted_count} instance(s)."
            if not not_found_ids
            else f"Deleted {result.deleted_count} instance(s), "
                f"but {len(not_found_ids)} ID(s) were not found."
        ),
        "ErrorCode": 1 if result.deleted_count > 0 else -1,
        "NotFoundIds": not_found_ids
    }

###################### without cloud ###############################

async def update_current_instance(portfolio_id, portfolio_list, name):
    current_instance_collection = get_collection(CollectionNames.CURRENT_INSTANCES)

    # Remove old
    await current_instance_collection.delete_many({"portfolio_id": portfolio_id})

    # Insert new
    now = datetime.utcnow()
    instance_docs = [
        {
            **record,
            "portfolio_id": portfolio_id,
            "created_at": now,
            "uploaded_date": now
        }
        for record in portfolio_list
    ]

    if instance_docs:
        await current_instance_collection.insert_many(instance_docs)

    log_message(LevelType.INFO, f"Portfolio with ID {str(portfolio_id)} : {name} updated", portfolio_id=str(portfolio_id))
    return {"Message": f"Portfolio {name} updated successfully", "ErrorCode": 1}


async def sync_portfolio_data(db, cloud_account_data, name, user_email, app_name, policy_engine, ipaddr, port_folio_id=None):
    try:
        portfolio_list = []
        if cloud_account_data['provider'].upper() == 'AWS':
            portfolio_list.extend(get_aws_instances(cloud_account_data, compute=True))
            portfolio_list.extend(get_aws_instances(cloud_account_data, reserved=True))
            portfolio_list.extend(get_aws_instances(cloud_account_data, spot=True))

        if cloud_account_data['provider'].upper() == 'AZURE':
            portfolio_list = get_azure_instances(cloud_account_data)
            
        if cloud_account_data['provider'].upper() == 'GCP':
            portfolio_list = get_gcp_instances(cloud_account_data)
        if port_folio_id:
            return await update_current_instance(port_folio_id, portfolio_list, name)
        else:
            cloud_account_data.pop("policy_engine")
            portfolio_data = {'provider': cloud_account_data['provider'].upper(), 'portfolioName' : name,"user_email" : user_email,
                            'data': portfolio_list if portfolio_list else [],"cloud_cred": cloud_account_data, "appName" : app_name.upper(), "policy_engine" : policy_engine,
                            "created_for": cloud_account_data.get("created_for")}
            portfolio_model = TypeAdapter(SavePortfolioRequest).validate_python(portfolio_data)
            return await save_portfolio_data(db, portfolio_model, app_name, ipaddr, user_email, True)
        
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Error in sync_portfolio_data: {str(e)}")
        return False


async def add_cloud_account_service(db, payload, user_email: str, app_name: str, ipaddr: str, endpoint: str):
    """"""
    try:

        # Convert Pydantic model to dict
        cloud_account_data = payload.model_dump()
        if "user_email" in cloud_account_data:
            cloud_account_data.pop("user_email")

        # Validation
        provider = cloud_account_data.get("provider")
        region = cloud_account_data.get("region")
        policy_engine = cloud_account_data.get("policy_engine").strip().lower() if cloud_account_data.get("policy_engine") else None
        account_name = cloud_account_data.get("accountName")

        if not provider or not region or not account_name:
            log_message(LevelType.ERROR, "Missing required fields", ErrorCode=-1)
            raise CustomAPIException(status_code=404, message="Missing required fields", error_code=-1)
        
        if cloud_account_data['provider'].lower() == 'gcp':
                cloud_account_data = data_extract(cloud_account_data.get("private_key"),cloud_account_data, user_email,endpoint)

        status, error_message = test_cloud_connection(cloud_account_data)
        if status:
            sync_status = await sync_portfolio_data(db, cloud_account_data, account_name, user_email, app_name, policy_engine, ipaddr)
            if not sync_status:
                log_message(LevelType.ERROR, "Unable to perform add cloud account operation", ErrorCode=-1)
                raise CustomAPIException(status_code=400, message="Unable to perform add cloud account operation", error_code=-1)
            return {"Message": f"Added {cloud_account_data['provider'].upper()} account: {account_name}","portfolio_id" : sync_status.get("portfolio_id") ,"ErrorCode": 1}
        else:
            log_message(LevelType.ERROR, error_message, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message=error_message, error_code=-1)
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Error in add_cloud_account_service : {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="unable to add account", error_code=-1)
    

async def sync_cloud_account_service(user_mail: str, app_name: str, ipaddr: str, _id: str):
    """sync_cloud_account_service"""
    
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    existing = await portfolio_collection.find_one({
            "_id": ObjectId(_id)
        })

    if not existing:
        log_message(LevelType.ERROR, f"Unable to locate portfolio for id {_id}", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=404, message=f"Unable to locate portfolio for id {_id}", error_code=-1)
    
    cloud_account_data = existing.get("cloud_cred")
    provider = existing.get("cloud_provider")
    account_name = existing.get("name")

    if not cloud_account_data:
        log_message(LevelType.ERROR, f"Account file '{account_name}' not found for user '{user_mail}'.", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=404, message=f"Account file '{account_name}' not found for user '{user_mail}'.", error_code=-1)

    # doing Decrypt before using
    cloud_account_data = decrypt_dict(cloud_account_data)
    
    if cloud_account_data.get('provider', '').lower() == 'gcp':
        cloud_account_data = sync_gcp_account(cloud_account_data, user_mail)

    if provider.lower() != cloud_account_data.get('provider', '').lower():
        raise CustomAPIException(status_code=400, message="Invalid provider.")

    status, error_message = test_cloud_connection(cloud_account_data)
    if status:
        sync_status = sync_portfolio_data(None, cloud_account_data, account_name, user_mail, app_name, ipaddr, _id)
        if not sync_status:
            log_message(LevelType.ERROR, "Unable to perform sync cloud account operation", ErrorCode=-1, portfolio_id=_id)
            raise CustomAPIException(status_code=400, message="Unable to perform sync cloud account operation", error_code=-1)

        log_message(LevelType.INFO,f"{provider.upper()} Data successfully synced", portfolio_id=_id)
        return {"Message": f"{provider.upper()} Data successfully synced", "ErrorCode": 1}
    else:
        log_message(LevelType.ERROR, error_message, ErrorCode=-1, portfolio_id=_id)
        return {"Message": error_message, "ErrorCode": -1}

PROVIDER_MODELS = {
    "aws": AWSAccount,
    "azure": AzureAccount,
    "gcp": GCPAccount
}

async def get_cloud_account_model(request: Request, provider: str = Query(...)) -> object:
    """
    Read all query params and construct the correct provider model.
    - provider must be present as a query param (e.g. ?provider=aws)
    - other provider specific fields are taken from request.query_params
    """
    try:
        provider_value = provider.strip().lower()
    except Exception:
        raise CustomAPIException(status_code=400, message="Missing or invalid provider query parameter")

    model_cls = PROVIDER_MODELS.get(provider_value)
    if not model_cls:
        raise CustomAPIException(status_code=400, message=f"Unsupported provider '{provider_value}'")

    # Build dict of query params and strip all string values
    params: Dict[str, str] = {
        k: v.strip() if isinstance(v, str) else v
        for k, v in request.query_params.multi_items()
    }

    # Ensure normalized provider value
    params["provider"] = provider_value
    await asyncio.sleep(0)
    try:
        validated = model_cls(**params)
        return validated
    except Exception as err:
        log_message(LevelType.ERROR, f"Cloud account validation error : {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=422, message=str(err))
    

async def test_cloud_connection_service(cloud_account_model, user_email):
    """test cloud connection services"""
    await asyncio.sleep(0)
    cloud_account_data = cloud_account_model.model_dump()  # pydantic v2
    provider = cloud_account_data.get("provider")
    # If GCP needs special extraction (private_key -> file / filtered data)
    if provider.lower() == "gcp":
        cloud_account_data = data_extract(cloud_account_data.get("private_key"), cloud_account_data, user_email, "TEST_CLOUD_CONNECTION")
    
    if provider.lower() == "aws":
        cloud_account_data["awsAccessSecret"] = cloud_account_data["awsAccessSecret"].replace(" ", "+")
    status, error = test_cloud_connection(cloud_account_data)

    if status:
        log_message(LevelType.INFO, f"{provider.upper()} Connection successful", ErrorCode=1)
        return {"Message": f"{provider.upper()} Connection successful", "ErrorCode": 1}
    else:
        log_message(LevelType.ERROR, f"Connection failed : {error}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=f"Connection failed for provider : {provider} invalid credentials", error_code=-1)
    

async def get_cloud_account(_id):
    """get clund account"""
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    doc = await portfolio_collection.find_one({"_id": ObjectId(_id)})

    if not doc:
        log_message(LevelType.ERROR, f"Unable to locate portfolio for id {_id}", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=404, message=f"Unable to locate portfolio for id {_id}", error_code=-1)
    
    account_data = doc.get("cloud_cred")
    account_name = doc.get("name")
    if not account_data:
        log_message(LevelType.ERROR, f"no account data found for {account_name}", ErrorCode=-1, portfolio_id=_id)
        raise CustomAPIException(status_code=404, message=f"no data found for {account_name}", error_code=-1)

    # doing Decrypt before using
    log_message(LevelType.INFO, "Doing Decrypt for cloud_cred", portfolio_id=_id)
    account_data = decrypt_dict(account_data)
    account_data["cloud_provider"] = doc.get('cloud_provider')

    log_message(LevelType.INFO, "Account data fetched successfully.", portfolio_id=_id)
    return {
        "Message": "Account fetched successfully",
        "Data": account_data,
        "ErrorCode": 1
    }

################################### with cloud ############################################


async def get_portfolios_common_data(provider, _id, cloud_csp, list_all, app_name, user_email, token, is_billing_data=False):
    """"""
    try:
        if not list_all and _id:
            return await get_portfolio_data(
                user_mail=user_email,
                _id=_id

            )
        elif list_all:
            return await get_all_portfolios_data(
                user_mail=user_email,
                app_name=app_name,
                provider=provider,
                cloud_csp=cloud_csp,
                    token=token, is_billing_data=is_billing_data)
        else:
            raise CustomAPIException(status_code=400, message="Missing required fields")
    except CustomAPIException:
        raise
    except Exception:
        raise CustomAPIException(status_code=500, message="Internal server error")
    

async def handle_cloud_account_service(query_type: str, provider: str, user_email: str, app_name: str,
    ipaddr: str, request: Request, _id: str):
    """"""
    try:
        query_type = query_type.strip().lower()

        if query_type == "get_account":
            return await get_cloud_account(_id)
        elif query_type == "sync_account":
            return await sync_cloud_account_service(user_email, app_name, ipaddr, _id)
        elif query_type == "test_account":
            cloud_account = await get_cloud_account_model(request, provider)
            return await test_cloud_connection_service(cloud_account, user_email)
        else:
            raise CustomAPIException(status_code=400, message=f"Invalid query type provided : {query_type}")
    except CustomAPIException:
        raise
    except Exception:
        raise CustomAPIException(status_code=500, message="Internal server error")

#################################################################################


async def list_all_filter_portfolios(
    token, filters: PortfolioFilter, user_email: str, app_name: str
):
    """listing portfolios by filters"""
    query = {"app_name" : app_name.upper()}

    # --------------------------
    # 1. CREATED BY / FOR LOGIC
    # --------------------------
    if filters.created_by:
        query["user_email"] = filters.created_by

    if filters.created_for:
        query["created_for"] = filters.created_for

    # list_for: All, self, others
    if filters.list_for and not filters.created_by and not filters.created_for:
        if filters.list_for.lower() == "self":
            query["user_email"] = user_email
        elif filters.list_for.lower() == "others":
            user_email_list = get_user_emailscope(token, user_email, app_name.upper())
            user_email_list = [email for email in user_email_list if email != user_email]
            query["user_email"] = {"$in": user_email_list}
        elif filters.list_for.lower() == "all":
            sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)
            clients_cursor = sales_client_collection.find({"user_email": user_email})
            clients = await clients_cursor.to_list(length=None)
            if clients:
                org_list = [client["client_name"] for client in clients]
                # Create OR regex: infobellit|amd
                org_pattern = "|".join(map(lambda x: x.strip(), org_list))

                # Include portfolios created for clients OR portfolios created by the user
                query["$or"] = [
                    {"created_for": {"$regex": org_pattern, "$options": "i"}},
                    {"user_email": user_email}
                ]
            else:
                user_email_list = get_user_emailscope(token, user_email, app_name.upper())
                query["user_email"] = {"$in": user_email_list}

    # --------------------------
    # 2. PROVIDER ENUM
    # --------------------------
    if filters.provider:
        provider = filters.provider.upper()

        if provider == "ALL":
            query["cloud_provider"] = {"$in": ["AWS", "AZURE", "GCP"]}
        else:
            query["cloud_provider"] = provider
        

    # --------------------------
    # 3. CLOUD CSP (custom field)
    # --------------------------
    if filters.cloud_scp:
        query["cloud_cred.provider"] = {"$regex": f"^{filters.cloud_scp.lower()}$", "$options": "i"}


    # --------------------------
    # 4. BILLING DATA FLAG
    # --------------------------
    if filters.is_billing_data:
        query["is_billing_data"] = True
    else:
        billing_condition = [
            {"is_billing_data": False},
            {"is_billing_data": {"$exists": False}}
        ]
        
        # If query already has $or (from list_for="all"), combine using $and
        existing_or = query.pop("$or", None)
        if existing_or:
            query["$and"] = [
                {"$or": existing_or},
                {"$or": billing_condition}
            ]
        else:
            query["$or"] = billing_condition

    # --------------------------
    # 5. NAME MATCH (case-insensitive)
    # --------------------------
    if filters.name:
        query["name"] = {"$regex": filters.name, "$options": "i"}

    # --------------------------
    # 6. DATE RANGE FILTER
    # --------------------------
    if filters.created_at_from or filters.created_at_to:
        query["created_at"] = {}

        if filters.created_at_from:
            start = filters.created_at_from
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            query["created_at"]["$gte"] = start

        if filters.created_at_to:
            end = filters.created_at_to
            if isinstance(end, str):
                end = datetime.fromisoformat(end)
            query["created_at"]["$lte"] = end

    # --------------------------
    # 7. ORGANIZATION FILTER
    # --------------------------
    if filters.created_org:
        # Extract organization from user_email
        # Example: testuser@infobellit.com -> infobellit
        org_pattern = f"@{filters.created_org}\\."
        query["user_email"] = {
            "$regex": org_pattern,
            "$options": "i"
        }

    

    # --------------------------
    # 9. EXECUTE QUERY
    # --------------------------
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)

    # Pagination defaults
    page = filters.page or 1
    page_size = filters.page_size or 10
    skip = (page - 1) * page_size

    log_message(LevelType.INFO, f"query generated is : {query}", ErrorCode=-1)

    total_items = await portfolio_collection.count_documents(query)   # FIXED
    total_pages = (total_items + page_size - 1) // page_size

    cursor = (
        portfolio_collection
        .find(query)
        .sort("created_at", DESCENDING)
        .skip(skip)
        .limit(page_size)
    )


    data = []
    skip_fields = {"cloud_cred", "advice_s3_key", "ppt_s3_key", "s3_key", "ip", "udf"}
    DEFAULT_BOOL_FIELDS = [
        "is_locked",
        "is_recommendation_available",
        "is_large_data",
        "is_billing_data",
        "is_cloud_cred",
        "submittedForRecommendations"
    ]

    DEFAULT_INT_FIELDS = [
        "recommendation_percentage",
        "current_instances_count"
    ]


    async for item in cursor:
        filtered_item = {
            k: (str(v) if k == "_id" else v)
            for k, v in item.items()
            if k not in skip_fields
        }
        # 🔥 Apply default boolean values if missing
        for field in DEFAULT_BOOL_FIELDS:
            if field not in filtered_item:
                filtered_item[field] = False

        # 🔥 Apply default integer values if missing
        for field in DEFAULT_INT_FIELDS:
            if field not in filtered_item:
                filtered_item[field] = 0

        data.append(filtered_item)

    # --------------------------
    # 10. FINAL RESPONSE FORMAT
    # --------------------------
    response = {
        "Message": "Portfolio status fetched successfully",
        "ErrorCode": 1,
        "total_items": total_items,
        "current_page": page,
        "total_pages": total_pages,
        "Data": data
    }

    return response

async def list_all_portfolios_without_pagination(token, filters: PortfolioFilter, user_email: str, app_name: str):
    """"""
    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)
    clients_cursor = sales_client_collection.find({"user_email": user_email})
    clients = await clients_cursor.to_list(length=None)
    query = {"app_name": app_name.upper()}
    if clients:
        org_list = [client["client_name"] for client in clients]
        # Create OR regex: infobellit|amd
        org_pattern = "|".join(map(lambda x: x.strip(), org_list))

        query["user_email"] = {
            "$regex": org_pattern,
            "$options": "i"
        }
    else:
        user_email_list = get_user_emailscope(token, user_email, app_name.upper())
        query["user_email"] = {"$in": user_email_list}

    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)

    log_message(LevelType.INFO, f"query generated is : {query}", ErrorCode=-1)

    total_items = await portfolio_collection.count_documents(query)   # FIXED

    cursor = (
        portfolio_collection
        .find(query, {"_id": 1, "name": 1, "user_email": 1, "app_name": 1})  # <-- projection here
        .sort("created_at", DESCENDING)
    )

    items = await cursor.to_list(length=None)

    # 🔥 Convert _id to string for all items
    formatted_items = [
        {
            "id": str(item["_id"]),
            "name": item.get("name", ""),
            "user_email": item.get("user_email", ""),
            "app_name": item.get("app_name", "")
        }
        for item in items
    ]

    return {
        "Message": "Portfolios fetched successfully",
        "ErrorCode": 1,
        "total_items": len(formatted_items),
        "Data": formatted_items
    }
    


async def list_portfolios_service(token, filters: PortfolioFilter, user_email: str, app_name: str):

    if filters.portfolio_id and not filters.list_all:
        return await get_portfolio_data(user_email,filters.portfolio_id)
    if filters.list_all and not filters.is_pagination:
        return await list_all_portfolios_without_pagination(token, filters, user_email, app_name)
    return await list_all_filter_portfolios(token, filters, user_email, app_name)


async def process_lock_unlock_portfolio(portfolio_id: str, user_email: str, app_name: str, is_locked: bool):
    """
    Lock or unlock portfolio by updating is_locked field.
    """

    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)

    # 1. Fetch portfolio
    portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(portfolio_id), "app_name" : app_name})
    if not portfolio_doc:
        log_message(LevelType.ERROR, f"Portfolio not found for app : {app_name}", ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(status_code=404, message=f"Portfolio not found for app : {app_name}", error_code=-1)

    # 2. Ownership check
    if portfolio_doc.get("user_email") != user_email:
        log_message(LevelType.ERROR, f"Unauthorized access: {portfolio_doc.get('user_email')} ≠ {user_email}", ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(status_code=403, message="You are not authorized to lock / unlock this portfolio", error_code=-1)

    # 3. Already locked/unlocked validation
    current_status = portfolio_doc.get("is_locked", False)

    if current_status == is_locked:
        # If trying to lock again
        if is_locked:
            raise CustomAPIException(status_code=400, message="Portfolio is already locked", error_code=-1)
        else:
            # If trying to unlock again
            raise CustomAPIException(status_code=400, message="Portfolio is already unlocked", error_code=-1)

    # 4. Perform update
    update_fields = {"is_locked": is_locked, "uploaded_date": datetime.utcnow()}

    await portfolio_collection.update_one(
        {"_id": ObjectId(portfolio_id)},
        {"$set": update_fields}
    )

    log_message(LevelType.INFO, f"Portfolio {portfolio_id} updated (is_locked={is_locked})", portfolio_id=portfolio_id)

    return {
        "Message": f"Portfolio {'locked' if is_locked else 'unlocked'} successfully",
        "ErrorCode": 1,
        "portfolio_id": portfolio_id,
        "is_locked": is_locked
    }


async def update_organization_instance_count(portfolio_id: str, new_current_instance_count: int, app_name: str):
    """
    Update organization instance count based on portfolio instance count
    """
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)
    org_collection = get_collection(CollectionNames.ORGANIZATION_DATA)

    # Fetch portfolio
    portfolio = await portfolio_collection.find_one({"_id": ObjectId(portfolio_id)})
    if not portfolio:
        return {"status": "error", "message": "Portfolio not found"}

    created_for = portfolio.get("created_for")
    old_count = portfolio.get("current_instances_count", 0)
    is_increment_added = portfolio.get("is_increment_added", False)

    # Fetch org data
    org_doc = await org_collection.find_one({"organization": created_for, "app_name": app_name})
    if not org_doc:
        return {"status": "error", "message": f"Org '{created_for}' not found"}

    # Case 1: First time increment
    if not is_increment_added:
        await org_collection.update_one(
            {"organization": created_for},
            {"$inc": {"current_instance_count": new_current_instance_count, "portfolio_count": 1}}
        )

        await portfolio_collection.update_one(
            {"_id": ObjectId(portfolio_id)},
            {"$set": {"is_increment_added": True}}
        )

        return {
            "status": "incremented",
            "diff": new_current_instance_count,
            "message": f"Added {new_current_instance_count} to org '{created_for}'"
        }

    # Case 2: Already added → adjust diff
    diff = new_current_instance_count - old_count

    if diff == 0:
        return {
            "status": "skipped",
            "diff": 0,
            "message": "No change in instance count"
        }

    # Increment or decrement based on diff
    await org_collection.update_one(
        {"organization": created_for},
        {"$inc": {"current_instance_count": diff}}
    )

    # Update portfolio instance count
    await portfolio_collection.update_one(
        {"_id": ObjectId(portfolio_id)},
        {"$set": {"current_instances_count": new_current_instance_count}}
    )

    return {
        "status": "updated",
        "diff": diff,
        "message": f"Adjusted instance count by {diff} for org '{created_for}'"
    }

