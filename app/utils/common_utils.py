""""""
import re
import uuid
from app.connections.custom_exceptions import CustomAPIException
from app.connections.pylogger import log_message
from .constants import LevelType
import requests
from app.connections.env_config import CS_URL
from pydantic import BaseModel, model_validator
from typing import Union, get_origin, get_args
from copy import deepcopy
from fastapi.routing import APIRoute
from app.utils.constants import ENDPOINT_DOCS
import io
from openpyxl import load_workbook
from msoffcrypto.format.ooxml import OOXMLFile
from typing import Dict, Any
from motor.motor_asyncio import AsyncIOMotorCollection
import pandas as pd
from datetime import timezone, datetime
import ast


def _is_optional(field_annotation) -> bool:
    """Detect Optional[...] (i.e., Union[..., None])."""
    origin = get_origin(field_annotation)
    if origin is Union:
        args = get_args(field_annotation)
        return type(None) in args
    return False

class RequiredFieldValidator(BaseModel):
    @model_validator(mode="before")
    def check_required_fields(cls, values):
        if not isinstance(values, dict):
            return values

        for field_name, field in cls.model_fields.items():
            if _is_optional(field.annotation):
                continue

            if not field.is_required():
                continue

            if field_name not in values and (field.alias not in values):
                raise ValueError(f"{field_name} is required")

            val = values.get(field_name, values.get(field.alias))

            if isinstance(val, str) and val.strip() == "":
                raise ValueError(f"{field_name} is required")

            if isinstance(val, list):
                if len(val) == 0:
                    raise ValueError(f"{field_name} must be a non-empty list")
                if all(isinstance(item, str) and item.strip() == "" for item in val):
                    raise ValueError(f"{field_name} must be a non-empty list")

        return values

class RequiredFieldValidatorBulk(BaseModel):
    @model_validator(mode="before")
    def check_required_fields(cls, values):
        if not isinstance(values, dict):
            return values

        missing_fields = []

        for field_name, field in cls.model_fields.items():
            if _is_optional(field.annotation):
                continue

            if not field.is_required():
                continue

            # Check if field is missing
            if field_name not in values and (field.alias not in values):
                missing_fields.append(field_name)
                continue

            val = values.get(field_name, values.get(field.alias))

            # Check if empty string
            if isinstance(val, str) and val.strip() == "":
                missing_fields.append(field_name)
                continue

            # Check if empty list or list of blank strings
            if isinstance(val, list):
                if len(val) == 0:
                    missing_fields.append(field_name)
                    continue
                if all(isinstance(item, str) and item.strip() == "" for item in val):
                    missing_fields.append(field_name)
                    continue

        if missing_fields:
            missing_fields_str = ", ".join(missing_fields)
            raise CustomAPIException(
                status_code=400,
                message=f"Missing or invalid required fields: {missing_fields_str}. Please refer the API documentation for proper payload",
                error_code=-1
            )
        return values


def save_portfolio_sanity_check(provider, portfolio_name, records, headroom, app_name):
    """basic sanity check"""
    if not all([provider, portfolio_name, records, app_name]):
        msg = "Missing required fields"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)
    if headroom < 0:
        msg = "Headroom must be a positive number"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)

    if len(records) > 25000:
        msg = "Data is too large to process"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=msg)

def get_user_emailscope(token, user_email, app_name):
    """"""
    updated_url = f"{CS_URL.rsplit('/', 1)[0]}/userScope?user_email={user_email}&app_name={app_name}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.request("GET", updated_url, headers=headers, verify=False)
        if response.status_code == 200:
            data = response.json()
            return data.get("user_emails")
        else:
            return [user_email]
    except Exception as err:
        msg = f"Error while user scope fromsc , got error: {err}"
        log_message(LevelType.ERROR, msg, ErrorCode=-1)
        return [user_email]

BASE_RES_CODES = {
    200: {
        "description": "Successful Response",
        "content": {
            "application/json": {
                "examples": {
                    "success": {
                        "summary": "Example success",
                        "value": {
                            "Message": "Operation completed successfully",
                            "ErrorCode": 1}
                    }
                }
            }
        }
    },
    400: {
        "description": "Bad Request",
        "content": {
            "application/json": {
                "examples": {
                    "badRequest": {
                        "summary": "Invalid request",
                        "value": {
                            "Message": "Invalid data",
                            "ErrorCode": -1
                            }
                    }
                }
            }
        }
    },
    401: {
        "description": "Unauthorized",
        "content": {
            "application/json": {
                "examples": {
                    "unauthorized": {
                        "summary": "Auth required",
                        "value": {
                            "Message": "Unauthorized",
                            "ErrorCode": -1
                            }
                    }
                }
            }
        }
    },
    404: {
        "description": "Not Found",
        "content": {
            "application/json": {
                "examples": {
                    "notFound": {
                        "summary": "Resource not found",
                        "value": {
                            "Message": "Data not found",
                            "ErrorCode": -1
                            }
                    }
                }
            }
        }
    },
    422: {
        "description": "Validation Error",
        "content": {
            "application/json": {
                "examples": {
                    "unprocessable": {
                        "summary": "Invalid input",
                        "value": {
                            "Message": "Invalid input value",
                            "ErrorCode": -1
                            }
                    }
                }
            }
        }
    },
    500: {
        "description": "Internal Server Error",
        "content": {
            "application/json": {
                "examples": {
                    "serverError": {
                        "summary": "Unexpected error",
                        "value": {
                            "Message": "Internal server error",
                            "ErrorCode": -1
                            }
                    }
                }
            }
        }
    }
}


def build_res_codes(success_msg: str = "Request Sucessfull"):
    """
    Returns a cloned res_codes dictionary with custom example messages.
    Only replaces messages if provided.
    """
    res = deepcopy(BASE_RES_CODES)
    if success_msg:
        res[200]["content"]["application/json"]["examples"]["success"]["value"]["Message"] = success_msg
    return res


def inject_endpoint_docs(router):
    for route in router.routes:
        if isinstance(route, APIRoute):
            for method in route.methods:
                key = (method, route.path)
                if key in ENDPOINT_DOCS:
                    docs = ENDPOINT_DOCS[key]
                    route.responses = build_res_codes(success_msg=docs["success"])
                    route.summary = docs["summary"]
                    route.description = docs["description"]



def protect_existing_excel(file_path: str, password: str):
    """Reads an existing Excel file and applies password protection (overwrites same file)."""
    # Load the existing Excel file with openpyxl
    wb = load_workbook(file_path)

    # Save to in-memory buffer
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    # Encrypt using msoffcrypto and overwrite same file
    encrypted_file = OOXMLFile(buffer)
    with open(file_path, "wb") as f:
        encrypted_file.encrypt(password, f)

def protect_pptx(file_path: str, password: str):
    """protect ppt with password"""
    # Read the existing file into memory
    with open(file_path, "rb") as f:
        file_data = io.BytesIO(f.read())

    # Encrypt the in-memory file
    file = OOXMLFile(file_data)
    encrypted_data = io.BytesIO()
    file.encrypt(password, encrypted_data)

    # Save back to the same filename
    with open(file_path, "wb") as f:
        f.write(encrypted_data.getvalue())


async def paginate_collection(
    collection: AsyncIOMotorCollection,
    query: Dict[str, Any],
    page: int = 1,
    limit: int = 10,
    projection: Dict[str, int] = None,
    sort: Dict[str, Any] = {"STATUS": ""},
) -> Dict[str, Any]:
    """
    Common pagination utility for MongoDB collections with dict-based sorting.
    
    :param collection: MongoDB collection
    :param query: MongoDB filter query
    :param page: Page number (1-based)
    :param limit: Number of items per page
    :param projection: Fields to include/exclude
    :param sort: Dict specifying sort rules (e.g., {"STATUS": ""})
    :return: Paginated response with metadata
    """
    skip = (page - 1) * limit

    # Handle sorting
    sort_stage = None
    if sort:
        for field, rule in sort.items():
            if rule == "":  # Empty string should come first
                sort_stage = [
                    {
                        "$addFields": {
                            "_empty_sort": {
                                "$cond": [{"$eq": [f"${field}", ""]}, 0, 1]
                            }
                        }
                    },
                    {"$sort": {"_empty_sort": 1, field: 1}},
                    {"$project": {"_empty_sort": 0, **(projection or {})}},
                ]
            else:
                # Normal ascending / descending sort
                direction = 1 if rule == "asc" else -1
                sort_stage = [{"$sort": {field: direction}}]

    if sort_stage:
        pipeline = [
            {"$match": query},
            *(sort_stage or []),
            {"$skip": skip},
            {"$limit": limit},
        ]
        if projection:
            pipeline.insert(1, {"$project": projection})

        cursor = collection.aggregate(pipeline)
        items = await cursor.to_list(length=limit)
    else:
        cursor = collection.find(query, projection or {})
        cursor = cursor.skip(skip).limit(limit)
        items = await cursor.to_list(length=limit)

    # Convert ObjectId to string
    for item in items:
        if "_id" in item:
            item["_id"] = str(item["_id"])

    total_count = await collection.count_documents(query)
    total_pages = (total_count + limit - 1) // limit

    return {
        "items": items,
        "current_page": page,
        "page_size": limit,
        "total_pages": total_pages,
        "total_items": total_count,
    }

def transform_cca_recommandation_data(db_data):
    result = {"Data": []}

    for item in db_data:
        current = {
            "zone": item.get("Zone"),
            "instanceType": item.get("Current Instance"),
            "numberOfInstances": item.get("Number of Instances"),
            "vCPU": item.get("vCPU"),
            "monthlyCost": item.get("Current Monthly Price"),
            "annualCost": item.get("Annual Cost"),
            "cspProvider": item.get("CSP"),
            "pricingModel": item.get("Pricing Model"),
            "status": item.get("STATUS", "")
        }

        # Build recommendations list
        recommendations = []
        for i in ["I", "II", "III"]:
            recommendations.append({
                "zone": item.get(f"Zone {i}", item.get("Zone")),
                "instanceType": item.get(f"Recommendation {i} Instance"),
                "vCPU": item.get(f"vCPU {i}"),
                "monthlyCost": item.get(f"Monthly Price {i}"),
                "totalCost": item.get(f"Annual Cost {i}"),
                "annualSavings": item.get(f"Annual Savings {i}"),
                "savingsInPercentage": item.get(f"Savings % {i}"),
                "perf": item.get(f"Perf Enhancement {i}")
            })

        result["Data"].append({
            "id": item.get("UUID"),
            "data": {
                "currentPlatform": current,
                "recommendations": recommendations
            }
        })

    return result

def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

async def calculate_grand_totals_cca(portfolio_id, recommended_instance_collection):
    """
    Calculate grand totals from a list of transformed db_data items.
    For most numeric fields we calculate the sum.
    For Perf Enhancement I/II/III we calculate the average.
    """
    totals = {
        "Number of Instances": 0,
        "Current Monthly Cost": 0.0,
        "Annual Cost": 0.0,
        "Monthly Cost I": 0.0,
        "Annual Cost I (perf scaled)": 0.0,
        "Annual Savings I": 0.0,
        "Perf Enhancement I": 0.0,
        "Monthly Cost II": 0.0,
        "Annual Cost II (perf scaled)": 0.0,
        "Annual Savings II": 0.0,
        "Perf Enhancement II": 0.0,
        "Monthly Cost III": 0.0,
        "Annual Cost III (perf scaled)": 0.0,
        "Annual Savings III": 0.0,
        "Perf Enhancement III": 0.0,
        "hSavingsInPercentage": 0.0,
        "mSavingsInPercentage": 0.0,
        "mdSavingsInPercentage": 0.0
    }

    # Counters for averages
    perf_counts = {"I": 0, "II": 0, "III": 0}

    instance_cursor = recommended_instance_collection.find({"portfolio_id": portfolio_id})
    data_list = await instance_cursor.to_list(length=None)  # Get all

    # ⭐ new: zone container (count inside same loop)
    unique_zones = set()

    for item in data_list:
        # Track unique Zones
        zone = item.get("Zone")
        if zone:
            unique_zones.add(zone)
        totals["Number of Instances"] += safe_float(item.get("Number of Instances"))
        totals["Current Monthly Cost"] += safe_float(item.get("Current Monthly Price"))
        totals["Annual Cost"] += safe_float(item.get("Annual Cost"))

        totals["Monthly Cost I"] += safe_float(item.get("Monthly Price I"))
        totals["Annual Cost I (perf scaled)"] += safe_float(item.get("Annual Cost I"))
        totals["Annual Savings I"] += safe_float(item.get("Annual Savings I"))
        val = safe_float(item.get("Perf Enhancement I"))
        if val != 0:
            totals["Perf Enhancement I"] += val
            perf_counts["I"] += 1

        totals["Monthly Cost II"] += safe_float(item.get("Monthly Price II"))
        totals["Annual Cost II (perf scaled)"] += safe_float(item.get("Annual Cost II"))
        totals["Annual Savings II"] += safe_float(item.get("Annual Savings II"))
        val = safe_float(item.get("Perf Enhancement II"))
        if val != 0:
            totals["Perf Enhancement II"] += val
            perf_counts["II"] += 1

        totals["Monthly Cost III"] += safe_float(item.get("Monthly Price III"))
        totals["Annual Cost III (perf scaled)"] += safe_float(item.get("Annual Cost III"))
        totals["Annual Savings III"] += safe_float(item.get("Annual Savings III"))
        val = safe_float(item.get("Perf Enhancement III"))
        if val != 0:
            totals["Perf Enhancement III"] += val
            perf_counts["III"] += 1


    # Calculate percentages for savings based on total savings and total cost
    annual_cost = totals["Annual Cost"]
    if annual_cost > 0:
        totals["hSavingsInPercentage"] = (totals["Annual Savings I"] / annual_cost) * 100
        totals["mSavingsInPercentage"] = (totals["Annual Savings II"] / annual_cost) * 100
        totals["mdSavingsInPercentage"] = (totals["Annual Savings III"] / annual_cost) * 100
    else:
        totals["hSavingsInPercentage"] = 0.0
        totals["mSavingsInPercentage"] = 0.0
        totals["mdSavingsInPercentage"] = 0.0

    # Convert sums to averages for Perf Enhancements
    for key in ["I", "II", "III"]:
        count = perf_counts[key]
        if count > 0:
            totals[f"Perf Enhancement {key}"] = totals[f"Perf Enhancement {key}"] / count
        else:
            totals[f"Perf Enhancement {key}"] = 0.0

    # Round all totals to 2 decimals
    grand_total = {k: round(v, 2) for k, v in totals.items()}
    grand_total["uniqueZones"] = len(unique_zones)

    chart_value = dollar_spend_eval_from_flat(data_list)
    return grand_total, chart_value

def transform_eia_data_format(db_data):
    result = {"Data": []}

    for item in db_data:
        # current platform info
        current_platform = {
            "type": item.get("Current Instance"),
            "cost": item.get("Current Monthly Price"),
            "power": item.get("Current Instance Energy Consumption (kwh)"),
            "carbon": item.get("Current Instance Emission"),
            "status": item.get("STATUS", ""),
            "vCPU": item.get("vCPU"),
            "pricingModel": item.get("Pricing Model"),
            "region": item.get("Zone")
        }

        # recommendations list
        recommendations = []
        for i in ["I", "II", "III"]:
            rec_type = item.get(f"Recommendation {i} Instance")
            if not rec_type:
                continue  # skip if recommendation is missing
            recommendations.append({
                "type": rec_type,
                "cost": item.get(f"Monthly Price {i}"),
                "power": item.get(f"Instance Energy Consumption {i} (kwh)"),
                "carbon": item.get(f"Instance Emission {i}"),
                "perf": item.get(f"Perf Enhancement {i}"),
                "monthlySavings": item.get(f"Monthly Savings {i}"),
                "vCPU": item.get(f"vCPU {i}"),
                "untappedCapacity": item.get(f"Untapped Capacity {i}")
            })

        result["Data"].append({
            "id": item.get("UUID"),
            "csp": item.get("CSP"),
            "data": {
                "currentPlatform": current_platform,
                "recommendations": recommendations
            }
        })

    return result


async def calculate_energy_grand_total(portfolio_id, recommended_instance_collection, is_chart_value=False):
    """
    Calculate grand totals / summary for a list of instance data dicts.
    - Most fields are summed.
    - Perf Enhancement I/II are averaged.
    Returns a dictionary with rounded numeric values.
    """

    totals = {
        "Current Monthly Price": 0.0,
        "Current Instance Energy Consumption (kwh)": 0.0,
        "Current Instance Emission": 0.0,
        "Monthly Price I": 0.0,
        "Instance Energy Consumption I (kwh)": 0.0,
        "Instance Emission I": 0.0,
        "Monthly Savings I": 0.0,
        "Perf Enhancement I": 0.0,
        "Untapped Capacity I": 0.0,
        "Monthly Price II": 0.0,
        "Instance Energy Consumption II (kwh)": 0.0,
        "Instance Emission II": 0.0,
        "Monthly Savings II": 0.0,
        "Perf Enhancement II": 0.0,
        "Untapped Capacity II": 0.0
    }

    # Counters for averages
    perf_counts = {"I": 0, "II": 0}
    untapped_counts = {"I": 0, "II": 0}

    instance_cursor = recommended_instance_collection.find({"portfolio_id": portfolio_id})
    data_list = await instance_cursor.to_list(length=None)  # Get all

    unique_zones = set()

    for item in data_list:
        # Track unique Zones
        zone = item.get("Zone")
        if zone:
            unique_zones.add(zone)
        totals["Current Monthly Price"] += safe_float(item.get("Current Monthly Price"))
        totals["Current Instance Energy Consumption (kwh)"] += safe_float(item.get("Current Instance Energy Consumption (kwh)"))
        totals["Current Instance Emission"] += safe_float(item.get("Current Instance Emission"))

        totals["Monthly Price I"] += safe_float(item.get("Monthly Price I"))
        totals["Instance Energy Consumption I (kwh)"] += safe_float(item.get("Instance Energy Consumption I (kwh)"))
        totals["Instance Emission I"] += safe_float(item.get("Instance Emission I"))
        totals["Monthly Savings I"] += safe_float(item.get("Monthly Savings I"))

        val = safe_float(item.get("Perf Enhancement I"))
        if val != 0:
            totals["Perf Enhancement I"] += val
            perf_counts["I"] += 1

        val = safe_float(item.get("Untapped Capacity I"))
        if val != 0:
            totals["Untapped Capacity I"] += val
            untapped_counts["I"] += 1

        totals["Monthly Price II"] += safe_float(item.get("Monthly Price II"))
        totals["Instance Energy Consumption II (kwh)"] += safe_float(item.get("Instance Energy Consumption II (kwh)"))
        totals["Instance Emission II"] += safe_float(item.get("Instance Emission II"))
        totals["Monthly Savings II"] += safe_float(item.get("Monthly Savings II"))

        val = safe_float(item.get("Perf Enhancement II"))
        if val != 0:
            totals["Perf Enhancement II"] += val
            perf_counts["II"] += 1
            
        val = safe_float(item.get("Untapped Capacity II"))
        if val != 0:
            totals["Untapped Capacity II"] += val
            untapped_counts["II"] += 1

    # Convert sums → averages for Perf Enhancements & Untapped Capacity
    for key in ["I", "II"]:
        count = perf_counts[key]
        if count > 0:
            totals[f"Perf Enhancement {key}"] = totals[f"Perf Enhancement {key}"] / count
        else:
            totals[f"Perf Enhancement {key}"] = 0.0

        count_untapped = untapped_counts[key]
        if count_untapped > 0:
            totals[f"Untapped Capacity {key}"] = totals[f"Untapped Capacity {key}"] / count_untapped
        else:
            totals[f"Untapped Capacity {key}"] = 0.0

    # Round all totals one final time
    grand_total = {k: round(v, 2) for k, v in totals.items()}
    grand_total["uniqueZones"] = len(unique_zones)
    grand_total["Number of Instances"] = len(data_list)

    if is_chart_value:
        chart_value = energy_chart_eval_from_flat(data_list)
    else:
        chart_value = None

    return grand_total, chart_value

async def reformat_recommendation_data(db_data, app_name, portfolio_id, recommended_instance_collection, is_chart_value=False):
    if app_name == "CCA":
        data = transform_cca_recommandation_data(db_data)
        grand_total, chart_value = await calculate_grand_totals_cca(portfolio_id, recommended_instance_collection)
        return data, grand_total, chart_value
    else:
        data = transform_eia_data_format(db_data)
        grand_total, chart_value = await calculate_energy_grand_total(portfolio_id, recommended_instance_collection, is_chart_value)
        return data, grand_total, chart_value

def paginate_transformed_data(
    transformed_data,
    page: int = 1,
    page_size: int = 10,
    sort: Dict[str, str] = {"STATUS": ""}
) -> Dict[str, Any]:
    """
    Paginate and sort the transformed_data list.

    Args:
        transformed_data (list): List of dicts (your transformed objects).
        page (int): Current page number (default=1).
        page_size (int): Number of items per page (default=10).
        sort (dict): Dict specifying sort rules (e.g., {"STATUS": ""}, {"name": "asc"})

    Returns:
        dict: {
            "Data": [...],
            "current_page": <int>,
            "page_size": <int>,
            "total_pages": <int>
        }
    """
    # Sorting
    if sort:
        for field, rule in sort.items():
            if rule == "":
                # Empty values first
                transformed_data = sorted(
                    transformed_data,
                    key=lambda x: (x.get(field, "") != "", str(x.get(field, "")))
                )
            elif rule.lower() == "asc":
                transformed_data = sorted(
                    transformed_data,
                    key=lambda x: x.get(field, "")
                )
            elif rule.lower() == "desc":
                transformed_data = sorted(
                    transformed_data,
                    key=lambda x: x.get(field, ""),
                    reverse=True
                )

    total_items = len(transformed_data)
    total_pages = (total_items + page_size - 1) // page_size  # ceil division

    # Calculate slice range
    start = (page - 1) * page_size
    end = start + page_size
    paginated_data = transformed_data[start:end]

    return {
        "Data": paginated_data,
        "current_page": page,
        "page_size": page_size,
        "total_pages": total_pages
    }


def format_currency(value: float) -> str:
    """Format number into $ with K/M suffixes."""
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"

def dollar_spend_eval_from_json(data, grand_total):
    # Extract currentPlatform info into a flat list
    rows = []
    for item in data:
        cp = item["data"]["currentPlatform"]
        try:
            annual_cost = float(cp.get("annualCost", 0)) if cp.get("annualCost") not in ["-", None, ""] else 0
        except:
            annual_cost = 0
        rows.append({
            "Current Instance": cp.get("instanceType", "Unknown"),
            "Total Annual Cost": annual_cost
        })
    
    # Make dataframe
    df = pd.DataFrame(rows)
    if df.empty:
        return {}
    
    # Group by Current Instance and sum Annual Cost
    grouped = (
        df.groupby("Current Instance", as_index=False)
        .agg({"Total Annual Cost": "sum"})
    )
    sorted_grouped = grouped.sort_values(by="Total Annual Cost", ascending=False).reset_index(drop=True)

    total_cost = sorted_grouped["Total Annual Cost"].sum()

    first_cost = sorted_grouped.iloc[0]["Total Annual Cost"] if not sorted_grouped.empty else 0
    max_spend = sorted_grouped.iloc[0]["Current Instance"] if not sorted_grouped.empty else None
    item_4a = round((first_cost / total_cost) * 100, 1) if total_cost != 0 else 0

    next_10_sum = sorted_grouped.iloc[1:11]["Total Annual Cost"].sum()
    item_4b = round((next_10_sum / total_cost) * 100, 1) if total_cost != 0 else 0

    remaining_sum = sorted_grouped.iloc[11:]["Total Annual Cost"].sum()
    item_4c = round((remaining_sum / total_cost) * 100, 1) if total_cost != 0 else 0

    # Extract from grandTotal (raw numbers)
    item_3b = float(grand_total.get("Annual Cost", 0))
    item_5a = float(grand_total.get("Annual Cost I (perf scaled)", 0))
    item_5b = float(grand_total.get("Annual Cost II (perf scaled)", 0))
    item_5c = float(grand_total.get("Annual Cost III (perf scaled)", 0))

    return {
        "Current Spend": {"raw": item_3b, "display": format_currency(item_3b)},
        "Hourly Cost Optimization": {"raw": item_5a, "display": format_currency(item_5a)},
        "Modernize": {"raw": item_5b, "display": format_currency(item_5b)},
        "Modernize & Downsize": {"raw": item_5c, "display": format_currency(item_5c)},
        "Dollar Spend (Top Instance)": {
            "instance": max_spend,
            "percentage": item_4a
        },
        "Next 10": item_4b,
        "Rest": item_4c
    }

def dollar_spend_eval_from_flat(data):
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    if df.empty:
        return {}

    # Clean numeric columns
    numeric_cols = [
        "Annual Cost",
        "Annual Cost I",
        "Annual Cost II",
        "Annual Cost III"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Aggregate totals
    total_current = df["Annual Cost"].sum()
    total_i = df["Annual Cost I"].sum()
    total_ii = df["Annual Cost II"].sum()
    total_iii = df["Annual Cost III"].sum()

    # Group by Current Instance and sum Annual Cost
    grouped = (
        df.groupby("Current Instance", as_index=False)
        .agg({"Annual Cost": "sum"})
    )

    # Sort for dollar spend distribution
    sorted_grouped = grouped.sort_values(by="Annual Cost", ascending=False).reset_index(drop=True)
    total_cost = total_current

    first_cost = sorted_grouped.iloc[0]["Annual Cost"] if not sorted_grouped.empty else 0
    max_spend = sorted_grouped.iloc[0]["Current Instance"] if not sorted_grouped.empty else None
    item_4a = round((first_cost / total_cost) * 100, 1) if total_cost != 0 else 0

    next_10_sum = sorted_grouped.iloc[1:11]["Annual Cost"].sum()
    item_4b = round((next_10_sum / total_cost) * 100, 1) if total_cost != 0 else 0

    remaining_sum = sorted_grouped.iloc[11:]["Annual Cost"].sum()
    item_4c = round((remaining_sum / total_cost) * 100, 1) if total_cost != 0 else 0

    return {
        "Current Spend": {"raw": total_current, "display": format_currency(total_current)},
        "Hourly Cost Optimization": {"raw": total_i, "display": format_currency(total_i)},
        "Modernize": {"raw": total_ii, "display": format_currency(total_ii)},
        "Modernize & Downsize": {"raw": total_iii, "display": format_currency(total_iii)},
        "Dollar Spend (Top Instance)": {
            "instance": max_spend,
            "percentage": item_4a
        },
        "Next 10": item_4b,
        "Rest": item_4c
    }

def generate_user_name_from_email(user_email: str) -> str:
    """
    Generate a user_name from user_email.
    
    Rules:
    - Split email by '@'
    - Replace '.' with '_' in local part
    - Take first part of domain before '.' 
    - Combine with '_' → local_part_domain_part
    """
    try:
        local_part, domain_part = user_email.split("@")
        local_part = local_part.replace(".", "_")
        domain_part = domain_part.split(".")[0]
        return f"{local_part}_{domain_part}"
    except Exception:
        # Fallback: replace @ and dots with underscores if unexpected format
        return user_email.replace("@", "_").replace(".", "_")

def get_cca_pipeline(portfolio_id):
    return [
            {"$match": {"portfolio_id": portfolio_id}},
            {"$group": {
                "_id": None,
                "Current_Vcpus": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU", "-"]},
                            {"$eq": ["$vCPU", None]},
                            {"$eq": ["$vCPU", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU"}
                    ]
                }},
                "HC_VCPUs": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU I", "-"]},
                            {"$eq": ["$vCPU I", None]},
                            {"$eq": ["$vCPU I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU I"}
                    ]
                }},
                "M_VCPUs": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU II", "-"]},
                            {"$eq": ["$vCPU II", None]},
                            {"$eq": ["$vCPU II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU II"}
                    ]
                }},
                "MD_VCPUs": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU III", "-"]},
                            {"$eq": ["$vCPU III", None]},
                            {"$eq": ["$vCPU III", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU III"}
                    ]
                }},
                "Sum_H_Saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings I", "-"]},
                            {"$eq": ["$Annual Savings I", None]},
                            {"$eq": ["$Annual Savings I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings I"}
                    ]
                }},
                "Sum_M_Saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings II", "-"]},
                            {"$eq": ["$Annual Savings II", None]},
                            {"$eq": ["$Annual Savings II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings II"}
                    ]
                }},
                "Sum_MD_Saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings III", "-"]},
                            {"$eq": ["$Annual Savings III", None]},
                            {"$eq": ["$Annual Savings III", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings III"}
                    ]
                }},
                "count_perfI": {"$sum": {"$cond": [{"$in": ["$Perf Enhancement I", ["-", None, ""]]}, 0, 1]}},
                "count_perfII": {"$sum": {"$cond": [{"$in": ["$Perf Enhancement II", ["-", None, ""]]}, 0, 1]}},
                "count_perfIII": {"$sum": {"$cond": [{"$in": ["$Perf Enhancement III", ["-", None, ""]]}, 0, 1]}},
                "sum_perfI": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Perf Enhancement I", "-"]},
                            {"$eq": ["$Perf Enhancement I", None]},
                            {"$eq": ["$Perf Enhancement I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Perf Enhancement I"}
                    ]
                }},
                "sum_perfII": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Perf Enhancement II", "-"]},
                            {"$eq": ["$Perf Enhancement II", None]},
                            {"$eq": ["$Perf Enhancement II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Perf Enhancement II"}
                    ]
                }},
                "sum_perfIII": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Perf Enhancement III", "-"]},
                            {"$eq": ["$Perf Enhancement III", None]},
                            {"$eq": ["$Perf Enhancement III", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Perf Enhancement III"}
                    ]
                }},
                
                # --- Cost Calculations (Annual) ---
                "current_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Cost", "-"]},
                            {"$eq": ["$Annual Cost", None]},
                            {"$eq": ["$Annual Cost", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Cost"}
                    ]
                }},
                "HC_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Cost I", "-"]},
                            {"$eq": ["$Annual Cost I", None]},
                            {"$eq": ["$Annual Cost I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Cost I"}
                    ]
                }},
                "M_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Cost II", "-"]},
                            {"$eq": ["$Annual Cost II", None]},
                            {"$eq": ["$Annual Cost II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Cost II"}
                    ]
                }},
                "M_D_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Cost III", "-"]},
                            {"$eq": ["$Annual Cost III", None]},
                            {"$eq": ["$Annual Cost III", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Cost III"}
                    ]
                }},
                
                # --- New Saving Fields (duplicates of Sum_* keys but naming aligned with request) ---
                "HC_saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings I", "-"]},
                            {"$eq": ["$Annual Savings I", None]},
                            {"$eq": ["$Annual Savings I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings I"}
                    ]
                }},
                "M_saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings II", "-"]},
                            {"$eq": ["$Annual Savings II", None]},
                            {"$eq": ["$Annual Savings II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings II"}
                    ]
                }},
                "M_D_saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Annual Savings III", "-"]},
                            {"$eq": ["$Annual Savings III", None]},
                            {"$eq": ["$Annual Savings III", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Annual Savings III"}
                    ]
                }},
                
                # --- Instance and Region Counts ---
                "recommendation_instances_count": {
                    "$sum": {
                        "$cond": [
                            {"$or": [
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price I", "-"]},
                                            {"$eq": ["$Monthly Price I", None]},
                                            {"$eq": ["$Monthly Price I", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price I"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price II", "-"]},
                                            {"$eq": ["$Monthly Price II", None]},
                                            {"$eq": ["$Monthly Price II", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price II"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price III", "-"]},
                                            {"$eq": ["$Monthly Price III", None]},
                                            {"$eq": ["$Monthly Price III", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price III"}
                                    ]
                                }, 0]}
                            ]},
                            1,
                            0
                        ]
                    }
                },
                "unique_zones": {
                    "$addToSet": {
                        "$cond": [
                            {"$or": [
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price I", "-"]},
                                            {"$eq": ["$Monthly Price I", None]},
                                            {"$eq": ["$Monthly Price I", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price I"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price II", "-"]},
                                            {"$eq": ["$Monthly Price II", None]},
                                            {"$eq": ["$Monthly Price II", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price II"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price III", "-"]},
                                            {"$eq": ["$Monthly Price III", None]},
                                            {"$eq": ["$Monthly Price III", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price III"}
                                    ]
                                }, 0]}
                            ]},
                            "$Zone I",
                            None
                        ]
                    }
                },
                
                "recommendation_date": {"$min": "$created_at"},
            }},
            {"$project": {
                "_id": 0,
                "Current_Vcpus": 1,
                "HC_VCPUs": 1,
                "M_VCPUs": 1,
                "MD_VCPUs": 1,
                "recommendation_date": 1,
                "H_Perf": {
                    "$cond": [
                        {"$eq": ["$count_perfI", 0]},
                        0,
                        {"$divide": ["$sum_perfI", "$count_perfI"]}
                    ]
                },
                "M_Perf": {
                    "$cond": [
                        {"$eq": ["$count_perfII", 0]},
                        0,
                        {"$divide": ["$sum_perfII", "$count_perfII"]}
                    ]
                },
                "MD_Perf": {
                    "$cond": [
                        {"$eq": ["$count_perfIII", 0]},
                        0,
                        {"$divide": ["$sum_perfIII", "$count_perfIII"]}
                    ]
                },
                "Total_Perf": {
                    "$let": {
                        "vars": {
                            "sumPerf": {"$add": ["$sum_perfI", "$sum_perfII", "$sum_perfIII"]},
                            "countPerf": {"$add": ["$count_perfI", "$count_perfII", "$count_perfIII"]}
                        },
                        "in": {
                            "$cond": [
                                {"$eq": ["$$countPerf", 0]},
                                0,
                                {"$divide": ["$$sumPerf", "$$countPerf"]}
                            ]
                        }
                    }
                },
                "H_Saving": "$Sum_H_Saving",
                "M_Saving": "$Sum_M_Saving",
                "MD_Saving": "$Sum_MD_Saving",
                
                # Cost fields
                "current_cost": 1,
                "HC_cost": 1,
                "M_cost": 1,
                "M_D_cost": 1,
                "HC_saving": 1,
                "M_saving": 1,
                "M_D_saving": 1,
                
                # Count fields
                "recommendation_instances_count": 1,
                "recommendation_regions_count": {
                    "$size": {
                        "$filter": {
                            "input": "$unique_zones",
                            "as": "z",
                            "cond": {"$and": [{"$ne": ["$$z", None]}, {"$ne": ["$$z", ""]}, {"$ne": ["$$z", "-"]}]}
                        }
                    }
                }
            }}
        ]

def get_eia_pipeline(portfolio_id):
    return [
            {"$match": {"portfolio_id": portfolio_id}},
            {"$group": {
                "_id": None,
                "Current_Vcpus": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU", "-"]},
                            {"$eq": ["$vCPU", None]},
                            {"$eq": ["$vCPU", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU"}
                    ]
                }},

                "HC_VCPUs": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU I", "-"]},
                            {"$eq": ["$vCPU I", None]},
                            {"$eq": ["$vCPU I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU I"}
                    ]
                }},

                "M_VCPUs": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$vCPU II", "-"]},
                            {"$eq": ["$vCPU II", None]},
                            {"$eq": ["$vCPU II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$vCPU II"}
                    ]
                }},

                "Sum_H_Saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Savings I", "-"]},
                            {"$eq": ["$Monthly Savings I", None]},
                            {"$eq": ["$Monthly Savings I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Savings I"}
                    ]
                }},

                "Sum_M_Saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Savings II", "-"]},
                            {"$eq": ["$Monthly Savings II", None]},
                            {"$eq": ["$Monthly Savings II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Savings II"}
                    ]
                }},

                "Sum_MD_Saving": {"$sum": 0},
                "H_carboncount": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Instance Energy Consumption I (kwh)", "-"]},
                            {"$eq": ["$Instance Energy Consumption I (kwh)", None]},
                            {"$eq": ["$Instance Energy Consumption I (kwh)", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Instance Energy Consumption I (kwh)"}
                    ]
                }},

                "M_carboncount": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Instance Energy Consumption II (kwh)", "-"]},
                            {"$eq": ["$Instance Energy Consumption II (kwh)", None]},
                            {"$eq": ["$Instance Energy Consumption II (kwh)", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Instance Energy Consumption II (kwh)"}
                    ]
                }},

                "H_energycount": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Instance Emission I", "-"]},
                            {"$eq": ["$Instance Emission I", None]},
                            {"$eq": ["$Instance Emission I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Instance Emission I"}
                    ]
                }},

                "M_energycount": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Instance Emission II", "-"]},
                            {"$eq": ["$Instance Emission II", None]},
                            {"$eq": ["$Instance Emission II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Instance Emission II"}
                    ]
                }},

                "count_perfI": {"$sum": {"$cond": [{"$in": ["$Perf Enhancement I", ["-", None, ""]]}, 0, 1]}},

                "count_perfII": {"$sum": {"$cond": [{"$in": ["$Perf Enhancement II", ["-", None, ""]]}, 0, 1]}},

                "count_perfIII": {"$sum": 0},
                "sum_perfI": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Perf Enhancement I", "-"]},
                            {"$eq": ["$Perf Enhancement I", None]},
                            {"$eq": ["$Perf Enhancement I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Perf Enhancement I"}
                    ]
                }},

                "sum_perfII": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Perf Enhancement II", "-"]},
                            {"$eq": ["$Perf Enhancement II", None]},
                            {"$eq": ["$Perf Enhancement II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Perf Enhancement II"}
                    ]
                }},
                "sum_perfIII": {"$sum": 0},
                
                # --- Cost Calculations (Monthly) ---
                "current_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Current Monthly Price", "-"]},
                            {"$eq": ["$Current Monthly Price", None]},
                            {"$eq": ["$Current Monthly Price", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Current Monthly Price"}
                    ]
                }},
                "O_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Price I", "-"]},
                            {"$eq": ["$Monthly Price I", None]},
                            {"$eq": ["$Monthly Price I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Price I"}
                    ]
                }},
                "G_cost": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Price II", "-"]},
                            {"$eq": ["$Monthly Price II", None]},
                            {"$eq": ["$Monthly Price II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Price II"}
                    ]
                }},
                
                # --- New Saving Fields ---
                "O_saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Savings I", "-"]},
                            {"$eq": ["$Monthly Savings I", None]},
                            {"$eq": ["$Monthly Savings I", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Savings I"}
                    ]
                }},
                "G_saving": {"$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$Monthly Savings II", "-"]},
                            {"$eq": ["$Monthly Savings II", None]},
                            {"$eq": ["$Monthly Savings II", ""]}
                        ]},
                        0,
                        {"$toDouble": "$Monthly Savings II"}
                    ]
                }},
                
                # --- Instance and Region Counts ---
                "recommendation_instances_count": {
                    "$sum": {
                        "$cond": [
                            {"$or": [
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price I", "-"]},
                                            {"$eq": ["$Monthly Price I", None]},
                                            {"$eq": ["$Monthly Price I", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price I"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price II", "-"]},
                                            {"$eq": ["$Monthly Price II", None]},
                                            {"$eq": ["$Monthly Price II", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price II"}
                                    ]
                                }, 0]}
                            ]},
                            1,
                            0
                        ]
                    }
                },
                "unique_zones": {
                    "$addToSet": {
                        "$cond": [
                            {"$or": [
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price I", "-"]},
                                            {"$eq": ["$Monthly Price I", None]},
                                            {"$eq": ["$Monthly Price I", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price I"}
                                    ]
                                }, 0]},
                                {"$gt": [{
                                    "$cond": [
                                        {"$or": [
                                            {"$eq": ["$Monthly Price II", "-"]},
                                            {"$eq": ["$Monthly Price II", None]},
                                            {"$eq": ["$Monthly Price II", ""]}
                                        ]},
                                        0,
                                        {"$toDouble": "$Monthly Price II"}
                                    ]
                                }, 0]}
                            ]},
                            "$Zone",
                            None
                        ]
                    }
                },
                
                "recommendation_date": {"$min": "$created_at"},
            }},
            {"$project": {
                "_id": 0,
                "Current_Vcpus": 1,
                "HC_VCPUs": 1,
                "M_VCPUs": 1,
                "H_carboncount": 1,
                "M_carboncount": 1,
                "H_energycount": 1,
                "M_energycount": 1,
                "recommendation_date": 1,
                "H_Perf": {
                    "$cond": [
                        {"$eq": ["$count_perfI", 0]},
                        0,
                        {"$divide": ["$sum_perfI", "$count_perfI"]}
                    ]
                },
                "M_Perf": {
                    "$cond": [
                        {"$eq": ["$count_perfII", 0]},
                        0,
                        {"$divide": ["$sum_perfII", "$count_perfII"]}
                    ]
                },
                "MD_Perf": {"$literal": 0},
                "Total_Perf": {
                    "$let": {
                        "vars": {
                            "sumPerf": {"$add": ["$sum_perfI", "$sum_perfII"]},
                            "countPerf": {"$add": ["$count_perfI", "$count_perfII"]}
                        },
                        "in": {
                            "$cond": [
                                {"$eq": ["$countPerf", 0]},
                                0,
                                {"$divide": ["$sumPerf", "$countPerf"]}
                            ]
                        }
                    }
                },
                "H_Saving": "$Sum_H_Saving",
                "M_Saving": "$Sum_M_Saving",
                "MD_Saving": {"$literal": 0},
                
                # Cost fields
                "current_cost": 1,
                "O_cost": 1,
                "G_cost": 1,
                "O_saving": 1,
                "G_saving": 1,
                
                # Count fields
                "recommendation_instances_count": 1,
                "recommendation_regions_count": {
                    "$size": {
                        "$filter": {
                            "input": "$unique_zones",
                            "as": "z",
                            "cond": {"$and": [{"$ne": ["$$z", None]}, {"$ne": ["$$z", ""]}, {"$ne": ["$$z", "-"]}]}
                        }
                    }
                }
            }}
        ]

def extract_org_and_user_from_email(email: str):
    if '@' in email:
        user, org_part = email.split('@', 1)
        org = org_part.split('.')[0].lower() if '.' in org_part else org_part.lower()
        return user.lower(), org
    else:
        lowered = email.lower()
        return lowered, lowered


def convert_to_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_aws_billing_data(df, provider, portfolio_id, regions_map):
    """Parse AWS billing data into a standardized format."""
    
    # ✅ Convert all column names to lowercase
    df.columns = df.columns.str.lower()

    required_cols = [
        "lineitem/productcode",
        "lineitem/operation",
        "product/productfamily",
        "lineitem/resourceid",
        "product/regioncode",
        "product/instancetype",
        "lineitem/usageamount",
        "pricing/term"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        msg = f"Missing required AWS billing columns: {missing}. Please download the template to get exact headers."
        log_message(LevelType.ERROR, msg, ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(
            status_code=422,
            message=msg,
            error_code=-1
        )
    # --- Filter Data ---
    filtered = df[
        (df["lineitem/productcode"] == "AmazonEC2") &
        (df["lineitem/operation"].str.startswith("RunInstances", na=False)) &
        (df["product/productfamily"] == "Compute Instance")
    ]
    # --- Map and transform columns ---
    mapped_df = filtered.rename(columns={
        "lineitem/resourceid": "uuid",
        "product/regioncode": "region",
        "product/instancetype": "size",
        "lineitem/usageamount": "total number of hours per month",
        "pricing/term": "pricing model"
    })[[
        "uuid", "region", "size", "total number of hours per month", "pricing model"
    ]]

    # --- Add extra fields ---
    mapped_df["cloud"] = provider
    mapped_df["quantity"] = 1

    # --- Return DataFrame ---
    return mapped_df

def normalize_instance_name(name):
    """Normalize Azure instance SKU names like 'Standard_D2s_v3'."""
    name = str(name).split('/')[-1].strip()
    name_clean = re.sub(r'^(standard[_\s-]*)', '', name, flags=re.IGNORECASE)
    normalized = "standard_" + re.sub(r'[\s-]+', '_', name_clean.lower())
    return normalized

def parse_azure_billing_data(df, provider, portfolio_id , regions_map):
    """Parse Azure billing data into standardized VM usage format."""

    azure_regions = regions_map.get(provider)

    # ✅ Lowercase all columns
    df.columns = df.columns.str.lower()

    required_cols = ["servicename", "meter", "resourcelocation", "costusd"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        msg = f"Missing required Azure billing columns: {missing}. Please download the template to get exact headers."
        log_message(LevelType.ERROR, msg, ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(
            status_code=422,
            message=msg,
            error_code=-1
        )

    # ✅ Filter to virtual machines
    vm_df = df[df["servicename"].str.lower() == "virtual machines"].copy()
    vm_df["size"] = vm_df['meter'].apply(normalize_instance_name)

    if vm_df.empty:
        raise CustomAPIException(
            status_code=404,
            message="No Virtual Machine records found for Azure billing.",
            error_code=-1
        )

    # ✅ Function to normalize Azure region
    def normalize_region(raw_region: str):
        if not raw_region or pd.isna(raw_region):
            return ""
        region = str(raw_region).strip().lower().replace("-", " ")

        # Example: "us east" → ["us", "east"] → reversed → "east us" → "eastus"
        words = region.split()
        reversed_region = "".join(words[::-1])

        # Pick the best match from list (if exists)
        if reversed_region in azure_regions:
            return reversed_region
        elif region.replace(" ", "") in azure_regions:
            return region.replace(" ", "")
        else:
            # fallback: leave normalized basic form
            return reversed_region

    # ✅ Apply region normalization
    vm_df["region"] = vm_df["resourcelocation"].apply(normalize_region)

    # ✅ Build output DataFrame
    mapped_df = pd.DataFrame({
        "uuid": [str(uuid.uuid4()) for _ in range(len(vm_df))],
        "cloud": provider,
        "region": vm_df["region"],
        "size": vm_df["size"],
        "quantity": 1,
        "total number of hours per month": vm_df["costusd"],
        "pricing model": "OnDemand"
    })

    return mapped_df

def parse_gcp_billing_data(df, provider, portfolio_id , regions_map):
    """
    Parse GCP billing data.
    """

    required_cols = ["region", "instance_type"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        msg = f"Missing required GCP billing columns: {missing}. Please download the template to get exact headers."
        log_message(LevelType.ERROR, msg, ErrorCode=-1, portfolio_id=portfolio_id)
        raise CustomAPIException(
            status_code=422,
            message=msg,
            error_code=-1
        )
    # ✅ Build output DataFrame
    mapped_df = pd.DataFrame({
        "uuid": [str(uuid.uuid4()) for _ in range(len(df))],
        "cloud": provider,
        "region": df["region"],
        "size": df  ["instance_type"],
        "quantity": 1,
        "total number of hours per month": 730,
        "pricing model": "OnDemand"
    })
    return mapped_df

BILLING_PARSERS = {
    "AWS": parse_aws_billing_data,
    "AZURE": parse_azure_billing_data,
    "GCP": parse_gcp_billing_data
}

def extract_organization_from_email(email: str) -> str | None:
    if "@" not in email:
        return email
    domain = email.split("@", 1)[1]
    return domain.split(".", 1)[0] if "." in domain else domain


def energy_chart_eval_from_flat(data):
    """
    Build full EIA chart values including:
      - Annual Cost (Current / Optimal / Good)
      - Annual Savings (I & II)
      - Energy totals (Power)
      - Emission totals (Carbon)
      - Dollar Spend Distribution (%) using the same logic as CCA
    Assumes format_currency(...) and pandas as pd are available.
    """
    df = pd.DataFrame(data)
    if df.empty:
        return {}

    # -------------------------------
    # CLEAN / CAST NUMERIC FIELDS
    # -------------------------------
    numeric_cols = [
        "Current Instance Energy Consumption (kwh)",
        "Current Instance Emission",
        "Current Monthly Price",
        "Instance Energy Consumption I (kwh)",
        "Instance Emission I",
        "Monthly Price I",
        "Monthly Savings I",
        "Instance Energy Consumption II (kwh)",
        "Instance Emission II",
        "Monthly Price II",
        "Monthly Savings II"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # -------------------------------
    # ANNUALIZE COSTS & SAVINGS
    # -------------------------------
    # Ensure columns exist (create if missing)
    df["Current Monthly Price"] = df.get("Current Monthly Price", 0)
    df["Monthly Price I"] = df.get("Monthly Price I", 0)
    df["Monthly Price II"] = df.get("Monthly Price II", 0)
    df["Monthly Savings I"] = df.get("Monthly Savings I", 0)
    df["Monthly Savings II"] = df.get("Monthly Savings II", 0)

    df["Annual Cost"] = df["Current Monthly Price"] * 12
    df["Annual Cost I"] = df["Monthly Price I"] * 12
    df["Annual Cost II"] = df["Monthly Price II"] * 12

    df["Annual Savings I"] = df["Monthly Savings I"] * 12
    df["Annual Savings II"] = df["Monthly Savings II"] * 12

    # -------------------------------
    # POWER / CARBON / ANNUAL COST BLOCKS
    # -------------------------------
    def build_block(power_col, carbon_col, annual_cost_col, annual_savings_col=None):
        power_raw = df.get(power_col, pd.Series(dtype=float)).sum()
        carbon_raw = df.get(carbon_col, pd.Series(dtype=float)).sum()
        cost_raw = df.get(annual_cost_col, pd.Series(dtype=float)).sum()
        block = {
            "Power": {"raw": power_raw, "display": f"{round(power_raw)}"},
            "Carbon": {"raw": carbon_raw, "display": f"{round(carbon_raw)}"},
            "Annual Cost": {"raw": cost_raw, "display": format_currency(cost_raw)}
        }
        if annual_savings_col:
            sav = df.get(annual_savings_col, pd.Series(dtype=float)).sum()
            block["Annual Savings"] = {"raw": sav, "display": format_currency(sav)}
        return block

    result = {
        "Current": build_block(
            "Current Instance Energy Consumption (kwh)",
            "Current Instance Emission",
            "Annual Cost"
        ),
        "Optimal": build_block(
            "Instance Energy Consumption I (kwh)",
            "Instance Emission I",
            "Annual Cost I",
            "Annual Savings I"
        ),
        "Good": build_block(
            "Instance Energy Consumption II (kwh)",
            "Instance Emission II",
            "Annual Cost II",
            "Annual Savings II"
        )
    }

    # -------------------------------
    # DOLLAR SPEND DISTRIBUTION (CCA logic)
    # -------------------------------
    # Group by Current Instance using CURRENT Annual Cost
    if "Current Instance" in df.columns:
        grouped = (
            df.groupby("Current Instance", as_index=False)
              .agg({"Annual Cost": "sum"})
        )
    else:
        # If no instance label, treat whole as single bucket
        grouped = pd.DataFrame({"Current Instance": [None], "Annual Cost": [df["Annual Cost"].sum()]})

    sorted_grouped = grouped.sort_values(by="Annual Cost", ascending=False).reset_index(drop=True)
    total_cost = sorted_grouped["Annual Cost"].sum()

    # Top instance (first row)
    if not sorted_grouped.empty:
        first_cost = float(sorted_grouped.iloc[0]["Annual Cost"])
        top_inst = sorted_grouped.iloc[0]["Current Instance"]
    else:
        first_cost = 0.0
        top_inst = None

    top_pct = round((first_cost / total_cost) * 100, 1) if total_cost else 0.0

    # Next 10
    next_10_sum = float(sorted_grouped.iloc[1:11]["Annual Cost"].sum()) if len(sorted_grouped) > 1 else 0.0
    next_10_pct = round((next_10_sum / total_cost) * 100, 1) if total_cost else 0.0

    # Rest (from 11th index onward)
    rest_sum = float(sorted_grouped.iloc[11:]["Annual Cost"].sum()) if len(sorted_grouped) > 11 else 0.0
    rest_pct = round((rest_sum / total_cost) * 100, 1) if total_cost else 0.0

    # Dollar Spend block - mirror CCA naming/values (Annual)
    result["Dollar Spend"] = {
        "Current Spend": {"raw": df["Annual Cost"].sum(), "display": format_currency(df["Annual Cost"].sum())},
        "Optimal Spend": {"raw": df["Annual Cost I"].sum(), "display": format_currency(df["Annual Cost I"].sum())},
        "Good Spend": {"raw": df["Annual Cost II"].sum(), "display": format_currency(df["Annual Cost II"].sum())},
        "Top Instance": {"instance": top_inst, "percentage": top_pct},
        "Next 10": next_10_pct,
        "Rest": rest_pct
    }

    return result
