"""
Insights service for fetching aggregated analytics data from MongoDB.

This service aggregates data from recommendation_analytics_new collection
by Service Provider, Client, or Portfolios for both CCA and EIA applications.
"""
from app.connections.mongodb import get_collection
from bson import ObjectId
from app.connections.pylogger import log_message
from app.connections.custom_exceptions import CustomAPIException
from app.utils.constants import LevelType, CollectionNames, AppName
from datetime import datetime, timedelta
from app.utils.common_utils import get_user_emailscope, extract_organization_from_email

def safe_float(val, default=0.0):
    """Safely convert value to float, returning default for invalid values."""
    if val in (None, "", "-"):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def calculate_savings_percent(savings, cost):
    """Calculate savings percentage: (savings / cost) * 100."""
    savings_val = safe_float(savings)
    cost_val = safe_float(cost)
    if cost_val > 0:
        return round((savings_val / cost_val) * 100, 2)
    return 0.0


async def get_latest_entries_for_portfolios(collection, app_name: str, portfolio_ids=None):
    """
    Get the latest entry for each portfolio_id based on recommendation_date.
    
    Args:
        collection: MongoDB collection
        app_name: CCA or EIA
        portfolio_ids: Optional list of portfolio_ids to filter
    
    Returns:
        List of latest documents for each portfolio_id
    """
    match_stage = {"app_name": app_name.upper()}
    if portfolio_ids:
        match_stage["portfolio_id"] = {"$in": portfolio_ids}
    
    pipeline = [
        {"$match": match_stage},
        {"$sort": {"portfolio_id": 1, "recommendation_date": -1}},
        {"$group": {
            "_id": "$portfolio_id",
            "latest_doc": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$latest_doc"}}
    ]
    
    cursor = collection.aggregate(pipeline)
    docs = await cursor.to_list(length=None)

    # Filter by existence in portfolios collection and get names
    # This ensures we only process data for portfolios that still exist
    if not docs:
        return []

    # Extract all unique portfolio_ids from the fetched docs
    fetched_portfolio_ids = list(set([d["portfolio_id"] for d in docs if "portfolio_id" in d]))
    
    if not fetched_portfolio_ids:
        return []

    portfolios_collection = get_collection(CollectionNames.PORTFOLIOS)
    valid_ids = set()
    portfolio_names = {}
    
    candidate_obj_ids = []
    for pid in fetched_portfolio_ids:
        try:
            candidate_obj_ids.append(ObjectId(pid))
        except:
            continue
    
    if candidate_obj_ids:
        found_portfolios = await portfolios_collection.find(
            {"_id": {"$in": candidate_obj_ids}}, 
            {"name": 1}
        ).to_list(None)
        
        for fp in found_portfolios:
            str_id = str(fp["_id"])
            valid_ids.add(str_id)
            portfolio_names[str_id] = fp.get("name")

    # Filter docs to keep only those with valid portfolio_ids
    valid_docs = []
    for doc in docs:
        pid = doc.get("portfolio_id")
        if pid in valid_ids:
            # Attach portfolio name for downstream use
            if pid in portfolio_names:
                doc["portfolio_name"] = portfolio_names[pid]
            valid_docs.append(doc)
            
    return valid_docs


def format_cca_response(group_key: str, group_value: str, docs: list) -> dict:
    """
    Format CCA response for a group (service provider, client, or portfolio).
    Sums values across all documents and calculates savings percentages.
    """
    # Initialize sums
    current_cost = 0.0
    current_instances = 0
    current_vcpu = 0.0
    
    hc_cost = 0.0
    hc_vcpu = 0.0
    hc_perf_sum = 0.0
    hc_perf_count = 0
    hc_saving = 0.0
    
    m_cost = 0.0
    m_vcpu = 0.0
    m_perf_sum = 0.0
    m_perf_count = 0
    m_saving = 0.0
    
    md_cost = 0.0
    md_vcpu = 0.0
    md_perf_sum = 0.0
    md_perf_count = 0
    md_saving = 0.0
    
    for doc in docs:
        current_cost += safe_float(doc.get("current_cost"))
        current_instances += int(safe_float(doc.get("recommendation_instances_count")))
        current_vcpu += safe_float(doc.get("current_vCPUs"))
        
        hc_cost += safe_float(doc.get("HC_cost"))
        hc_vcpu += safe_float(doc.get("HC_vCPUs"))
        hc_perf = safe_float(doc.get("HC_perf"))
        if hc_perf > 0:
            hc_perf_sum += hc_perf
            hc_perf_count += 1
        hc_saving += safe_float(doc.get("HC_saving"))
        
        m_cost += safe_float(doc.get("M_cost"))
        m_vcpu += safe_float(doc.get("M_vCPUs"))
        m_perf = safe_float(doc.get("M_perf"))
        if m_perf > 0:
            m_perf_sum += m_perf
            m_perf_count += 1
        m_saving += safe_float(doc.get("M_saving"))
        
        md_cost += safe_float(doc.get("M_D_cost"))
        md_vcpu += safe_float(doc.get("M_D_vCPUs"))
        md_perf = safe_float(doc.get("M_D_perf"))
        if md_perf > 0:
            md_perf_sum += md_perf
            md_perf_count += 1
        md_saving += safe_float(doc.get("M_D_saving"))
    
    # Calculate averages for performance
    hc_perf_avg = round(hc_perf_sum / hc_perf_count, 2) if hc_perf_count > 0 else 0.0
    m_perf_avg = round(m_perf_sum / m_perf_count, 2) if m_perf_count > 0 else 0.0
    md_perf_avg = round(md_perf_sum / md_perf_count, 2) if md_perf_count > 0 else 0.0
    
    # Calculate savings percentages: savings / current_cost * 100
    hc_savings_percent = calculate_savings_percent(hc_saving, current_cost)
    m_savings_percent = calculate_savings_percent(m_saving, current_cost)
    md_savings_percent = calculate_savings_percent(md_saving, current_cost)
    
    response = {
        group_key: group_value,
        "current": {
            "current_cost_value": round(current_cost, 2),
            "current_instances": current_instances,
            "current_vcpu": round(current_vcpu, 2)
        },
        "hourly_cost_optimization": {
            "hourly_cost_optimization_cost": round(hc_cost, 2),
            "hourly_cost_optimization_vcpu": round(hc_vcpu, 2),
            "hourly_cost_optimization_performance": hc_perf_avg,
            "hourly_cost_optimization_savings": round(hc_saving, 2),
            "hourly_cost_optimization_savings_percent": hc_savings_percent
        },
        "modernize": {
            "modernize_cost": round(m_cost, 2),
            "modernize_vcpu": round(m_vcpu, 2),
            "modernize_performance": m_perf_avg,
            "modernize_savings": round(m_saving, 2),
            "modernize_savings_percent": m_savings_percent
        },
        "modernize_downsize": {
            "modernize_downsize_cost": round(md_cost, 2),
            "modernize_downsize_vcpu": round(md_vcpu, 2),
            "modernize_downsize_performance": md_perf_avg,
            "modernize_downsize_savings": round(md_saving, 2),
            "modernize_downsize_savings_percent": md_savings_percent
        }
    }

    client_map = {}
    for doc in docs:
        org = doc.get("org")
        if not org:
            continue
            
        if org not in client_map:
            client_map[org] = {
                "client": org,
                "current": 0.0,
                "hourly_cost_optimization": 0.0,
                "modernize": 0.0,
                "modernize_downsize": 0.0,
                "hc_perf_sum": 0.0, "hc_perf_count": 0,
                "m_perf_sum": 0.0, "m_perf_count": 0,
                "md_perf_sum": 0.0, "md_perf_count": 0,
            }
        
        # vCPUs
        client_map[org]["current"] += safe_float(doc.get("current_vCPUs"))
        client_map[org]["hourly_cost_optimization"] += safe_float(doc.get("HC_vCPUs"))
        client_map[org]["modernize"] += safe_float(doc.get("M_vCPUs"))
        client_map[org]["modernize_downsize"] += safe_float(doc.get("M_D_vCPUs"))

        # Performance
        hc_p = safe_float(doc.get("HC_perf"))
        if hc_p > 0:
            client_map[org]["hc_perf_sum"] += hc_p
            client_map[org]["hc_perf_count"] += 1
            
        m_p = safe_float(doc.get("M_perf"))
        if m_p > 0:
            client_map[org]["m_perf_sum"] += m_p
            client_map[org]["m_perf_count"] += 1
            
        md_p = safe_float(doc.get("M_D_perf"))
        if md_p > 0:
            client_map[org]["md_perf_sum"] += md_p
            client_map[org]["md_perf_count"] += 1
        
    # Round values and calculate averages
    client_analysis = []
    for org, data in client_map.items():
        # Calculate Averages
        hc_avg = round(data["hc_perf_sum"] / data["hc_perf_count"], 2) if data["hc_perf_count"] > 0 else 0.0
        m_avg = round(data["m_perf_sum"] / data["m_perf_count"], 2) if data["m_perf_count"] > 0 else 0.0
        md_avg = round(data["md_perf_sum"] / data["md_perf_count"], 2) if data["md_perf_count"] > 0 else 0.0

        client_analysis.append({
            "client": org,
            "current": round(data["current"], 2),
            "hourly_cost_optimization": round(data["hourly_cost_optimization"], 2),
            "modernize": round(data["modernize"], 2),
            "modernize_downsize": round(data["modernize_downsize"], 2),
            "hourly_cost_optimization_performance": hc_avg,
            "modernize_performance": m_avg,
            "modernize_downsize_performance": md_avg
        })
        
    response["client_wise_vcpu_analysis"] = client_analysis

    return response


def format_eia_response(group_key: str, group_value: str, docs: list) -> dict:
    """
    Format EIA response for a group (service provider, client, or portfolio).
    Sums values across all documents and calculates savings percentages.
    """
    # Initialize sums
    current_cost = 0.0
    current_instances = 0
    current_vcpu = 0.0
    
    o_cost = 0.0
    o_vcpu = 0.0
    o_perf_sum = 0.0
    o_perf_count = 0
    o_saving = 0.0
    o_energy = 0.0
    o_carbon = 0.0
    
    g_cost = 0.0
    g_vcpu = 0.0
    g_perf_sum = 0.0
    g_perf_count = 0
    g_saving = 0.0
    g_energy = 0.0
    g_carbon = 0.0
    
    for doc in docs:
        current_cost += safe_float(doc.get("current_cost"))
        current_instances += int(safe_float(doc.get("recommendation_instances_count")))
        current_vcpu += safe_float(doc.get("current_vCPUs"))
        
        o_cost += safe_float(doc.get("O_cost"))
        o_vcpu += safe_float(doc.get("O_vCPUs"))
        o_perf = safe_float(doc.get("O_perf"))
        if o_perf > 0:
            o_perf_sum += o_perf
            o_perf_count += 1
        o_saving += safe_float(doc.get("O_saving"))
        o_energy += safe_float(doc.get("O_energycount"))
        o_carbon += safe_float(doc.get("O_carboncount"))
        
        g_cost += safe_float(doc.get("G_cost"))
        g_vcpu += safe_float(doc.get("G_vCPUs"))
        g_perf = safe_float(doc.get("G_perf"))
        if g_perf > 0:
            g_perf_sum += g_perf
            g_perf_count += 1
        g_saving += safe_float(doc.get("G_saving"))
        g_energy += safe_float(doc.get("G_energycount"))
        g_carbon += safe_float(doc.get("G_carboncount"))
    
    # Calculate averages for performance
    o_perf_avg = round(o_perf_sum / o_perf_count, 2) if o_perf_count > 0 else 0.0
    g_perf_avg = round(g_perf_sum / g_perf_count, 2) if g_perf_count > 0 else 0.0
    
    # Calculate savings percentages: savings / current_cost * 100
    o_savings_percent = calculate_savings_percent(o_saving, current_cost)
    g_savings_percent = calculate_savings_percent(g_saving, current_cost)
    
    # Current power/carbon is average of optimal and good
    current_power = round((o_energy + g_energy) / 2, 2) if (o_energy + g_energy) > 0 else 0.0
    current_carbon = round((o_carbon + g_carbon) / 2, 2) if (o_carbon + g_carbon) > 0 else 0.0

    response = {
        group_key: group_value,
        "current": {
            "current_cost_value": round(current_cost, 2),
            "current_instances": current_instances,
            "current_vcpu": round(current_vcpu, 2),
            "current_power_consumption": current_power,
            "current_carbon_emission": current_carbon
        },
        "optimal": {
            "optimal_cost": round(o_cost, 2),
            "optimal_vcpu": round(o_vcpu, 2),
            "optimal_performance": o_perf_avg,
            "optimal_savings": round(o_saving, 2),
            "optimal_percent": o_savings_percent,
            "optimal_power_consumption": round(o_energy, 2),
            "optimal_carbon_emission": round(o_carbon, 2)
        },
        "good": {
            "good_cost": round(g_cost, 2),
            "good_vcpu": round(g_vcpu, 2),
            "good_performance": g_perf_avg,
            "good_savings": round(g_saving, 2),
            "good_savings_percent": g_savings_percent,
            "good_power_consumption": round(g_energy, 2),
            "good_carbon_emission": round(g_carbon, 2)
        }
    }

    client_map = {}
    for doc in docs:
        org = doc.get("org")
        if not org:
            continue
            
        if org not in client_map:
            client_map[org] = {
                "client": org,
                "current": 0.0,
                "optimal": 0.0,
                "good": 0.0,
                "o_perf_sum": 0.0, "o_perf_count": 0,
                "g_perf_sum": 0.0, "g_perf_count": 0
            }
        
        # vCPUs
        client_map[org]["current"] += safe_float(doc.get("current_vCPUs"))
        client_map[org]["optimal"] += safe_float(doc.get("O_vCPUs"))
        client_map[org]["good"] += safe_float(doc.get("G_vCPUs"))
        
        # Performance
        o_p = safe_float(doc.get("O_perf"))
        if o_p > 0:
            client_map[org]["o_perf_sum"] += o_p
            client_map[org]["o_perf_count"] += 1
            
        g_p = safe_float(doc.get("G_perf"))
        if g_p > 0:
            client_map[org]["g_perf_sum"] += g_p
            client_map[org]["g_perf_count"] += 1
    
    # Round values and calculate averages
    client_analysis = []
    for org, data in client_map.items():
        # Calculate Averages
        o_avg = round(data["o_perf_sum"] / data["o_perf_count"], 2) if data["o_perf_count"] > 0 else 0.0
        g_avg = round(data["g_perf_sum"] / data["g_perf_count"], 2) if data["g_perf_count"] > 0 else 0.0
        
        client_analysis.append({
            "client": org,
            "current": round(data["current"], 2),
            "optimal": round(data["optimal"], 2),
            "good": round(data["good"], 2),
            "optimal_performance": o_avg,
            "good_performance": g_avg
        })
        
    response["client_wise_vcpu_analysis"] = client_analysis

    return response


async def aggregate_by_service_provider(app_name: str, user_orgs: list = None, user_email: str = None, token: str = None) -> list:
    """Aggregate insights data grouped by cloud service provider."""
    collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
    
    # Get portfolio IDs filtered by user organizations and/or user_email if provided
    portfolio_ids = None
    if user_orgs or user_email:
        # Build match stage with $or condition when both user_orgs and user_email are provided
        match_stage = {"app_name": app_name.upper()}
        
        if user_orgs and user_email:
            # Filter by org IN user_orgs OR user_email matches
            match_stage["$or"] = [
                {"org": {"$in": user_orgs}},
                {"user_email": user_email}
            ]
            log_message(
                LevelType.INFO,
                f"Service Provider: Filtering by user_orgs ({len(user_orgs)} orgs) OR user_email ({user_email})"
            )
        elif user_orgs:
            # Only filter by org
            match_stage["org"] = {"$in": user_orgs}
            log_message(
                LevelType.INFO,
                f"Service Provider: Filtering by user_orgs only ({len(user_orgs)} organizations)"
            )
        elif user_email:
            # Only filter by user_email
            user_email_list = get_user_emailscope(token, user_email, app_name.upper())
            match_stage["user_email"] = {"$in": user_email_list}
            log_message(
                LevelType.INFO,
                f"Service Provider: Filtering by user_email only ({user_email})"
            )
        
        try:
            cursor = collection.find(match_stage, {"portfolio_id": 1})
            portfolio_docs = await cursor.to_list(length=None)
            portfolio_ids = list(set([doc["portfolio_id"] for doc in portfolio_docs if "portfolio_id" in doc]))
            
            if not portfolio_ids:
                log_message(
                    LevelType.WARNING,
                    f"Service Provider: No portfolio IDs found for app={app_name}, user_orgs={user_orgs}, user_email={user_email}"
                )
            else:
                log_message(
                    LevelType.INFO,
                    f"Service Provider: Found {len(portfolio_ids)} unique portfolio IDs"
                )
        except Exception as err:
            log_message(
                LevelType.ERROR,
                f"Service Provider: Error fetching portfolio IDs - {str(err)}"
            )
            raise
    
    docs = await get_latest_entries_for_portfolios(collection, app_name, portfolio_ids)
    
    # Group documents by cloud provider
    grouped = {}
    for doc in docs:
        cloud = (doc.get("cloud") or "").upper()
        if cloud:
            if cloud not in grouped:
                grouped[cloud] = []
            grouped[cloud].append(doc)
    
    result = []
    format_func = format_cca_response if app_name.upper() == "CCA" else format_eia_response
    
    for cloud, cloud_docs in grouped.items():
        result.append(format_func("provider", cloud, cloud_docs))
    
    return result


async def aggregate_by_client(app_name: str, user_orgs: list = None) -> list:
    """Aggregate insights data grouped by client (organization)."""
    collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
    
    # Get portfolio IDs filtered by user organizations if provided
    portfolio_ids = None
    if user_orgs:
        # Fetch portfolios that belong to user's organizations
        match_stage = {"app_name": app_name.upper(), "org": {"$in": user_orgs}}
        cursor = collection.find(match_stage, {"portfolio_id": 1})
        portfolio_docs = await cursor.to_list(length=None)
        portfolio_ids = list(set([doc["portfolio_id"] for doc in portfolio_docs if "portfolio_id" in doc]))
    
    docs = await get_latest_entries_for_portfolios(collection, app_name, portfolio_ids)
    
    # Group documents by organization
    grouped = {}
    for doc in docs:
        org = (doc.get("org") or "").upper()
        if org:
            if org not in grouped:
                grouped[org] = []
            grouped[org].append(doc)
    
    result = []
    format_func = format_cca_response if app_name.upper() == "CCA" else format_eia_response
    
    for org, org_docs in grouped.items():
        result.append(format_func("client", org, org_docs))
    
    return result


async def aggregate_by_portfolio(app_name: str, user_orgs: list = None) -> list:
    """Aggregate insights data grouped by portfolio_id."""
    collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
    
    # Get portfolio IDs filtered by user organizations if provided
    portfolio_ids = None
    if user_orgs:
        # Fetch portfolios that belong to user's organizations
        match_stage = {"app_name": app_name.upper(), "org": {"$in": user_orgs}}
        cursor = collection.find(match_stage, {"portfolio_id": 1})
        portfolio_docs = await cursor.to_list(length=None)
        portfolio_ids = list(set([doc["portfolio_id"] for doc in portfolio_docs if "portfolio_id" in doc]))
    
    docs = await get_latest_entries_for_portfolios(collection, app_name, portfolio_ids)
    
    # Get portfolio names from portfolios collection
    # Optimization: Names are now attached in get_latest_entries_for_portfolios
    
    result = []
    format_func = format_cca_response if app_name.upper() == "CCA" else format_eia_response
    
    for doc in docs:
        portfolio_id = doc.get("portfolio_id", "")
        # Use attached portfolio_name if available, else fallback to ID
        portfolio_name = doc.get("portfolio_name", portfolio_id)
        
        item = format_func("portfolio_name", portfolio_name, [doc])
        item["portfolio_id"] = portfolio_id
        result.append(item)
    
    return result


async def get_insights_data_service(insight: str, clients: list, app_name: str, user_email: str = None, token: str = None):
    """
    Service to fetch insights data from MongoDB.
    
    Args:
        insight: Type of insight - "Service Provider", "Client", or "Portfolios"
        clients: List of filters to apply based on insight type:
            - For "Service Provider": cloud providers (AWS, GCP, AZURE) or ["all"]
            - For "Client": organization names or ["all"]
            - For "Portfolios": portfolio IDs or ["all"]
        app_name: Application name - "CCA" or "EIA"
        user_email: Optional user email to filter data by user's organizations from sales_client
    
    Returns:
        Formatted insights response with aggregated data
    """
    try:
        # Validate app_name
        valid_apps = ["CCA", "EIA"]
        if app_name.upper() not in valid_apps:
            log_message(
                LevelType.ERROR,
                f"Invalid app_name provided: {app_name}. Valid values are: {valid_apps}"
            )
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid app_name '{app_name}'. Valid values are: {', '.join(valid_apps)}",
                error_code=-1
            )
        
        # Validate insight type
        valid_insights = ["Service Provider", "Client", "Portfolios"]
        if insight not in valid_insights:
            log_message(
                LevelType.ERROR,
                f"Invalid insight type provided: {insight}. Valid values are: {valid_insights}"
            )
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid insight type '{insight}'. Valid values are: {', '.join(valid_insights)}",
                error_code=-1
            )
            
        # Validate required parameters for insight type
        if clients is None:
            error_msg = ""
            if insight == "Service Provider":
                error_msg = "'providers' parameter is required for Service Provider insight"
            elif insight == "Client":
                error_msg = "'clients' parameter is required for Client insight"
            elif insight == "Portfolios":
                error_msg = "'portfolio_ids' parameter is required for Portfolios insight"
            
            if error_msg:
                log_message(LevelType.ERROR, f"Insights Validation Failed: {error_msg}")
                raise CustomAPIException(status_code=422, message=error_msg, error_code=-1)
            
        
        # Normalize app_name to uppercase
        app_name = app_name.upper()
        # If user_email is provided, fetch user's organizations from sales_client
        user_orgs = None
        if user_email:
            log_message(LevelType.INFO, f"Fetching organizations for user_email: {user_email}")
            user_orgs = await get_user_orgs(user_email)
            
            if user_orgs:
                log_message(LevelType.INFO, f"User {user_email} has access to {len(user_orgs)} organizations: {user_orgs}")
            else:
                log_message(LevelType.WARNING, f"User {user_email} has no valid organization mappings or not found in sales_client")
        
        # Fetch data based on insight type, passing user_orgs for filtering
        if insight == "Service Provider":
            # Service Provider uses BOTH user_orgs (from sales_client) AND user_email (direct field)
            data = await aggregate_by_service_provider(app_name, user_orgs, user_email, token)
            filter_key = "provider"
            log_message(
                LevelType.INFO,
                f"Service Provider insight: Using org-based filtering ({len(user_orgs) if user_orgs else 0} orgs) AND user_email filtering ({user_email or 'None'})"
            )
        elif insight == "Client":
            # Client uses ONLY user_orgs filtering (NOT user_email direct field)
            data = await aggregate_by_client(app_name, user_orgs)
            filter_key = "client"
            log_message(
                LevelType.INFO,
                f"Client insight: Using org-based filtering ONLY ({len(user_orgs) if user_orgs else 0} orgs), NOT using user_email direct filtering"
            )
        elif insight == "Portfolios":
            # Portfolios uses ONLY user_orgs filtering (NOT user_email direct field)
            data = await aggregate_by_portfolio(app_name, user_orgs)
            filter_key = "portfolio_id"
            log_message(
                LevelType.INFO,
                f"Portfolios insight: Using org-based filtering ONLY ({len(user_orgs) if user_orgs else 0} orgs), NOT using user_email direct filtering"
            )
        else:
            data = []
            filter_key = None
        
        # Apply client parameter filtering if specified and not "all"
        if clients and clients[0].lower() != "all" and filter_key:
            filtered_data = []
            # Normalize filter values to lowercase for case-insensitive comparison
            filter_values_lower = [c.lower() for c in clients]
            
            log_message(
                LevelType.INFO,
                f"Applying client filter for {filter_key}: {clients}"
            )
            
            for item in data:
                item_value = item.get(filter_key, "")
                if filter_key == "portfolio_id":
                    name_value = item.get("portfolio_name", "")
                    
                    if item_value.lower() in filter_values_lower or \
                       name_value.lower() in filter_values_lower:
                        filtered_data.append(item)
                else:
                    # For Service Provider and Client, do case-insensitive match
                    if item_value.lower() in filter_values_lower:
                        filtered_data.append(item)
            
            log_message(
                LevelType.INFO,
                f"Filter results: {len(data)} items before filter, {len(filtered_data)} items after filter"
            )
            
            data = filtered_data
        
        log_message(
            LevelType.INFO,
            f"Insights query completed: insight={insight}, app_name={app_name}, returned {len(data)} items"
        )
        
        response_message = "Intellect data fetched successfully"
        if not data:
            response_message = "No Intellect data found due to recommendations not available"

        return {
            "message": response_message,
            "errorCode": 1,
            "data": data
        }
    except CustomAPIException:
        # Re-raise CustomAPIException to preserve status code and message
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"Intellect Fetch Failed: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(
            status_code=500, 
            message="Intellect Fetch Failed", 
            error_code=-1
        )


async def get_user_orgs(user_email: str):
    """Fetch organizations for a user from sales_client."""
    if not user_email:
        return []
    
    try:
        sales_client = get_collection(CollectionNames.SALES_CLIENT)
        docs = await sales_client.find({"user_email": user_email}, {"client_name": 1}).to_list(None)
        orgs = [d.get("client_name") for d in docs if d.get("client_name")]
        return list(set(orgs))
    except Exception as e:
        log_message(LevelType.ERROR, f"Error fetching user orgs: {str(e)}")
        return []

async def get_dashboard_analytics_service(app_name: str, user_email: str):
    """
    Get dashboard analytics data.
    """
    try:
        app_name = app_name.upper()
        if app_name not in [AppName.CCA, AppName.EIA]:
            raise CustomAPIException(400, "Invalid App Name", -1)

        # 1. Get User Orgs
        log_message(LevelType.INFO, f"Fetching user orgs for user_email: {user_email}")
        user_orgs = await get_user_orgs(user_email)
        
        # 2. Build Match Stage
        match_stage = {"app_name": app_name}
        if user_orgs or user_email:
            or_conditions = []
            if user_orgs:
                log_message(LevelType.INFO, f"entering user orgs: {user_orgs}")
                or_conditions.append({"org": {"$in": user_orgs}})
            if user_email:
                log_message(LevelType.INFO, f"entering user email: {user_email}")
                or_conditions.append({"user_email": user_email})
            
            if or_conditions:
                log_message(LevelType.INFO, f"entering or conditions: {or_conditions}")
                match_stage["$or"] = or_conditions

        # 3. Fetch Data (Latest entries)
        collection = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)
        portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)

        log_message(LevelType.INFO, f"entering match stage: {match_stage}")
        cursor = collection.find(match_stage, {"portfolio_id": 1})
        p_docs = await cursor.to_list(None)
        portfolio_ids = list(set([d["portfolio_id"] for d in p_docs if "portfolio_id" in d]))
        
        if not portfolio_ids:
            return {
                "message": "Dashboard data fetched successfully",
                "errorCode": 1,
                "current_spend_vs_savings": {},
                "top_vcpus_performance": {"summary": "No data available", "vcpus_data": []},
                "power_and_carbon": {} if app_name == AppName.EIA else None
            }

        docs = await get_latest_entries_for_portfolios(collection, app_name, portfolio_ids)

        # 4. Aggregate Data
        if app_name == AppName.CCA:
            return await _aggregate_cca_dashboard(docs, portfolio_collection, user_email)
        else:
            return await _aggregate_eia_dashboard(docs, portfolio_collection, user_email)

    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Dashboard Analytics Failed: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(500, "Dashboard Analytics Failed", -1)

async def _aggregate_cca_dashboard(docs, portfolio_collection, user_email):
    # Initialize totals
    total_current_cost = 0.0
    total_hc_cost = 0.0
    total_m_cost = 0.0
    total_md_cost = 0.0
    
    # Details aggregators
    current_instances = 0
    current_vcpu = 0.0
    
    hc_cost_val = 0.0
    hc_savings = 0.0
    hc_vcpu = 0.0
    hc_perf_sum = 0.0
    hc_perf_count = 0
    
    m_cost_val = 0.0
    m_savings = 0.0
    m_vcpu = 0.0
    m_perf_sum = 0.0
    m_perf_count = 0
    
    md_cost_val = 0.0
    md_savings = 0.0
    md_vcpu = 0.0
    md_perf_sum = 0.0
    md_perf_count = 0

    # For Top vCPUs
    client_map = {}

    for doc in docs:
        # Extract values
        c_cost = safe_float(doc.get("current_cost"))
        hc_c = safe_float(doc.get("HC_cost"))
        m_c = safe_float(doc.get("M_cost"))
        md_c = safe_float(doc.get("M_D_cost"))
        
        total_current_cost += c_cost
        total_hc_cost += hc_c
        total_m_cost += m_c
        total_md_cost += md_c
        
        # Details
        current_instances += int(safe_float(doc.get("recommendation_instances_count")))
        current_vcpu += safe_float(doc.get("current_vCPUs"))
        
        hc_cost_val += hc_c
        hc_savings += safe_float(doc.get("HC_saving"))
        hc_vcpu += safe_float(doc.get("HC_vCPUs"))
        if safe_float(doc.get("HC_perf")) > 0:
            hc_perf_sum += safe_float(doc.get("HC_perf"))
            hc_perf_count += 1
            
        m_cost_val += m_c
        m_savings += safe_float(doc.get("M_saving"))
        m_vcpu += safe_float(doc.get("M_vCPUs"))
        if safe_float(doc.get("M_perf")) > 0:
            m_perf_sum += safe_float(doc.get("M_perf"))
            m_perf_count += 1
            
        md_cost_val += md_c
        md_savings += safe_float(doc.get("M_D_saving"))
        md_vcpu += safe_float(doc.get("M_D_vCPUs"))
        if safe_float(doc.get("M_D_perf")) > 0:
            md_perf_sum += safe_float(doc.get("M_D_perf"))
            md_perf_count += 1

        # Group by Client for Top vCPUs
        portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(doc.get("portfolio_id"))})
        org = portfolio_doc.get("created_for", None)
        if not org:
            org = extract_organization_from_email(doc.get("user_email", user_email))
        if org not in client_map:
            client_map[org] = {
                "client": org,
                "current": 0.0,
                "hourly_cost_optimization": 0.0,
                "modernize": 0.0,
                "modernize_downsize": 0.0,
                "performance_times": 0.0, # This seems to be avg perf? or total? Mock has small numbers like 5.3. Likely avg perf.
                "perf_sum": 0.0,
                "perf_count": 0,
                "portfolio_details": [],
                "current_cost": 0.0,
                "md_savings": 0.0
            }
        
        c_vcpu = safe_float(doc.get("current_vCPUs"))
        hc_vcpu_doc = safe_float(doc.get("HC_vCPUs"))
        m_vcpu_doc = safe_float(doc.get("M_vCPUs"))
        md_vcpu_doc = safe_float(doc.get("M_D_vCPUs"))
        hc_perf = safe_float(doc.get("HC_perf"))
        m_perf = safe_float(doc.get("M_perf"))
        md_perf = safe_float(doc.get("M_D_perf"))

        perf = safe_float(doc.get("total_perf"))
        
        client_map[org]["current"] += c_vcpu
        client_map[org]["hourly_cost_optimization"] += hc_vcpu_doc
        client_map[org]["modernize"] += m_vcpu_doc
        client_map[org]["modernize_downsize"] += md_vcpu_doc
        client_map[org]["current_cost"] += c_cost
        client_map[org]["md_savings"] += safe_float(doc.get("M_D_saving"))
        
        if perf > 0:
            client_map[org]["perf_sum"] += perf
            client_map[org]["perf_count"] += 1
            
        # Portfolio Detail
        client_map[org]["portfolio_details"].append({
            "name": doc.get("portfolio_name", doc.get("portfolio_id")),
            "_id": doc.get("portfolio_id"),
            "current": c_vcpu,
            "hourly_cost_optimization": hc_vcpu_doc,
            "modernize": m_vcpu_doc,
            "modernize_downsize": md_vcpu_doc,
            "hourly_perf": hc_perf,
            "modernize_perf": m_perf,
            "modernize_downsize_perf": md_perf 
        })

    # Finalize Client Data
    vcpus_data = []
    for org, data in client_map.items():
        avg_perf = round(data["perf_sum"] / data["perf_count"], 1) if data["perf_count"] > 0 else 0.0
        data["performance_times"] = avg_perf
        del data["perf_sum"]
        del data["perf_count"]
        
        # Sort portfolio_details by sum of all vCPUs descending and take top 5
        data["portfolio_details"].sort(
            key=lambda p: p.get("current", 0) + p.get("hourly_cost_optimization", 0) + p.get("modernize", 0) + p.get("modernize_downsize", 0),
            reverse=True
        )
        data["portfolio_details"] = data["portfolio_details"][:5]
        data["summary"] = f"{org.title()}: Using {int(data['hourly_cost_optimization'])} current vCPUs delivers up to {calculate_savings_percent(data.get("md_savings"), data.get("current_cost"))}% cost savings and {data['performance_times']}x higher performance compared to {int(data['current'])} Intel vCPUs."
        
        vcpus_data.append(data)
    
    # Sort by sum of all vCPUs descending and take top 5 clients
    vcpus_data.sort(
        key=lambda x: x.get("current", 0) + x.get("hourly_cost_optimization", 0) + x.get("modernize", 0) + x.get("modernize_downsize", 0),
        reverse=True
    )
    vcpus_data = vcpus_data[:5]

    # Averages for Details
    hc_perf_avg = round(hc_perf_sum / hc_perf_count, 1) if hc_perf_count > 0 else 0.0
    m_perf_avg = round(m_perf_sum / m_perf_count, 1) if m_perf_count > 0 else 0.0
    md_perf_avg = round(md_perf_sum / md_perf_count, 1) if md_perf_count > 0 else 0.0

    current_perf_avg = 1.0 

    response = {
        "message": "Dashboard data fetched successfully",
        "errorCode": 1,
        "current_spend_vs_savings": {
            "all_clients": {
                "current": round(total_current_cost, 2),
                "hourly_cost_optimization": round(total_hc_cost, 2),
                "modernize": round(total_m_cost, 2),
                "modernize_downsize": round(total_md_cost, 2)
            },
            "details": {
                "all_clients_current": {
                    "no_of_instances": current_instances,
                    "current_cost_value": round(total_current_cost, 2),
                    "total_vcpu": round(current_vcpu, 2),
                    "average_performance": current_perf_avg
                },
                "hourly_cost_optimization": {
                    "cost_of_value": round(hc_cost_val, 2),
                    "cost_of_savings": round(hc_savings, 2),
                    "total_vcpu": round(hc_vcpu, 2),
                    "average_performance": hc_perf_avg,
                    "savings": calculate_savings_percent(hc_savings, total_current_cost)
                },
                "modernize": {
                    "cost_of_value": round(m_cost_val, 2),
                    "cost_of_savings": round(m_savings, 2),
                    "total_vcpu": round(m_vcpu, 2),
                    "average_performance": m_perf_avg,
                    "savings": calculate_savings_percent(m_savings, total_current_cost)
                },
                "modernize_downsize": {
                    "cost_of_value": round(md_cost_val, 2),
                    "cost_of_savings": round(md_savings, 2),
                    "total_vcpu": round(md_vcpu, 2),
                    "average_performance": md_perf_avg,
                    "savings": calculate_savings_percent(md_savings, total_current_cost)
                }
            }
        },
        "top_vcpus_performance": {
            "summary": "Performance summary placeholder",
            "vcpus_data": vcpus_data
        }
    }
    return response

async def _aggregate_eia_dashboard(docs, portfolio_collection, user_email):
    # Initialize totals
    total_current_cost = 0.0
    total_optimal_cost = 0.0
    total_good_cost = 0.0
    
    # Details
    current_instances = 0
    current_vcpu = 0.0
    
    opt_cost_val = 0.0
    opt_savings = 0.0
    opt_vcpu = 0.0
    opt_perf_sum = 0.0
    opt_perf_count = 0
    
    good_cost_val = 0.0
    good_savings = 0.0
    good_vcpu = 0.0
    good_perf_sum = 0.0
    good_perf_count = 0
    
    # Power & Carbon
    current_power = 0.0
    current_carbon = 0.0
    opt_power = 0.0
    opt_carbon = 0.0
    good_power = 0.0
    good_carbon = 0.0

    client_map = {}

    for doc in docs:
        c_cost = safe_float(doc.get("current_cost"))
        o_cost = safe_float(doc.get("O_cost"))
        g_cost = safe_float(doc.get("G_cost"))
        
        total_current_cost += c_cost
        total_optimal_cost += o_cost
        total_good_cost += g_cost
        
        current_instances += int(safe_float(doc.get("recommendation_instances_count")))
        current_vcpu += safe_float(doc.get("current_vCPUs"))
        
        opt_cost_val += o_cost
        opt_savings += safe_float(doc.get("O_saving"))
        opt_vcpu += safe_float(doc.get("O_vCPUs"))
        if safe_float(doc.get("O_perf")) > 0:
            opt_perf_sum += safe_float(doc.get("O_perf"))
            opt_perf_count += 1
            
        good_cost_val += g_cost
        good_savings += safe_float(doc.get("G_saving"))
        good_vcpu += safe_float(doc.get("G_vCPUs"))
        if safe_float(doc.get("G_perf")) > 0:
            good_perf_sum += safe_float(doc.get("G_perf"))
            good_perf_count += 1
            
        # Power/Carbon
        # Sample has O_carboncount, G_carboncount, O_energycount, G_energycount
        o_c = safe_float(doc.get("O_carboncount"))
        g_c = safe_float(doc.get("G_carboncount"))
        o_e = safe_float(doc.get("O_energycount"))
        g_e = safe_float(doc.get("G_energycount"))
        
        opt_carbon += o_c
        good_carbon += g_c
        opt_power += o_e
        good_power += g_e
        
        # Estimate current as average of O and G if not present? 
        # Insights service does: current_power = (o_energy + g_energy) / 2
        current_carbon += (o_c + g_c) / 2
        current_power += (o_e + g_e) / 2

        # Client Map
        portfolio_doc = await portfolio_collection.find_one({"_id": ObjectId(doc.get("portfolio_id"))})
        org = portfolio_doc.get("created_for", None)
        if not org:
            org = extract_organization_from_email(doc.get("user_email", user_email))
        if org not in client_map:
            client_map[org] = {
                "client": org,
                "current": 0.0,
                "optimal": 0.0,
                "good": 0.0,
                "performance_times": 0.0,
                "perf_sum": 0.0,
                "perf_count": 0,
                "portfolio_details": [],
                "optimal_savings" : 0.0,
                "current_cost": 0.0,
                "optimal_cost": 0.0,
            }
            
        c_vcpu = safe_float(doc.get("current_vCPUs"))
        o_vcpu_doc = safe_float(doc.get("O_vCPUs"))
        g_vcpu_doc = safe_float(doc.get("G_vCPUs"))
        o_perf_doc = safe_float(doc.get("O_perf"))
        g_perf_doc = safe_float(doc.get("G_perf"))

        perf = safe_float(doc.get("total_perf"))
        
        client_map[org]["current"] += c_vcpu
        client_map[org]["optimal"] += o_vcpu_doc
        client_map[org]["good"] += g_vcpu_doc
        
        client_map[org]["current_cost"] += c_cost
        client_map[org]["optimal_cost"] += o_cost
        client_map[org]["optimal_savings"] += safe_float(doc.get("O_saving"))
        
        if perf > 0:
            client_map[org]["perf_sum"] += perf
            client_map[org]["perf_count"] += 1
            
        client_map[org]["portfolio_details"].append({
            "name": doc.get("portfolio_name", doc.get("portfolio_id")),
            "_id": doc.get("portfolio_id"),
            "current": c_vcpu,
            "optimal": o_vcpu_doc,
            "good": g_vcpu_doc,
            "optimal_performance": o_perf_doc,
            "good_performance": g_perf_doc
        })

    # Finalize Client Data
    vcpus_data = []
    for org, data in client_map.items():
        avg_perf = round(data["perf_sum"] / data["perf_count"], 1) if data["perf_count"] > 0 else 0.0
        data["performance_times"] = avg_perf
        del data["perf_sum"]
        del data["perf_count"]
        
        # Sort portfolio_details by sum of all vCPUs descending and take top 5
        data["portfolio_details"].sort(
            key=lambda p: p.get("current", 0) + p.get("optimal", 0) + p.get("good", 0),
            reverse=True
        )
        data["portfolio_details"] = data["portfolio_details"][:5]
        data["summary"] = f"{org.title()}: Using {int(data['optimal'])} optimal vCPUs delivers up to {calculate_savings_percent(data.get("optimal_savings"), data.get("current_cost"))}% cost savings and {data['performance_times']}x higher performance compared to {int(data['current'])} Intel vCPUs."
        
        vcpus_data.append(data)

    # Sort by sum of all vCPUs descending and take top 5 clients
    vcpus_data.sort(
        key=lambda x: x.get("current", 0) + x.get("optimal", 0) + x.get("good", 0),
        reverse=True
    )
    vcpus_data = vcpus_data[:5]

    opt_perf_avg = round(opt_perf_sum / opt_perf_count, 1) if opt_perf_count > 0 else 0.0
    good_perf_avg = round(good_perf_sum / good_perf_count, 1) if good_perf_count > 0 else 0.0
    current_perf_avg = 1.0

    response = {
        "message": "Dashboard data fetched successfully",
        "errorCode": 1,
        "current_spend_vs_savings": {
            "all_clients": {
                "current": round(total_current_cost, 2),
                "optimal": round(total_optimal_cost, 2),
                "good": round(total_good_cost, 2)
            },
            "details": {
                "all_clients_current": {
                    "no_of_instances": current_instances,
                    "current_cost_value": round(total_current_cost, 2),
                    "total_vcpu": round(current_vcpu, 2),
                    "average_performance": current_perf_avg
                },
                "optimal": {
                    "cost_of_value": round(opt_cost_val, 2),
                    "cost_of_savings": round(opt_savings, 2),
                    "total_vcpu": round(opt_vcpu, 2),
                    "average_performance": opt_perf_avg,
                    "savings_percent": calculate_savings_percent(opt_savings, total_current_cost)
                },
                "good": {
                    "cost_of_value": round(good_cost_val, 2),
                    "cost_of_savings": round(good_savings, 2),
                    "total_vcpu": round(good_vcpu, 2),
                    "average_performance": good_perf_avg,
                    "savings_percent": calculate_savings_percent(good_savings, total_current_cost)
                }
            }
        },
        "top_vcpus_performance": {
            "summary": "Performance summary placeholder",
            "vcpus_data": vcpus_data
        },
        "power_and_carbon": {
            "current": {"power_kw": round(current_power, 2), "carbon_kgco2eq": round(current_carbon, 2)},
            "optimal": {"power_kw": round(opt_power, 2), "carbon_kgco2eq": round(opt_carbon, 2), "cost_of_value": round(opt_cost_val, 2), "cost_savings_percent": calculate_savings_percent(opt_savings, total_current_cost), "power_savings_percent": calculate_savings_percent(current_power-opt_power, current_power), "carbon_savings_percent": calculate_savings_percent(current_carbon - opt_carbon, current_carbon)},
            "good": {"power_kw": round(good_power, 2), "carbon_kgco2eq": round(good_carbon, 2), "cost_of_value": round(good_cost_val, 2), "cost_savings_percent": calculate_savings_percent(good_savings, total_current_cost), "power_savings_percent": calculate_savings_percent(current_power-good_power, current_power), "carbon_savings_percent": calculate_savings_percent(current_carbon - good_carbon, current_carbon)}
        }
    }
    return response

async def get_dashboard_summary_service(user_email: str, app_name: str):
    """
    Dashboard summary: portfolio count, instances count, client count, active clients
    """
    sales_client_collection = get_collection(CollectionNames.SALES_CLIENT)
    organization_data_collection = get_collection(CollectionNames.ORGANIZATION_DATA)
    org_user_summary_collection = get_collection(CollectionNames.ORG_USER_SUMMARY)
    portfolio_collection = get_collection(CollectionNames.PORTFOLIOS)


    # 1️⃣ Fetch all sales clients for this user
    client_docs = await sales_client_collection.find({"user_email": user_email}).to_list(None)

    total_clients = len(client_docs)
    total_portfolios = 0
    total_instances = 0
    # Safety: If no clients → return empty summary
    if total_clients == 0:
        cursor = portfolio_collection.find({"user_email": user_email, "app_name": app_name})
        docs = await cursor.to_list(length=None)
        msg = "No clients found for dashboard summary"
        if docs:
            current_instance_count = [
                doc.get("current_instances_count") if doc.get("current_instances_count") else 0
                for doc in docs
            ]
            total_instances += sum(current_instance_count)
            total_portfolios += len(docs)
            msg = "Dashboard summary fetched successfully"
        return {
            "Message": msg,
            "ErrorCode": 1,
            "total_portfolios": total_portfolios,
            "total_instances": total_instances,
            "total_clients": 0,
            "active_clients": 0
        }


    # 2️⃣ Join with organization_data_collection based on organization_id
    org = extract_organization_from_email(user_email)

    for client in client_docs:
        client_name = client.get("client_name") 

        if not client_name:
            continue

        org_doc = await organization_data_collection.find_one({"organization": client_name, "app_name": app_name})

        if not org_doc:
            continue

        portfolio_count = org_doc.get("portfolio_count", 0)
        instance_count = org_doc.get("current_instance_count", 0)

        total_portfolios += portfolio_count
        total_instances += instance_count

    

    # 3️⃣ Personal portfolios (exclude created_for matching a client to avoid double-count)
    # Count distinct organizations that have endpoint usage in the last 30 days
    client_names = [client.get("client_name").upper() for client in client_docs if client.get("client_name")]
    if org and org.upper() not in client_names:
        query = {"user_email": user_email, "app_name": app_name}
        org_pattern = "|".join(map(lambda x: x.strip(), client_names))
        if org_pattern:
            query["$or"] = [
                {"created_for": {"$exists": False}},
                {"created_for": {"$in": [None, ""]}},
                {"created_for": {"$not": {"$regex": org_pattern, "$options": "i"}}},
            ]
        cursor = portfolio_collection.find(query)
        docs = await cursor.to_list(length=None)
        current_instance_count = [
            doc.get("current_instances_count") if doc.get("current_instances_count") else 0
            for doc in docs
        ]
        total_instances += sum(current_instance_count)
        total_portfolios += len(docs)

    # Calculate cutoff date (30 days ago)
    cutoff_date = datetime.utcnow() - timedelta(days=30)
    
    # Use aggregation to get distinct organizations with endpoint activity in last 30 days
    log_message(LevelType.INFO, f"Fetching active clients count from org_endpoint_summary with 30-day filter client_names {client_names} cutoff_date {cutoff_date}")
    active_clients_pipeline = [
        {
            "$match": {
                "app_name": app_name,
                "organisation": {"$in": client_names}
            }
        },
        {
            # Convert string date -> actual Date
            "$addFields": {
                "updated_at_date": {
                    "$dateFromString": {
                        "dateString": "$updated_at",
                        "format": "%m/%d/%Y"
                    }
                }
            }
        },
        {
            "$match": {
                "updated_at_date": {"$gte": cutoff_date}
            }
        },
        {
            "$group": {
                "_id": "$organisation"
            }
        }
    ]
    active_clients_docs = await org_user_summary_collection.aggregate(active_clients_pipeline).to_list(None)
    active_clients = len(active_clients_docs)

    # 4️⃣ Final Response
    return {
        "Message": "Dashboard summary fetched successfully",
        "ErrorCode": 1,
        "total_portfolios": total_portfolios,
        "total_instances": total_instances,
        "total_clients": total_clients,
        "active_clients": active_clients
    }
