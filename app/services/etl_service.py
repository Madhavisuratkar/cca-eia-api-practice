import configparser
import logging
import os
from datetime import datetime, timedelta, date
from collections import defaultdict
import json
import requests
from app.connections.mongodb import get_collection
from app.utils.constants import (
    CollectionNames
)
from app.connections.env_config import SONAR_URL, JENKINS_URL
from app.connections.custom_exceptions import CustomAPIException
from typing import Optional
from collections import defaultdict
from dateutil import parser as date_parser
from app.utils.constants import REGEX_NAME, OPTIONS_NAME, INVALID_DAYS_MSG, APP_HEADER_MSG, DATE_FORMATTER, MATCH_NAME, SORT_NAME, GROUP_NAME, INACTIVE_USERS, FEATURES_MSG, PROJECT_NAME, IS_NULL


config = configparser.ConfigParser()
last_autosave_time = datetime.now()
base_path = os.path.dirname(os.path.abspath(__file__))
app_dir = os.path.dirname(base_path)
 

# -------------------------
# Main Function
# -------------------------
def to_timestamp_ms(d):
    if isinstance(d, str):
        d = datetime.strptime(d, '%Y-%m-%d')
    elif isinstance(d, date) and not isinstance(d, datetime):
        d = datetime.combine(d, datetime.min.time())
    return int(d.timestamp()) * 1000

# --- CCA Savings Helpers ---
def get_exclusion_conditions(app_name: str):
    ignore_file = os.path.join(os.getcwd(),'app','connections', "ignore.json")
    with open(ignore_file, "r") as file:
        exclusions = json.load(file)

    ignored_users = set(exclusions.get("ignored_users", []))
    ignored_organizations = set(exclusions.get("ignored_organizations", []))
    ignored_endpoints = set(exclusions.get("ignored_endpoints", []))

    # Special rule: if app_name is eia, remove explorer from ignored_endpoints
    if app_name.lower() == "eia":
        ignored_endpoints.discard("explorer")

    return ignored_users, ignored_organizations, ignored_endpoints


def extract_org_from_email(email: str) -> str:
    try:
        if "@" in email:
            domain = email.split("@")[1] 
            return domain.split(".")[0]
        else:
            return email
    except Exception:
        return "unknown"

def get_date_filter_condition(date_filter, app_name):
    if date_filter.lower() == "all":
        return {"app_name": app_name}, None
    try:
        days = int(date_filter) if date_filter.isdigit() else None
        if days is None:
            return INVALID_DAYS_MSG, 400
        start_date = datetime.utcnow() - timedelta(days=days)
        return {"app_name": app_name, "created_at": {"$gte": start_date}}, None
    except ValueError:
        return INVALID_DAYS_MSG, 400
    
def fetch_jenkins_data(job_name, tree, auth_header):
    try:
        url = f'{JENKINS_URL}/{job_name}/api/json'
        response = requests.get(url, headers={'Authorization': auth_header}, params={'tree': tree})

        if response.status_code != 200:
            return {'error': f'Error fetching data: {response.text}'}, response.status_code

        return {"Data": response.json()}
    except CustomAPIException:
        raise
    except Exception as e:
        return {'error': str(e)}, 500
    
def fetch_sonar_data(feature, component, metrics, from_date, to_date, token):
    params = {
        'component': component,
        'metrics': metrics,
        'from': from_date,
        'to': to_date
    }
    headers = {
        'Authorization': token
    }

    response = requests.get(SONAR_URL, params=params, headers=headers)

    if not response:
        return {"Message": f"{feature} data not found."}
    try:
        if response.content:
            data = response.json()
        else:
            data = None
    except CustomAPIException:
        raise
    except requests.exceptions.JSONDecodeError:
        data = None

    return {
        "Message":FEATURES_MSG if data else "No data available",
        "ErrorCode": 1 if data else 0,
        "Data": data
    }

async def get_metrics_data(app_name: str, date_filter: str):
    if not app_name:
        raise CustomAPIException(status_code=400, message=APP_HEADER_MSG, error_code=-1)

    cpu_count = await vcpu_count_new(app_name, date_filter)
    perf_count = await perf_count_new(app_name, date_filter)

    response = {
        "vCPU_count": cpu_count,
        "perf_count": perf_count
    }

    if app_name == 'EIA':
        carbon_count = await carboncount(app_name, date_filter)
        energy_count = await energycount(app_name, date_filter)
        response.update({"carbon_count": carbon_count, "energy_count": energy_count})

    if not any(response.values()):
        raise CustomAPIException(status_code=404, message="No Metrics data found", error_code=-1)

    return {"Message": "Metrics data fetched successfully", "Data": response, "ErrorCode": 1}


def get_new_mongo_date_filter(date_filter: str):
    """Return MongoDB date filter based on number of days or 'All'."""
    if not date_filter or date_filter.lower() == "all":
        return {}

    try:
        days = int(date_filter)
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return cutoff_date
    except ValueError:
        return None


def parse_date_safe_to_date(v):
    """Return a date object (YYYY-MM-DD) or None. Accepts datetime/date/str."""
    if not v:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return date_parser.parse(str(v)).date()
    except Exception:
        return None


def classify_user_activity(doc, cutoff_date, active_users_map, inactive_users_map, loggedin_count_by_date):
    org_raw = doc.get("organisation") or ""
    org = str(org_raw).strip().lower()
    uname = doc.get("user_name")
    if not org or org == "unknown" or not uname:
        return None, None, None

    updated_date = parse_date_safe_to_date(doc.get("updated_at"))
    if cutoff_date and updated_date:
        if updated_date >= cutoff_date:
            active_users_map[org].add(uname)
            loggedin_count_by_date[updated_date].add(uname)
            inactive_users_map[org].discard(uname)
        else:
            if uname not in active_users_map[org]:
                inactive_users_map[org].add(uname)
                month_key = updated_date.strftime("%Y-%b")
                return org, uname, month_key
            return org, uname, None
    else:
        active_users_map[org].add(uname)
        if updated_date:
            loggedin_count_by_date[updated_date].add(uname)
    return org, uname, None

def clean_inactive_users(active_users_map, inactive_users_map):
    for org, inactive_set in inactive_users_map.items():
        active_set = active_users_map.get(org, set())
        filtered = inactive_set - active_set
        if filtered:
            inactive_users_map[org] = filtered
        else:
            inactive_users_map.pop(org, None)

def build_series_data(name, data, orgs):
    return [
        {"name": name, "y": count, "drilldown": org}
        for org, count in zip(orgs, data)
        if count > 0
    ]

def build_drilldown_series(name, drilldown_dict, orgs):
    drilldown_series = []
    for org in orgs:
        users = sorted(drilldown_dict.get(org, set()))
        if users:
            drilldown_series.append({"name": org, "id": org, "data": [[u, 1] for u in users]})
    return drilldown_series

async def get_organisation_summary_data(app_name: str, date_filter: str):
    if not app_name:
        raise CustomAPIException(status_code=400, message=APP_HEADER_MSG, error_code=-1)

    org_summary_collection = get_collection(CollectionNames.ORG_USER_SUMMARY)
    cutoff_dt = get_new_mongo_date_filter(date_filter)
    if isinstance(cutoff_dt, datetime):
        cutoff_date = cutoff_dt.date()
    elif isinstance(cutoff_dt, date):
        cutoff_date = cutoff_dt
    else:
        cutoff_date = None

    cursor = org_summary_collection.find({"app_name": app_name.upper()}, {"_id": 0})
    all_users = await cursor.to_list(length=None)

    if not all_users:
        return {"Message": "No organisation data found", "ErrorCode": 1, "Data": {}}

    user_by_org = defaultdict(set)
    active_users_map = defaultdict(set)
    inactive_users_map = defaultdict(set)
    inactive_per_month_map = defaultdict(set)
    loggedin_count_by_date = defaultdict(set)

    for doc in all_users:
        org, uname, month_key = classify_user_activity(doc, cutoff_date, active_users_map, inactive_users_map, loggedin_count_by_date)
        if org is None or uname is None:
            continue
        user_by_org[org].add(uname)
        if month_key:
            inactive_per_month_map[org].add(month_key)

    clean_inactive_users(active_users_map, inactive_users_map)

    orgs = sorted(user_by_org.keys())

    active_counts = [len(active_users_map.get(org, set())) for org in orgs]
    inactive_counts = [len(inactive_users_map.get(org, set())) for org in orgs]

    active_series_data = build_series_data("Active Users", active_counts, orgs)
    active_drilldown_series = build_drilldown_series("Active Users", active_users_map, orgs)

    inactive_series_data = build_series_data(INACTIVE_USERS, inactive_counts, orgs)
    inactive_drilldown_series = build_drilldown_series(INACTIVE_USERS, inactive_users_map, orgs)

    user_by_org_data = {
        "xAxis": {"categories": orgs},
        "series": [{"data": [len(user_by_org[o]) for o in orgs]}],
    }

    loggedin_user_series = sorted([
        [int(datetime.combine(day, datetime.min.time()).timestamp() * 1000), len(users)]
        for day, users in loggedin_count_by_date.items()
    ])

    inactive_month_series = []
    inactive_month_drilldown = []
    for org in orgs:
        months = inactive_per_month_map.get(org, set())
        if not months:
            continue
        inactive_month_series.append({"name": org, "y": len(months), "drilldown": org})
        inactive_month_drilldown.append({
            "name": org,
            "id": org,
            "data": [[m, 1] for m in sorted(months)]
        })

    return {
        "Message": "Organisation data fetched successfully",
        "ErrorCode": 1,
        "Data": {
            "Active Users Data": {
                "series": [{"name": "Active Users", "data": active_series_data}],
                "drilldown": {"series": active_drilldown_series},
            },
            "Inactive Users Data": {
                "series": [{"name": INACTIVE_USERS, "data": inactive_series_data}],
                "drilldown": {"series": inactive_drilldown_series},
            },
            "User By Org Data": user_by_org_data,
            "Loggedin User Data": loggedin_user_series,
            "Inactive Per Month Data": {
                "series": [{"name": INACTIVE_USERS, "data": inactive_month_series}],
                "drilldown": {"series": inactive_month_drilldown},
            },
        },
    }

def filter_docs_by_date(docs, cutoff_date):
    filtered = []
    for d in docs:
        date_str = d.get("updated_at") or d.get("create_at")
        if not date_str:
            continue
        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
            if dt >= cutoff_date:
                filtered.append(d)
        except Exception:
            continue
    return filtered

async def get_features_endpoint_summary_count_data(app_name: str, date_filter: str):
    if not app_name:
        raise CustomAPIException(status_code=400, message=APP_HEADER_MSG, error_code=-1)

    cutoff_date = get_new_mongo_date_filter(date_filter)
    collection = get_collection(CollectionNames.ORG_ENDPOINT_SUMMARY)

    query_filter = {"app_name": app_name.upper()}
    docs = await collection.find(query_filter, {"_id": 0}).to_list(length=None)

    if not docs:
        return {
            "Message": "No data found for given filters",
            "ErrorCode": 1,
            "Data": {"series": [], "xAxis": {"categories": []}},
        }

    if cutoff_date:
        docs = filter_docs_by_date(docs, cutoff_date)

    if not docs:
        return {
            "Message": "No data found for given filters",
            "ErrorCode": 1,
            "Data": {"series": [], "xAxis": {"categories": []}},
        }

    orgs = sorted({d["organisation"].lower() for d in docs if d.get("organisation")})
    endpoints = sorted({d["endpoint"] for d in docs if d.get("endpoint")})

    org_index = {org: i for i, org in enumerate(orgs)}
    endpoint_data = {ep: [0] * len(orgs) for ep in endpoints}

    for doc in docs:
        org = doc.get("organisation", "").lower()
        endpoint = doc.get("endpoint")
        count = int(doc.get("count", 0))

        if org in org_index and endpoint in endpoint_data:
            idx = org_index[org]
            endpoint_data[endpoint][idx] += count

    series = [{"name": ep, "data": data} for ep, data in endpoint_data.items()]

    return {
        "Message": FEATURES_MSG,
        "ErrorCode": 1,
        "Data": {"series": series, "xAxis": {"categories": orgs}},
    }

    
def get_metrics_date_filter_condition(date_filter, app_name):
    if date_filter.lower() == "all":
        return {"app_name": app_name}, None
    try:
        days = int(date_filter) if date_filter.isdigit() else None
        if days is None:
            return INVALID_DAYS_MSG, 400
        start_date = datetime.utcnow() - timedelta(days=days)
        return {"app_name": app_name, "recommendation_date": {"$gte": start_date}}, None
    except ValueError:
        return INVALID_DAYS_MSG, 400

async def carboncount(app_name: str, date_filter: str):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Build match on app_name + recommendation_date window
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    pipeline = [
        {MATCH_NAME: base_filter},
        # Coalesce nulls to zero first
        {PROJECT_NAME: {
            "_id": 0,
            "org": 1,
            "O_carboncount": {IS_NULL: ["$O_carboncount", 0]},
            "G_carboncount": {IS_NULL: ["$G_carboncount", 0]},
        }},
        # Drop records where both values are zero
        {MATCH_NAME: {
            "$expr": {
                "$not": {
                    "$and": [
                        {"$eq": ["$O_carboncount", 0]},
                        {"$eq": ["$G_carboncount", 0]},
                    ]
                }
            }
        }},
        {GROUP_NAME: {
            "_id": "$org",
            "o_sum": {"$sum": "$O_carboncount"},
            "g_sum": {"$sum": "$G_carboncount"},
        }},
        {SORT_NAME: {"_id": 1}},
    ]

    docs = await coll.aggregate(pipeline).to_list(length=None)

    categories = [d["_id"] for d in docs]
    series_o = [float(d.get("o_sum", 0) or 0) for d in docs]
    series_g = [float(d.get("g_sum", 0) or 0) for d in docs]

    return {
        "xAxis": {"categories": categories},
        "series": [
            {"name": "Carbon I", "data": series_o},
            {"name": "Carbon II", "data": series_g},
        ],
    }

async def energycount(app_name: str, date_filter: str):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Build match on app_name + recommendation_date window
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    pipeline = [
        {MATCH_NAME: base_filter},
        # Normalize null/missing to 0 so comparisons are reliable
        {PROJECT_NAME: {
            "_id": 0,
            "org": 1,
            "O_energycount": {IS_NULL: ["$O_energycount", 0]},
            "G_energycount": {IS_NULL: ["$G_energycount", 0]},
        }},
        # Drop records where both values are zero
        {MATCH_NAME: {
            "$expr": {
                "$not": {
                    "$and": [
                        {"$eq": ["$O_energycount", 0]},
                        {"$eq": ["$G_energycount", 0]},
                    ]
                }
            }
        }},
        {GROUP_NAME: {
            "_id": "$org",
            "o_sum": {"$sum": "$O_energycount"},
            "g_sum": {"$sum": "$G_energycount"},
        }},
        {SORT_NAME: {"_id": 1}},
    ]

    docs = await coll.aggregate(pipeline).to_list(length=None)

    # Format to required shape
    categories = [d["_id"] for d in docs]
    series_o = [float(d.get("o_sum", 0) or 0) for d in docs]
    series_g = [float(d.get("g_sum", 0) or 0) for d in docs]

    return {
        "xAxis": {"categories": categories},
        "series": [
            {"name": "Energy I", "data": series_o},
            {"name": "Energy II", "data": series_g},
        ],
    }


async def vcpu_count_new(app_name: str, date_filter: str):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Build created_at window + app_name
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    # Project required numeric fields with null coalescing
    project_stage = {
        "_id": 0,
        "org": 1,
        "current_vCPUs": {IS_NULL: ["$current_vCPUs", 0]},
        "total_vCPUs": {IS_NULL: ["$total_vCPUs", 0]},
        "HC_vCPUs": {IS_NULL: ["$HC_vCPUs", 0]},
        "M_vCPUs": {IS_NULL: ["$M_vCPUs", 0]},
        "M_D_vCPUs": {IS_NULL: ["$M_D_vCPUs", 0]},
        "O_vCPUs": {IS_NULL: ["$O_vCPUs", 0]},
        "G_vCPUs": {IS_NULL: ["$G_vCPUs", 0]},
    }

    # After coalescing, drop records where all relevant vCPU fields are zero.
    # For EIA: consider current, total, O, G; for CCA: consider current, total, HC, M, M_D.
    eia = app_name.upper() == "EIA"
    all_zero_cond_eia = {
        "$and": [
            {"$eq": ["$current_vCPUs", 0]},
            {"$eq": ["$total_vCPUs", 0]},
            {"$eq": ["$O_vCPUs", 0]},
            {"$eq": ["$G_vCPUs", 0]},
        ]
    }
    all_zero_cond_cca = {
        "$and": [
            {"$eq": ["$current_vCPUs", 0]},
            {"$eq": ["$total_vCPUs", 0]},
            {"$eq": ["$HC_vCPUs", 0]},
            {"$eq": ["$M_vCPUs", 0]},
            {"$eq": ["$M_D_vCPUs", 0]},
        ]
    }

    drop_all_zero_stage = {
        MATCH_NAME: {
            "$expr": {"$not": (all_zero_cond_eia if eia else all_zero_cond_cca)}
        }
    }

    group_stage = {
        "_id": "$org",
        "current_sum": {"$sum": "$current_vCPUs"},
        "total_sum": {"$sum": "$total_vCPUs"},
        "count": {"$sum": 1},
        "hc_sum": {"$sum": "$HC_vCPUs"},
        "m_sum": {"$sum": "$M_vCPUs"},
        "md_sum": {"$sum": "$M_D_vCPUs"},
        "o_sum": {"$sum": "$O_vCPUs"},
        "g_sum": {"$sum": "$G_vCPUs"},
    }

    project_avg_stage = {
        "_id": 1,
        "current_sum": 1,
        "avg_vcpu": {
            "$cond": [{"$gt": ["$count", 0]}, {"$divide": ["$total_sum", "$count"]}, 0]
        },
        "hc_sum": 1, "m_sum": 1, "md_sum": 1, "o_sum": 1, "g_sum": 1,
    }

    pipeline = [
        {MATCH_NAME: base_filter},
        {PROJECT_NAME: project_stage},
        drop_all_zero_stage,              # drop all-zero records before grouping
        {GROUP_NAME: group_stage},
        {PROJECT_NAME: project_avg_stage},
        {SORT_NAME: {"_id": 1}},
    ]

    rows = await coll.aggregate(pipeline).to_list(length=None)

    categories = [r["_id"] for r in rows]
    current_series = [{"name": r["_id"], "y": float(r["current_sum"])} for r in rows]
    avg_series = []
    drill_series = []

    for r in rows:
        org = r["_id"]
        drill_id = f"{org}-average-vcpu".lower()
        avg_series.append({"name": org, "y": float(r["avg_vcpu"]), "drilldown": drill_id})
        if eia:
            drill_data = [
                {"name": "Optimal", "y": float(r.get("o_sum", 0) or 0)},
                {"name": "Good", "y": float(r.get("g_sum", 0) or 0)},
            ]
        else:
            drill_data = [
                {"name": "HC", "y": float(r.get("hc_sum", 0) or 0)},
                {"name": "M", "y": float(r.get("m_sum", 0) or 0)},
                {"name": "M&D", "y": float(r.get("md_sum", 0) or 0)},
            ]
        drill_series.append({"id": drill_id, "name": f"{org} Average vCPU", "data": drill_data})

    return {
        "xAxis": {"categories": categories},
        "series": [
            {"name": "Current vCPU", "data": current_series},
            {"name": "Average vCPU", "data": avg_series},
        ],
        "drilldown": {"series": drill_series},
    }


async def perf_count_new(app_name: str, date_filter: str):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    # Normalize perf fields, then drop docs where all are 0 for the app
    project_stage = {
        "_id": 0,
        "org": 1,
        "total_perf": {IS_NULL: ["$total_perf", 0]},
        "HC_perf": {IS_NULL: ["$HC_perf", 0]},
        "M_perf": {IS_NULL: ["$M_perf", 0]},
        "M_D_perf": {IS_NULL: ["$M_D_perf", 0]},
        "O_perf": {IS_NULL: ["$O_perf", 0]},
        "G_perf": {IS_NULL: ["$G_perf", 0]},
    }

    eia = app_name.upper() == "EIA"
    # For EIA, consider total_perf, O_perf, G_perf; for CCA, consider total_perf, H/M/M_D.
    all_zero_cond_eia = {
        "$and": [
            {"$eq": ["$total_perf", 0]},
            {"$eq": ["$O_perf", 0]},
            {"$eq": ["$G_perf", 0]},
        ]
    }
    all_zero_cond_cca = {
        "$and": [
            {"$eq": ["$total_perf", 0]},
            {"$eq": ["$HC_perf", 0]},
            {"$eq": ["$M_perf", 0]},
            {"$eq": ["$M_D_perf", 0]},
        ]
    }

    drop_all_zero_stage = {
        MATCH_NAME: {
            "$expr": {"$not": (all_zero_cond_eia if eia else all_zero_cond_cca)}
        }
    }

    group_stage = {
        "_id": "$org",
        "avg_total_perf": {"$avg": "$total_perf"},
        "hc_avg": {"$avg": "$HC_perf"},
        "m_avg": {"$avg": "$M_perf"},
        "md_avg": {"$avg": "$M_D_perf"},
        "o_avg": {"$avg": "$O_perf"},
        "g_avg": {"$avg": "$G_perf"},
    }

    pipeline = [
        {MATCH_NAME: base_filter},
        {PROJECT_NAME: project_stage},
        drop_all_zero_stage,  # remove all-zero docs before averaging
        {GROUP_NAME: group_stage},
        {SORT_NAME: {"_id": 1}},
    ]

    rows = await coll.aggregate(pipeline).to_list(length=None)

    if not rows:
        return {
            "xAxis": {"categories": []},
            "series": [{"data": []}],
            "drilldown": {"series": []},
        }

    categories = [r["_id"] for r in rows]
    series_data = []
    drill_series = []

    for r in rows:
        org = r["_id"]
        drill_id = f"{org}_perf"
        series_data.append({
            "name": org,
            "y": float(r.get("avg_total_perf", 0) or 0),
            "drilldown": drill_id
        })
        if eia:
            drill = [
                ["Optimal", float(r.get("o_avg", 0) or 0)],
                ["Good", float(r.get("g_avg", 0) or 0)],
            ]
        else:
            drill = [
                ["HC", float(r.get("hc_avg", 0) or 0)],
                ["M", float(r.get("m_avg", 0) or 0)],
                ["M&D", float(r.get("md_avg", 0) or 0)],
            ]
        drill_series.append({"name": org, "id": drill_id, "data": drill})

    return {
        "xAxis": {"categories": categories},
        "series": [{"data": series_data}],
        "drilldown": {"series": drill_series},
    }


async def upload_by_org(app_name: str, date_filter: str):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Optional filter scaffold (app/date/org exclusions), reuse your helpers if needed.
    # If you want a date filter on created_at or recommendation_date, build it here similarly to other functions.
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    # 1) Count per org
    org_counts_pipeline = [
        {MATCH_NAME: base_filter},
        {GROUP_NAME: {"_id": "$org", "count": {"$sum": 1}}},
        {SORT_NAME: {"_id": 1}},
    ]
    org_rows = await coll.aggregate(org_counts_pipeline).to_list(length=None)

    # 2) Count per org+user for drilldown
    org_user_counts_pipeline = [
        {MATCH_NAME: base_filter},
        {GROUP_NAME: {"_id": {"org": "$org", "user": "$user"}, "count": {"$sum": 1}}},
        {SORT_NAME: {"_id.org": 1, "_id.user": 1}},
    ]
    org_user_rows = await coll.aggregate(org_user_counts_pipeline).to_list(length=None)

    # Build top-level series points
    series_points = [
        {"name": r["_id"], "y": int(r["count"]), "drilldown": r["_id"]}
        for r in org_rows
        if r["_id"] is not None
    ]

    # Group drilldown data by org
    drill_map = {}
    for row in org_user_rows:
        org = row["_id"]["org"]
        user = row["_id"]["user"]
        if org is None or user is None:
            continue
        drill_map.setdefault(org, []).append([user, int(row["count"])])

    # Produce drilldown series array with stable ids matching parent points
    drill_series = [
        {"name": org, "id": org, "data": users}
        for org, users in drill_map.items()
    ]

    return {
        "series": [
            {
                "name": "Organizations",
                "data": series_points
            }
        ],
        "drilldown": {
            "series": drill_series
        }
    }


async def upload_trend(app_name: str, date_filter: str = "30"):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Build time window for recommendation_date
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    # Optional ignored orgs
    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {REGEX_NAME: str(o), OPTIONS_NAME: "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    pipeline = [
        {MATCH_NAME: base_filter},
        # Truncate to UTC day; this makes one bucket per calendar day (UTC)
        {PROJECT_NAME: {
            "_id": 0,
            "day": {"$dateTrunc": {"date": "$recommendation_date", "unit": "day"}}
        }},
        {GROUP_NAME: {
            "_id": "$day",
            "count": {"$sum": 1}
        }},
        {PROJECT_NAME: {
            "_id": 0,
            "ts": {"$toLong": "$_id"},  # UTC midnight epoch ms for the day
            "count": 1
        }},
        {SORT_NAME: {"ts": 1}}
    ]

    rows = await coll.aggregate(pipeline).to_list(length=None)
    # Return only days that exist (skip days with zero)
    return [[int(r["ts"]), int(r["count"])] for r in rows]

def eia_savings_fetch(top_rows, dd_id_1,dd_id_2,drill_rows,drill_map_1, drill_map_2,categories, dd_name_1, dd_name_2, s1_name, s2_name):
    series1 = []
    series2 = []
    for r in top_rows:
        org = r["_id"]
        series1.append({"y": float(r.get("opt_sum", 0) or 0), "drilldown": dd_id_1(org)})
        series2.append({"y": float(r.get("good_sum", 0) or 0), "drilldown": dd_id_2(org)})

    for r in drill_rows:
        org = r["_id"]["org"]
        user = r["_id"]["user"]
        if org is None or user is None:
            continue
        drill_map_1.setdefault(org, []).append([user, float(r.get("opt_sum", 0) or 0)])
        drill_map_2.setdefault(org, []).append([user, float(r.get("good_sum", 0) or 0)])

    drill_series = []
    for org in categories:
        drill_series.append({"id": dd_id_1(org), "name": dd_name_1(org), "data": drill_map_1.get(org, [])})
        drill_series.append({"id": dd_id_2(org), "name": dd_name_2(org), "data": drill_map_2.get(org, [])})

    result = {
            "xAxis": {"categories": categories},
            "series": [
                {"name": s1_name, "data": series1},
                {"name": s2_name, "data": series2},
            ],
            "drilldown": {"series": drill_series},
        }
    return result
    

async def total_savings(app_name: str, date_filter: str = "ALL"):
    coll = get_collection(CollectionNames.RECOMMENDATION_ANALYTICS)

    # Build app/date filter; if you need a specific date basis, wire it here
    base_filter, status_code = get_metrics_date_filter_condition(date_filter, app_name)
    if status_code:
        raise CustomAPIException(status_code=status_code, message=base_filter, error_code=-1)

    # Exclude ignored orgs and null orgs
    _, ignored_orgs, _ = get_exclusion_conditions(app_name)
    if ignored_orgs:
        base_filter["$nor"] = (
            [{"org": {"$regex": str(o), "$options": "i"}} for o in ignored_orgs if isinstance(o, str) and o]
            + [{"org": None}]
        )

    is_eia = app_name.upper() == "EIA"

    # Field mapping per app
    if is_eia:
        # EIA series: Optimal, Good
        proj = {
            "_id": 0,
            "org": 1,
            "user": 1,
            "O_saving": {"$ifNull": ["$O_saving", 0]},
            "G_saving": {"$ifNull": ["$G_saving", 0]},
        }
        top_group = {
            "_id": "$org",
            "opt_sum": {"$sum": "$O_saving"},
            "good_sum": {"$sum": "$G_saving"},
        }
        # Drilldown org-user sums
        drill_group = {
            "_id": {"org": "$org", "user": "$user"},
            "opt_sum": {"$sum": "$O_saving"},
            "good_sum": {"$sum": "$G_saving"},
        }
        # Labels and ids
        s1_name, s2_name = "Optimal", "Good"
        dd_id_1 = lambda org: f"{org}_m_savings_i"
        dd_id_2 = lambda org: f"{org}_m_savings_ii"
        dd_name_1 = lambda org: f"Optimal ({org})"
        dd_name_2 = lambda org: f"Good ({org})"
    else:
        # CCA series: HC, M, M&D
        proj = {
            "_id": 0,
            "org": 1,
            "user": 1,
            "HC_saving": {"$ifNull": ["$HC_saving", 0]},
            "M_saving": {"$ifNull": ["$M_saving", 0]},
            "M_D_saving": {"$ifNull": ["$M_D_saving", 0]},
        }
        top_group = {
            "_id": "$org",
            "hc_sum": {"$sum": "$HC_saving"},
            "m_sum": {"$sum": "$M_saving"},
            "md_sum": {"$sum": "$M_D_saving"},
        }
        drill_group = {
            "_id": {"org": "$org", "user": "$user"},
            "hc_sum": {"$sum": "$HC_saving"},
            "m_sum": {"$sum": "$M_saving"},
            "md_sum": {"$sum": "$M_D_saving"},
        }
        s1_name, s2_name, s3_name = "HC", "M", "M&D"
        dd_id_1 = lambda org: f"{org}_hourly"
        dd_id_2 = lambda org: f"{org}_modernize"
        dd_id_3 = lambda org: f"{org}_downsize"
        dd_name_1 = lambda org: f"HC ({org})"
        dd_name_2 = lambda org: f"M ({org})"
        dd_name_3 = lambda org: f"M&D ({org})"

    # Top series pipeline (org-level sums)
    top_pipeline = [
        {"$match": base_filter},
        {"$project": proj},
        {"$group": top_group},
        {"$sort": {"_id": 1}},
    ]
    top_rows = await coll.aggregate(top_pipeline).to_list(length=None)

    # Drilldown pipeline (org-user-level sums)
    drill_pipeline = [
        {"$match": base_filter},
        {"$project": proj},
        {"$group": drill_group},
        {"$sort": {"_id.org": 1, "_id.user": 1}},
    ]
    drill_rows = await coll.aggregate(drill_pipeline).to_list(length=None)

    # Categories
    categories = [r["_id"] for r in top_rows]

    # Build series and drilldown
    drill_map_1 = {}
    drill_map_2 = {}
    drill_map_3 = {}  # CCA only

    if is_eia:
        return eia_savings_fetch(top_rows, dd_id_1,dd_id_2,drill_rows,drill_map_1, drill_map_2,categories, dd_name_1, dd_name_2, s1_name, s2_name)
    else:
        series1 = []
        series2 = []
        series3 = []
        for r in top_rows:
            org = r["_id"]
            series1.append({"y": float(r.get("hc_sum", 0) or 0), "drilldown": dd_id_1(org)})
            series2.append({"y": float(r.get("m_sum", 0) or 0), "drilldown": dd_id_2(org)})
            series3.append({"y": float(r.get("md_sum", 0) or 0), "drilldown": dd_id_3(org)})

        for r in drill_rows:
            org = r["_id"]["org"]
            user = r["_id"]["user"]
            if org is None or user is None:
                continue
            drill_map_1.setdefault(org, []).append([user, float(r.get("hc_sum", 0) or 0)])
            drill_map_2.setdefault(org, []).append([user, float(r.get("m_sum", 0) or 0)])
            drill_map_3.setdefault(org, []).append([user, float(r.get("md_sum", 0) or 0)])

        drill_series = []
        for org in categories:
            drill_series.append({"id": dd_id_1(org), "name": dd_name_1(org), "data": drill_map_1.get(org, [])})
            drill_series.append({"id": dd_id_2(org), "name": dd_name_2(org), "data": drill_map_2.get(org, [])})
            drill_series.append({"id": dd_id_3(org), "name": dd_name_3(org), "data": drill_map_3.get(org, [])})

        result = {
                "xAxis": {"categories": categories},
                "series": [
                    {"name": s1_name, "data": series1},
                    {"name": s2_name, "data": series2},
                    {"name": s3_name, "data": series3},
                ],
                "drilldown": {"series": drill_series},
            }
        return result

async def get_savings_data(app_name: str, date_filter: Optional[str]):
    try:
        if not app_name:
            raise CustomAPIException(status_code=400, message="Appname header missing", error_code=-1)

        upload_by_org_response = await upload_by_org(app_name, date_filter)
        upload_trend_data = await upload_trend(app_name, date_filter)
        total_savings_response = await total_savings(app_name, date_filter)
        instance_with_no_rec_response = await fetch_analytics_without_recommendation(app_name, date_filter)

        response = {
            "UploadByOrg": upload_by_org_response,
            "UploadTrend": upload_trend_data,
            "TotalSavings": total_savings_response,
            "InstanceWithNoResponse": instance_with_no_rec_response
        }

        if not any(response.values()):
            raise CustomAPIException(status_code=404, message="No savings data found", error_code=-1)

        return {"Message": "Savings data fetched successfully", "Data": response, "ErrorCode": 1}
    except CustomAPIException:
        raise
    except Exception as err:
        logging.error(f"Unable to fetch Savings data: {str(err)}")
        raise CustomAPIException(status_code=500, message="Unable to fetch Savings data", error_code=-1)


async def fetch_analytics_without_recommendation(app_name: str, date_filter: Optional[str]):
    """
    Fetch analytics data from 'analytics_without_recommendation' collection,
    grouped by CSP → Zone → Current Instance.
    """
    # Step 1: Build date filter condition
    query_filter, error_code = get_date_filter_condition(date_filter, app_name)
    if error_code == 400:
        raise CustomAPIException(status_code=400, message=query_filter, error_code=-1)

    # Step 2: Fetch data from analytics_without_recommendation collection
    analytics_data = await get_collection(CollectionNames.ANALYTICS_WITHOUT_RECOMMENDATION).find(
        query_filter, {"_id": 0, "cloud": 1, "region": 1, "current_instance": 1}
    ).to_list(length=None)

    if not analytics_data:
        return {"series": [], "drilldown_zone": {"series": []}, "drilldown_instance": {"series": []}}

    # Step 3: Initialize aggregators
    csp_summary = defaultdict(int)
    zone_summary = defaultdict(lambda: defaultdict(int))
    instance_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    # Step 4: Aggregate by CSP → Zone → Instance
    for record in analytics_data:
        try:
            csp = record.get("cloud", "Unknown").upper().strip()
            zone = record.get("region", "Unknown").lower().strip()
            instance = record.get("current_instance", "Unknown").lower().strip()
        except Exception:
            continue

        csp_summary[csp] += 1
        zone_summary[csp][zone] += 1
        instance_summary[csp][zone][instance] += 1

    # Step 5: Build Highcharts-compatible structure
    series_data = [{
        "name": "Cloud Service Provider",
        "data": [{"name": csp, "y": count, "drilldown": csp} for csp, count in csp_summary.items()]
    }]

    drilldown_zone_series = [
        {"name": csp, "id": csp, "data": [[zone, count] for zone, count in zones.items()]}
        for csp, zones in zone_summary.items()
    ]

    drilldown_instance_series = [
        {"name": zone, "id": zone, "data": [[instance, count] for instance, count in instances.items()]}
        for csp, zones in instance_summary.items()
        for zone, instances in zones.items()
    ]

    return {
        "series": series_data,
        "drilldown_zone": {"series": drilldown_zone_series},
        "drilldown_instance": {"series": drilldown_instance_series},
    }