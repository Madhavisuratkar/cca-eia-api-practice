from fastapi import HTTPException
from app.schema.telemetry_schema import TelemetrySource
from app.services.datadog_service import check_datadog_connection, datadog_hosts, fetch_data
from app.services.prometheus_service import check_prometheus_connection, collect
from app.services.cloudwatch_service import check_cloudwatch_connection, cloudwatch_instances, collect_metrics
from app.services.azureinsights_service import check_azureinsights_connection, separate_metrics
from app.services.gcptelemetry_service import check_gcp_telemetry_connection, selected_instances
from datetime import datetime, timedelta
from app.utils.constants import LevelType
from app.connections.pylogger import log_message
import time
from app.connections.custom_exceptions import CustomAPIException
from app.services.portfolios_service import save_portfolio_data
from app.schema.portfolio_model_without_cloud import SavePortfolioRequest
from typing import Any, Dict, List, Optional
from pydantic import TypeAdapter
from sqlalchemy.orm import Session


def handle_datadog(query, get_metrics: bool):
    """"""
    conn, message, data = check_datadog_connection(query.apiKey, query.appKey, query.apiHost, query.provider.lower())
    if not conn:
        log_message(LevelType.ERROR, message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=message)
    if not get_metrics and getattr(query, "testFlag", False):
        return {"Message": message, "ErrorCode": 1}
    if not data:
        log_message(LevelType.ERROR, "No instances found.", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="No instances found.")
    
    response, res_message = datadog_hosts(data, query.region, query.provider.lower())
    if not response:
        log_message(LevelType.ERROR, res_message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=res_message)
    if get_metrics:
        return data
    return {"Data": response, "Message": res_message, "ErrorCode": 1}

def handle_prometheus(query):
    conn, message, data = check_prometheus_connection(query.prometheus_url, query.region, query.provider, query.username, query.password)
    if not conn:
        log_message(LevelType.ERROR, message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=message)
    if getattr(query, "testFlag", False):
        return {"Message": message, "ErrorCode": 1}
    if not data:
        log_message(LevelType.ERROR, "No instances found.", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="No instances found.")
    return {"Data": data, "Message": "Prometheus instances fetched successfully.", "ErrorCode": 1}

def handle_cloudwatch(query):
    conn, message, data = check_cloudwatch_connection(query.aws_access_key_id, query.aws_secret_access_key, query.region)
    if not conn:
        log_message(LevelType.ERROR, message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=message)
    if getattr(query, "testFlag", False):
        return {"Message": message, "ErrorCode": 1}
    if not data:
        log_message(LevelType.ERROR, "No instances found.", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="No instances found.")
    response, res_message = cloudwatch_instances(query.aws_access_key_id, query.aws_secret_access_key, query.region, data)
    if not response:
        log_message(LevelType.ERROR, res_message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=res_message)
    return {"Data": response, "Message": res_message, "ErrorCode": 1}

def handle_azureinsights(query, get_metrics: bool):
    conn, message, data, all_instances = check_azureinsights_connection(
        query.tenant_id, query.client_id, query.client_secret, query.subscription_id, query.region
    )
    if not conn:
        log_message(LevelType.ERROR, message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=message)
    if not get_metrics and getattr(query, "testFlag", False):
        return {"Message": message, "ErrorCode": 1}
    if not data:
        log_message(LevelType.ERROR, "No instances found.", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="No instances found.")
    if get_metrics:
        return all_instances
    return {"Data": data, "Message": "Azure Insights instances fetched successfully.", "ErrorCode": 1}

def handle_gcptelemetry(query, app_name: str, get_metrics: bool):
    conn, message, data, eia_result, cca_result = check_gcp_telemetry_connection(
        query.private_key, query.client_email, query.project_id, query.region, app_name
    )
    if not conn:
        log_message(LevelType.ERROR, message, ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=message)
    if not get_metrics and getattr(query, "testFlag", False):
        return {"Message": message, "ErrorCode": 1}
    if not data:
        log_message(LevelType.ERROR, "No instances found.", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="No instances found.")
    if get_metrics:
        return eia_result, cca_result
    return {"Data": data, "Message": "GCP telemetry instances fetched successfully.", "ErrorCode": 1}

# =========================
# Helpers: cloud_cred and saving
# =========================

def build_cloud_cred(source_type: TelemetrySource, query) -> Dict[str, Any]:
    """
    Build provider-specific cloud_cred payload to be saved in portfolio.
    Includes region list when available.
    """
    regions = getattr(query, "region", None)

    if source_type == TelemetrySource.datadog:
        cred = {
            "provider": "datadog",
            "apiKey": query.apiKey,
            "appKey": query.appKey,
            "apiHost": query.apiHost,
        }
        if regions:
            cred["region"] = regions
        return cred

    if source_type == TelemetrySource.prometheus:
        cred = {
            "provider": "prometheus",
            "prometheus_url": query.prometheus_url,
        }
        if regions:
            cred["region"] = regions
        return cred

    if source_type == TelemetrySource.cloudwatch:
        cred = {
            "provider": "cloudwatch",
            "aws_access_key_id": query.aws_access_key_id,
            "aws_secret_access_key": query.aws_secret_access_key,
            "cloud_csp": getattr(query, "provider", None) or "AWS",
        }
        if regions:
            cred["region"] = regions
        return cred

    if source_type == TelemetrySource.azureinsights:
        cred = {
            "provider": "azureinsights",
            "tenant_id": query.tenant_id,
            "client_id": query.client_id,
            "client_secret": query.client_secret,
            "subscription_id": query.subscription_id,
        }
        if regions:
            cred["region"] = regions
        return cred

    if source_type == TelemetrySource.gcptelemetry:
        cred = {
            "provider": "gcptelemetry",
            "private_key": query.private_key,
            "client_email": query.client_email,
            "project_id": query.project_id,
        }
        if regions:
            cred["region"] = regions
        return cred

    return {}


async def save_portfolio_and_instances(
    *,
    app_name: str,
    portfolio_name: str,
    provider: str,
    user_email: Optional[str],
    headroom: Optional[int],
    region: List[str],
    ipaddr: Optional[str],
    cloud_cred: Dict[str, Any],
    data: List[str],
    db: Session,
    policy_engine:str,
    created_for : Optional[str]
) -> Dict[str, Any]:
    """
    Uses your save_portfolio_logic to insert into portfolios and current_instances.
    - Portfolio name comes from the telemetry payload's 'name' field.
    - Saves cloud_cred (encrypted by save_portfolio_logic).
    - current_instances documents built from instances_for_current.
    """
    provider_up = (provider or "").upper() if provider else "UNKNOWN"
    user_email_eff = user_email
    headroom_eff = headroom if headroom is not None else 20

    payload_dict: Dict[str, Any] = {
        "portfolioName": portfolio_name,
        "provider": provider_up,
        "user_email": user_email_eff,
        "headroom%": headroom_eff,
        "data": data,
        "cloud_cred": cloud_cred,
        "appName" : app_name,
        "created_for" : created_for 
    }

    if app_name.upper() == "CCA":
        payload_dict["policy_engine"] = policy_engine

    request_model = TypeAdapter(SavePortfolioRequest).validate_python(payload_dict)
    result = await save_portfolio_data(db, request_model, app_name, ipaddr or None, user_email)
    return result


async def handle_telemetry_metrics(db: Session, query, source_type: TelemetrySource, app_name: str, user_email: str, ipaddr):
    try:
        provider = getattr(query, "provider", None)
        policy_engine = getattr(query, "policy_engine", None)
        region = getattr(query, "region", []) or []

        created_for =  getattr(query, "created_for", None)

        # Build cloud_cred once from query
        cloud_cred = build_cloud_cred(source_type, query)
        
        if source_type == TelemetrySource.gcptelemetry:
            eia_result, cca_result = handle_gcptelemetry(query, app_name, True)
            eia_result, cca_result = selected_instances(eia_result, cca_result, query.instances)
            if not eia_result and not cca_result:
                log_message(LevelType.ERROR, "No matching instances found.", ErrorCode=-1)
                raise CustomAPIException(status_code=500, message="No matching instances found.")

        elif source_type == TelemetrySource.azureinsights:
            all_instances = handle_azureinsights(query, True)
            eia_result, cca_result = separate_metrics(all_instances, query.instances)
            if not eia_result and not cca_result:
                log_message(LevelType.ERROR, "No matching instances found.", ErrorCode=-1)
                raise CustomAPIException(status_code=500, message="No matching instances found.")

        elif source_type == TelemetrySource.datadog:
            datadog_host_details = handle_datadog(query, True)
            host_names = [host['host_name'] for host in datadog_host_details]

            missing_hosts = [host for host in query.instances if host not in host_names]
            if missing_hosts:
                missing_hosts_str = ', '.join(missing_hosts)
                log_message(LevelType.ERROR, f"The account provided does not contain the hosts {missing_hosts_str}", ErrorCode=-1)
                raise CustomAPIException(
                    status_code=400,
                    message=f"The account provided does not contain the hosts {missing_hosts_str}"
                )

            instance_list = [f'host:{host}' for host in query.instances]
            end_time = int(time.time())
            start_time = int((datetime.now() - timedelta(days=1)).timestamp())

            cca_result, eia_result = fetch_data(
                app_name, instance_list, datadog_host_details,
                start_time, end_time, query.provider.lower()
            )
            if not eia_result and not cca_result:
                log_message(LevelType.ERROR, "No matching instances found.", ErrorCode=-1)
                raise CustomAPIException(status_code=500, message="No matching instances found.")

        elif source_type == TelemetrySource.prometheus:
            _ = handle_prometheus(query)
            eia_result, cca_result = collect(query.prometheus_url, query.region, query.provider, query.instances, query.username, query.password)
            if not eia_result and not cca_result:
                log_message(LevelType.ERROR, "No matching instances found.", ErrorCode=-1)
                raise CustomAPIException(status_code=500, message="No matching instances found.")

        elif source_type == TelemetrySource.cloudwatch:
            _ = handle_cloudwatch(query)
            eia_result, cca_result = collect_metrics(query.instances, query.aws_access_key_id, query.aws_secret_access_key, query.region)
            if not eia_result and not cca_result:
                log_message(LevelType.ERROR, "No matching instances found.", ErrorCode=-1)
                raise CustomAPIException(status_code=500, message="No matching instances found.")

        else:
            log_message(LevelType.ERROR, "Invalid telemetry source.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid telemetry source.")
        
        # ----- Persist portfolio and current_instances based on app_name -----
        app_upper = app_name.upper()
        if app_upper == "CCA":
            await save_portfolio_and_instances(
                app_name="CCA",
                portfolio_name=query.name,
                provider=provider,
                user_email=user_email,
                headroom=20,
                region=region,
                ipaddr=ipaddr,
                cloud_cred=cloud_cred,
                data=cca_result,
                db=db,
                policy_engine=policy_engine,
                created_for=created_for
            )
        elif app_upper == "EIA":
            await save_portfolio_and_instances(
                app_name="EIA",
                portfolio_name=query.name,
                provider=provider,
                user_email=user_email,
                headroom=20,
                region=region,
                ipaddr=ipaddr,
                cloud_cred=cloud_cred,
                data=eia_result,
                db=None,
                policy_engine=policy_engine,
                created_for=created_for
            )
        else:
            # If other apps call this route, skip saving to avoid misclassification
            log_message(LevelType.INFO, f"Skipping portfolio save for app '{app_name}' (only CCA/EIA handled).", ErrorCode=1000)

        # Return both result sets as before
        return eia_result, cca_result

    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Unexpected error occurred: {str(e)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"Unexpected error occurred: {str(e)}")



def handle_telemetry_connection(query, source_type: TelemetrySource, app_name: str):
    """"""
    try:
        if not app_name:
            log_message(LevelType.ERROR, "Appname header is required.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Appname header is required.")
        
        handler_map = {
            TelemetrySource.datadog: lambda: handle_datadog(query, False),
            TelemetrySource.prometheus: lambda: handle_prometheus(query),
            TelemetrySource.cloudwatch: lambda: handle_cloudwatch(query),
            TelemetrySource.azureinsights: lambda: handle_azureinsights(query, False),
            TelemetrySource.gcptelemetry: lambda: handle_gcptelemetry(query, app_name, False),
        }
        
        handler = handler_map.get(source_type)
        if not handler:
            log_message(LevelType.ERROR, "Invalid source_type provided.", ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid source_type provided.")

        return handler()
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"err in handle_telemetry_connection : {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"err in handle_telemetry_connection : {str(err)}")
