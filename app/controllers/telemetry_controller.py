from fastapi import APIRouter, Request, Depends, Path, Body, Path
from app.schema.telemetry_schema import (
    TelemetrySource,TelemetryConnectionBody, TelemetryMetricsBody
)
from app.services.telemetry_service import handle_telemetry_connection, handle_telemetry_metrics
from app.utils.constants import ApplicationEndpoints, ApplicationModuleTag, LevelType
from app.connections.pylogger import log_message
from app.connections.custom_exceptions import CustomAPIException
from sqlalchemy.orm import Session
from app.utils.cs_database import get_db

telemetry_router = APIRouter()

@telemetry_router.post(
    ApplicationEndpoints.TELEMETRY_CONNECTION,
    tags=[ApplicationModuleTag.TELEMETRY_CONTROLLERS],
)
async def test_telemetry_connection(
    request: Request,
    source_type: TelemetrySource = Path(..., description="Telemetry source type"),
    body: TelemetryConnectionBody = Body(..., description="Telemetry connection parameters"),
):
    try:
        app_name = request.headers.get("Appname")
        if not app_name:
            raise CustomAPIException(status_code=400, message="Missing Appname header")

        # source_type matches body.source_type due to discriminator so safe to use either
        return handle_telemetry_connection(body, source_type, app_name)
    except CustomAPIException as err:
        raise err
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to check Telemetry connection  {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to check Telemetry connection.")


@telemetry_router.post(
    ApplicationEndpoints.TELEMETRY_METRICS,
    tags=[ApplicationModuleTag.TELEMETRY_CONTROLLERS],
)
async def get_telemetry_metrics(
    request: Request,
    source_type: TelemetrySource = Path(..., description="Telemetry source type"),
    body: TelemetryMetricsBody = Body(..., description="Telemetry metrics query parameters"),
    db: Session = Depends(get_db)
):
    try:
        user_email = request.state.user_email
        ipaddr = request.client.host
        app_name = request.headers.get("Appname")
        if not app_name:
            raise CustomAPIException(status_code=400, message="Missing Appname header")

        eia_result, cca_result = await handle_telemetry_metrics(db, body, source_type, app_name, user_email, ipaddr)
        if app_name.lower() == "cca":
            return {
                "Data": cca_result,
                "headroom%": 20,
                "Message": "Data fetched successfully",
                "ErrorCode": 1,
            }
        else:
            return {
                "Data": eia_result,
                "headroom%": 20,
                "Message": "Data fetched successfully",
                "ErrorCode": 1,
            }
    except CustomAPIException as err:
        raise err
    except Exception as err:
        log_message(LevelType.ERROR, f"Unable to get Telemetry metrics  {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to get Telemetry metrics.")
