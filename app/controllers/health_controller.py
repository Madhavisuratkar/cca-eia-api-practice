from datetime import datetime, timezone
from fastapi import APIRouter, Path
from fastapi.responses import JSONResponse
from app.connections.mongodb import check_mongo_status
from app.utils.constants import ApplicationEndpoints
from app.connections.env_config import CS_UI, EIA_UI, CCA_UI, TO_EMAIL, app_env
import httpx

# Import your send_email function here
from app.utils.email_utils import send_email

health_router = APIRouter()

APP_URLS = {
    "CS UI": CS_UI,
    "CCA UI": CCA_UI,
    "EIA UI": EIA_UI
}

@health_router.get(ApplicationEndpoints.HEALTH_CHECK, tags=["Health"])
async def health_check(app_name: str = Path(..., regex="^(eia|cca|CCA|EIA)$")):
    app_name_upper = app_name.upper()
    mongo_ok = await check_mongo_status() is True
    cs_api_state = "unreachable"
    cs_db_state = "unreachable"
    health_url = str(CS_UI) + "/csapi/health"
    try:
        async with httpx.AsyncClient(verify=False, timeout=3.0) as client:
            response = await client.get(health_url)
            if response.status_code == 200:
                cs_api_state = "ok"
                cs_db_state = "ok"
            elif response.status_code == 500:
                cs_api_state = "ok"
                cs_db_state = "unreachable"
            else:
                cs_api_state = "unreachable"
                cs_db_state = "unreachable"
    except Exception:
        cs_api_state = "unreachable"
        cs_db_state = "unreachable"

    app_ui_statuses = {}

    async with httpx.AsyncClient(verify=False, timeout=3.0) as client:
        # Always check CSUI
        try:
            resp = await client.get(APP_URLS["CS UI"])
            app_ui_statuses["CS UI"] = "ok" if resp.status_code == 200 else "unreachable"
        except Exception:
            app_ui_statuses["CS UI"] = "unreachable"

        # Check UI based on app_type parameter from URL
        relevant_ui_key = "EIA UI" if app_name_upper == "EIA" else "CCA UI"
        try:
            resp = await client.get(APP_URLS[relevant_ui_key])
            app_ui_statuses[relevant_ui_key] = "ok" if resp.status_code == 200 else "unreachable"
        except Exception:
            app_ui_statuses[relevant_ui_key] = "unreachable"

    overall_ok = (
        mongo_ok and (cs_api_state == "ok" and cs_db_state == "ok")
        and all(status == "ok" for status in app_ui_statuses.values())
    )
    http_status = 200 if overall_ok else 500
    timestamp = datetime.now(timezone.utc)

    # If status is unhealthy, send an alert email
    if http_status != 200:
        subject = f"[{app_env}] Health check alert for {app_name_upper}"
        body = f"""
        <p>Hi All,</p>
        <p>Health check for application <b>{app_name_upper}</b> failed at <i>{timestamp}</i>.</p>
        <p><b>Environment:</b> {app_env}</p>
        <p>Details:</p>
        <ul>
            <li><b>{app_name_upper} API:</b> ok</li>
            <li><b>{app_name_upper} DB:</b> {'ok' if mongo_ok else 'unreachable'}</li>
            <li><b>CS API:</b> {cs_api_state}</li>
            <li><b>CS DB:</b> {cs_db_state}</li>
        """

        for ui_name, status in app_ui_statuses.items():
            body += f"<li><b>{ui_name}:</b> {status}</li>"

        body += "</ul>"

        await send_email(subject, TO_EMAIL, body)

    return JSONResponse(
        status_code=http_status,
        content={
            "status": "healthy" if overall_ok else "unhealthy",
            "timestamp": str(timestamp),
            "services": {
                app_name_upper: {
                    "api": "ok",
                    "database": "ok" if mongo_ok else "unreachable"
                },
                "CS": {
                    "api": cs_api_state,
                    "database": cs_db_state
                },
                **app_ui_statuses
            }
        }
    )
