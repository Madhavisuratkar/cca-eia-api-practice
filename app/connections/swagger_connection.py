from fastapi.openapi.utils import get_openapi
from fastapi import FastAPI

def custom_openapi(app: FastAPI):
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="CCA - EIA",
        version="2.0.0",
        description=(
            "This API serves as the backend for the CCA - EIA (Cloud Cost Analyzer - "
            "EPYC Instance Advisor) application. It provides endpoints to manage "
            "cloud portfolio data, including saving, retrieving, renaming, and deleting portfolios. "
            "Authentication is handled via Bearer tokens (JWT), and the API supports request-level "
            "context such as user identity and application scope through headers and middleware."
        ),
        routes=app.routes,
    )

    # Ensure components and securitySchemes keys exist
    openapi_schema.setdefault("components", {})
    openapi_schema["components"].setdefault("securitySchemes", {})

    openapi_schema["components"]["securitySchemes"]["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT"
    }

    # Define the AppName header parameter
    appname_header = {
        "name": "AppName",
        "in": "header",
        "description": "Name of the application (AppName header)",
        "required": True,
        "schema": {
            "type": "string",
            "example": "CCA"
        }
    }

    # Add BearerAuth security and AppName header to all methods
    for path in openapi_schema["paths"].values():
        for operation in path.values():
            # Add security
            operation.setdefault("security", []).append({"BearerAuth": []})

            # Add parameters
            if "parameters" not in operation:
                operation["parameters"] = []
            # Prevent duplicate AppName if it already exists
            if not any(param.get("name") == "AppName" and param.get("in") == "header" for param in operation["parameters"]):
                operation["parameters"].append(appname_header)

    app.openapi_schema = openapi_schema
    return app.openapi_schema
