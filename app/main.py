from app.connections.env_config import GET_ENV
from app.middleware.activity_middleware import ActivityLoggingMiddleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from app.utils.cs_database import init_db
from app.connections.custom_exceptions import CustomAPIException
from app.middleware.auth_middleware import AuthMiddleware
from app.middleware.exception_handlers import custom_api_exception_handler
from app.middleware.org_summary_middleware import OrgSummaryMiddleware
from app.middleware.request_context_middleware import RequestContextMiddleware
from .connections.mongodb import connect_to_mongo, close_mongo_connection
from .controllers import include_routes
from app.connections.swagger_connection import custom_openapi


def create_app() -> FastAPI:
    app = FastAPI(title="FastAPI with MongoDB",
                  root_path="/ccaapi",
                  docs_url=None if GET_ENV == "PROD" else "/docs",  
                  redoc_url = None if GET_ENV == "PROD" else "/redoc",
                  openapi_url = None if GET_ENV == "PROD" else "/openapi.json")

    app.add_middleware(AuthMiddleware)
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(ActivityLoggingMiddleware)
    app.add_middleware(OrgSummaryMiddleware)


    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Exception handler registration
    app.add_exception_handler(CustomAPIException, custom_api_exception_handler)
    
    # Handle Pydantic validation errors using same custom format
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        message = exc.errors()[0]['msg'] if exc.errors() else "Invalid request"
        return JSONResponse(
            status_code=422,
            content={
                "Message": message,
                "ErrorCode": -1
            }
        )

    init_db()
    include_routes(app)

    app.add_event_handler("startup", connect_to_mongo)
    app.add_event_handler("shutdown", close_mongo_connection)

    app.openapi = lambda: custom_openapi(app)

    return app