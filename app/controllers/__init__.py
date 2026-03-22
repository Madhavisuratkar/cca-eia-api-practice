from fastapi import FastAPI
from app.utils.common_utils import inject_endpoint_docs

def include_routes(app: FastAPI):
    """
    Import and include all application routers into the FastAPI app,
    optionally injecting endpoint documentation enhancements.
    """
    # Import routers
    from app.controllers.health_controller import health_router
    from app.controllers.portfolios_controller import portfolios_router
    from app.controllers.telemetry_controller import telemetry_router
    from app.controllers.etl_controller import etl_router
    from app.controllers.login_controller import login_router
    from app.controllers.cost_advise_controller import cost_advise_router
    from app.controllers.validation_controller import input_validation
    from app.controllers.explorer_controller import explorer_router
    from app.controllers.bulk_upload_controller import bulk_router
    from app.controllers.notification_controller import notifications_router
    from app.controllers.sales_client_controller import sales_client_router
    from app.controllers.insights_controller import insights_router

    routers = [
        health_router,
        portfolios_router,
        telemetry_router,
        etl_router,
        login_router,
        cost_advise_router,
        input_validation,
        explorer_router,
        bulk_router,
        notifications_router,
        sales_client_router,
        insights_router
    ]

    # Inject docs and include routers
    for router in routers:
        inject_endpoint_docs(router)
        app.include_router(router)
