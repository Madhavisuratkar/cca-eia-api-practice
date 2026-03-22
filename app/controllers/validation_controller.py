from fastapi import APIRouter, Request
from app.utils.constants import (
    CLOUD_PROVIDERS,
    UNSUPPORTED_PROVIDERS,
    UNKNOWN_APP,
    LevelType,
    ApplicationEndpoints,
    ApplicationModuleTag
)
from app.services.validation_service import (
    file_upload_validate_service,
    validate_input_data,
    input_data_correction
)
from app.schema.validation_schema import (
    InputValidateRequest,
    InputCorrectRequest,
    UploadvalidateSchema
)
from app.connections.pylogger import log_message
input_validation = APIRouter()
from app.connections.custom_exceptions import CustomAPIException

@input_validation.post(
    ApplicationEndpoints.FILE_UPLOAD_VALIDATE,
    tags=[ApplicationModuleTag.INPUT_VALIDATION],
)
async def file_upload_validate(
    request: Request,
    payload: UploadvalidateSchema,
):
    """
    Controller: Validate user-uploaded file (fetched from S3 using portfolio_id)
    """
    try:
        app_name = request.state.app_name
        return await file_upload_validate_service(request, payload, app_name)

    except CustomAPIException as ce:
        raise ce

    except Exception as e:
        log_message(LevelType.ERROR, f"Unexpected error: {str(e)}", request=request, ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to validate input data", error_code=-1)


@input_validation.post(ApplicationEndpoints.INPUT_VALIDATE, tags=[ApplicationModuleTag.INPUT_VALIDATION])
async def input_validate(
    request: Request,
    body: InputValidateRequest
):
    """
    Method to validate the user provided jason form of data and show if any errors
    """
    try:
        app_name = request.state.app_name
        content_type = request.headers.get("Content-Type", "").lower()
        if "application/json" not in content_type:
            log_message(LevelType.ERROR, "Invalid Content-Type", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Unable to validate input data", error_code=-1)

        provider = body.provider.upper()

        if provider not in CLOUD_PROVIDERS:
            log_message(LevelType.ERROR, f"Invalid provider: {provider}", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid provider", error_code=-1)

        if provider in UNSUPPORTED_PROVIDERS:
            log_message(LevelType.ERROR, f"Unsupported provider: {provider}", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Unsupported provider", error_code=-1)
        input_data = body.data
        udf_data = getattr(body, "udf", None)
        if app_name.upper() == "EIA":
            instance_data, udf_errors, message, _ = await validate_input_data(input_data, provider, app_name, udf_data, request)
        else:
            instance_data, _, message, _ = await validate_input_data(input_data, provider, app_name, udf_data, request)
        if app_name == "EIA":
            return {
                "Message": message,
                "Provider": provider,
                "Data": instance_data,
                "UDF": udf_errors,
                "ErrorCode": 1
            }

        return {
            "Message": message,
            "Provider": provider,
            "Data": instance_data,
            "ErrorCode": 1
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Unexpected error: {str(e)}", request=request, ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to validate the input data", error_code=-1)

@input_validation.post(ApplicationEndpoints.INPUT_CORRECT, tags=[ApplicationModuleTag.INPUT_VALIDATION])
async def input_correct(
    request: Request,
    body: InputCorrectRequest
):
    """
    Method to correct the invalid inputs provided
    """
    try:
        provider = body.provider.strip().upper()
        input_data = body.data
        instance_type = body.instanceType or {}
        region = body.region or {}
        pricing_model = body.pricingModel or {}
        appname = request.state.app_name
        app = appname.strip().upper() if appname else UNKNOWN_APP

        if not provider or not input_data or not app:
            log_message(LevelType.ERROR, "Required field missing", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Required fields are missing", error_code=-1)

        if provider not in CLOUD_PROVIDERS:
            log_message(LevelType.ERROR, f"Invalid provider: {provider}", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Invalid provider", error_code=-1)

        corrected_data, message = input_data_correction(
            provider, instance_type, region, pricing_model, input_data, app
        )

        if not corrected_data:
            log_message(LevelType.ERROR, f"Correction failed: {message}", request=request, ErrorCode=-1)
            raise CustomAPIException(status_code=400, message="Unable to correct the input data", error_code=-1)

        return {
            "Message": message,
            "Provider": provider,
            "Data": corrected_data,
            "ErrorCode": 1
        }
    except CustomAPIException:
        raise
    except Exception as e:
        log_message(LevelType.ERROR, f"Unexpected error: {str(e)}", request=request, ErrorCode=-1)
        raise CustomAPIException(status_code=500, message="Unable to correct input data", error_code=-1)