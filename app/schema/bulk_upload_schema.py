from bson import ObjectId
from pydantic import BaseModel, BeforeValidator, Field, ValidationInfo, field_validator, model_validator, validator
from typing import Annotated, Optional
from enum import Enum

from app.connections.custom_exceptions import CustomAPIException

class CloudProvider(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"

CloudProviderType = Annotated[
    CloudProvider,
    BeforeValidator(lambda v: v.strip().upper() if isinstance(v, str) else v)
]


class BulkUploadQuery(BaseModel):
    portfolio_id: Optional[str] = Field(None, description="Existing portfolio ID (required when updating or uploading UDF)")
    file_name: Optional[str] = Field(None, description="Name of the main file to upload")
    udf_file: Optional[str] = Field(None, description="Optional UDF file path")

    portfolioName: Optional[str] = Field(None, description="Portfolio name (required only for new portfolio creation)")
    provider: Optional[str] = Field(None, description="Cloud provider (required only for new portfolio creation)", example="AWS")
    headroom: Optional[int] = Field(20, description="Optional headroom value, default 20")
    file_type: str = Field("application/octet-stream", description="MIME type of the file (required for all uploads)")

    # ✅ New field
    is_billing_data: bool = Field(False, description="Set to True only when both file_name and portfolioName are provided")

    # policy engine
    policy_engine: Optional[str] = Field(None, description="Policy Engine")

    created_for: Optional[str] = Field(None, description="created for organization")


    # --- Field validators ---
    @field_validator("file_name", mode="before")
    def strip_file_name(cls, v):
        return v.strip() if isinstance(v, str) else v
    
    # --- Field-level validators ---
    @field_validator("file_name", "udf_file", mode="before")
    def validate_xlsx_file(cls, v, info: ValidationInfo):
        """Ensure file names are .xlsx"""
        if v is None:
            return v  # Optional field
        v = v.strip()
        if "." not in v or not v.lower().endswith(".xlsx"):
            raise CustomAPIException(
                status_code=422,
                message=f"{info.field_name} must be a valid .xlsx file",
                error_code=-1
            )
        return v

    @field_validator("portfolioName", mode="before")
    def strip_portfolio_name(cls, v):
        if isinstance(v, str):
            v = v.strip()
        if v == "":
            raise CustomAPIException(status_code=422, message="portfolioName cannot be empty", error_code=-1)
        return v

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v not in CloudProvider.__members__:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be one of {list(CloudProvider.__members__.keys())}",
                error_code=-1
            )
        return v

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v

    # --- Model-level validator ---
    @model_validator(mode="after")
    def validate_dependencies(cls, model):
         # ✅ Validate MongoDB ObjectId format
        if model.portfolio_id and not ObjectId.is_valid(model.portfolio_id):
            raise CustomAPIException(
                status_code=400,
                message=f"Invalid portfolio_id '{model.portfolio_id}'. Must be a valid 24-character hex string.",
                error_code=-1
            )


        if not model.file_name and not model.udf_file:
            raise CustomAPIException(
                status_code=422,
                message="At least one of 'file_name' or 'udf_file' is required",
                error_code=-1
            )

        # Case 1: UDF upload
        if model.udf_file:
            if not model.portfolio_id:
                raise CustomAPIException(
                    status_code=422,
                    message="portfolio_id is required when uploading udf_file",
                    error_code=-1
                )
            if model.portfolioName:
                raise CustomAPIException(
                    status_code=422,
                    message="portfolioName must not be provided when uploading udf_file",
                    error_code=-1
                )
            
        # ---- Case 1: UDF upload ----
        if model.udf_file and model.portfolio_id and model.file_name:
            raise CustomAPIException(
                status_code=422,
                message="Cannot send both file_name and udf_file with portfolio_id",
                error_code=-1
            )

        # Case 2: Main file update (file_name + portfolio_id)
        if model.file_name and model.portfolio_id:
            if model.udf_file:
                raise CustomAPIException(
                    status_code=422,
                    message="Cannot send both file_name and udf_file with portfolio_id",
                    error_code=-1
                )
            if model.portfolioName:
                raise CustomAPIException(
                    status_code=422,
                    message="portfolioName must not be provided when updating existing portfolio with file_name",
                    error_code=-1
                )
            # provider not required in this case

        # Case 3: New portfolio creation (file_name only)
        if model.file_name and not model.portfolio_id:
            missing = []
            if not model.portfolioName:
                missing.append("portfolioName")
            if not model.provider:
                missing.append("provider")
            if missing:
                raise CustomAPIException(
                    status_code=422,
                    message=f"Missing required fields for new portfolio: {', '.join(missing)}",
                    error_code=-1
                )

        # ✅ Validate is_billing_data rules
        if model.is_billing_data:
            if not (model.file_name and model.portfolioName):
                raise CustomAPIException(
                    status_code=422,
                    message="is_billing_data can only be True when both file_name and portfolioName are provided",
                    error_code=-1
                )

        return model



class CostAdviceRequestSchema(BaseModel):
    """
    Schema for initiating cost advice recommendation.
    - portfolio_id: ID of the portfolio
    - app_name: Application name ('CCA' or 'EIA')
    """

    portfolio_id: str = Field(..., description="The ID of the portfolio")