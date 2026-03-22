from typing import List, Optional, Dict, Any, Union, Literal, Annotated
from pydantic import BaseModel, Field, validator, ValidationError
from enum import Enum
from fastapi import Query, HTTPException
from app.utils.common_utils import RequiredFieldValidator
from app.utils.constants import ALLOWED_PROVIDERS, LevelType
from app.connections.pylogger import log_message
from app.connections.custom_exceptions import CustomAPIException

from typing import Optional
from fastapi import Query, HTTPException
from pydantic import BaseModel, ValidationError, validator
from app.connections.custom_exceptions import CustomAPIException
from app.connections.pylogger import log_message
from app.utils.constants import LevelType

# Shared mixin for case-insensitive provider validation
class CaseInsensitiveProviderMixin(BaseModel):
    provider: str

    @validator("provider")
    def check_provider(cls, v):
        valid = {"AWS", "AZURE", "GCP"}
        v_up = v.upper().strip()
        if v_up not in valid:
            raise CustomAPIException(status_code=400, message=f"provider must be one of {sorted(valid)}")
        return v_up
    
class CaseInsensitiveCloudCspMixin(BaseModel):
    cloud_csp: str

    @validator("cloud_csp")
    def check_provider(cls, v):
        valid = {"AWS", "AZURE", "GCP"}
        v_up = v.upper().strip()
        if v_up not in valid:
            raise CustomAPIException(status_code=400, message=f"cloud provider must be one of {sorted(valid)}")
        return v_up
    
class ValidateDataRecordEIA(CaseInsensitiveCloudCspMixin, RequiredFieldValidator):
    instance_type: Optional[str] = Field(None, alias="instance type", example="c5.12xlarge")
    region: Optional[str] = Field(None, example="ap-east-1")
    uuid: Optional[str] = Field(None, example="1c2415e8-ac96-447f-b2fb-b31c2953caf4")
    max_cpu_percent: Optional[Any] = Field(None, alias="max cpu%", example=12)
    max_mem_used: Optional[Any] = Field(None, alias="max mem used", example=12)
    max_network_bw: Optional[Any] = Field(None, alias="max network bw", example=11)
    max_disk_bw_used: Optional[Any] = Field(None, alias="max disk bw used", example=12)
    max_iops: Optional[Any] = Field(None, alias="max iops", example=1234)
    pricingModel: Optional[str] = Field(None, example="ondemand")
    uavg: Optional[Any] = Field(None, example=1)
    u95: Optional[Any] = Field(None, example=1)

    class Config:
        allow_population_by_field_name = True  # allows passing either alias or field name as input keys


class DataRecordEIA(CaseInsensitiveCloudCspMixin, RequiredFieldValidator):
    instance_type: Optional[str] = Field(None, alias="instance type", example="c5.12xlarge")
    region: Optional[str] = Field(None, example="ap-east-1")
    uuid: Optional[str] = Field(None, example="1c2415e8-ac96-447f-b2fb-b31c2953caf4")
    max_cpu_percent: Optional[Any] = Field(None, alias="max cpu%", example=12)
    max_mem_used: Optional[Any] = Field(None, alias="max mem used", example=12)
    max_network_bw: Optional[Any] = Field(None, alias="max network bw", example=11)
    max_disk_bw_used: Optional[Any] = Field(None, alias="max disk bw used", example=12)
    max_iops: Optional[Any] = Field(None, alias="max iops", example=1234)
    pricingModel: Optional[str] = Field(None, example="ondemand")
    uavg: Optional[Any] = Field(None, example=1)
    u95: Optional[Any] = Field(None, example=1)
    isSaved: Optional[bool] = Field(None, example=True)
    adjusted: Optional[bool] = Field(None, example=False)

    class Config:
        allow_population_by_field_name = True  # allows passing either alias or field name as input keys

class DataRecordCCA(CaseInsensitiveCloudCspMixin, RequiredFieldValidator):
    region: Optional[str] = Field(None, example="af-south-1")
    instance_type: Optional[str] = Field(None, alias="instance type", example="c5.18xlarge")
    monthly_utilization_hourly: Optional[Any] = Field(None, alias="monthly utilization (hourly)", example="1")
    pricingModel: Optional[str] = Field(None, example="ondemand")
    quantity: Optional[Any] = Field(None, example="2")
    uuid: Optional[str] = Field(None, example="abc")

    class Config:
        allow_population_by_field_name = True  # allows passing either alias or field name as input keys

class InputValidateCCA(CaseInsensitiveProviderMixin, RequiredFieldValidator):
    appName: Literal["CCA"] = Field(..., description="Application name discriminator")
    data: List[DataRecordCCA]

# EIA input request model
class InputValidateEIA(CaseInsensitiveProviderMixin, RequiredFieldValidator):
    appName: Literal["EIA"] = Field(..., description="Application name discriminator")
    data: List[ValidateDataRecordEIA]
    udf: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        example=[{"key": "env", "value": "production"}]
    )

# Discriminated union schema
InputValidateRequest = Annotated[Union[InputValidateCCA, InputValidateEIA],Field(discriminator="appName")]

class InputCorrectCCA(CaseInsensitiveProviderMixin, RequiredFieldValidator):
    appName: Literal["CCA"] = Field(..., description="Application name discriminator", example="CCA")
    selectedColumn: Optional[str] = Field("", description="Selected column filter", example="region")
    instanceType: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Instance type range filter as dict with keys 'from' and 'to'",
        example={"from": "c5.12xlarge", "to": "c6.8xlarge"}
    )
    region: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Region range filter as dict with keys 'from' and 'to'",
        example={"from": "ap-east-1", "to": "us-east-2"}
    )
    pricingModel: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Pricing model range filter as dict with keys 'from' and 'to'",
        example={"from": "", "to": ""}
    )
    data: List[DataRecordCCA] = Field(
        ...,
        description="List of input data records in CCA style"
    )

class InputCorrectEIA(CaseInsensitiveProviderMixin, CaseInsensitiveCloudCspMixin,RequiredFieldValidator):
    appName: Literal["EIA"] = Field(..., description="Application name discriminator", example="EIA")
    selectedColumn: Optional[str] = Field("", description="Selected column filter", example="region")
    instanceType: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Instance type range filter as dict with keys 'from' and 'to'",
        example={"from": "c5.12xlarge", "to": "c6.8xlarge"}
    )
    region: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Region range filter as dict with keys 'from' and 'to'",
        example={"from": "ap-east-1", "to": "us-east-2"}
    )
    pricingModel: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Pricing model range filter as dict with keys 'from' and 'to'",
        example={"from": "", "to": ""}
    )
    data: List[DataRecordEIA] = Field(
        ...,
        description="List of input data records in EIA style"
    )

# Discriminated union for InputCorrect
InputCorrectRequest = Annotated[Union[ InputCorrectEIA,InputCorrectCCA],Field(discriminator="appName")]

class FileUploadValidateParams(CaseInsensitiveProviderMixin):
    provider: str = Query(..., description="Cloud provider", example="AWS")

    @validator('provider')
    def provider_uppercase(cls, v: str) -> str:
        return v.strip().upper()


def file_upload_validate_params(
    provider: str = Query(..., example="AWS", description="Cloud provider")
) -> FileUploadValidateParams:
    clean_provider = provider.strip().upper() if isinstance(provider, str) else ""
    if not clean_provider or clean_provider not in ALLOWED_PROVIDERS:
        log_message(LevelType.ERROR, "Invalid provider", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid provider")
    try:
        return FileUploadValidateParams(provider=clean_provider)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid provider. {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Internal server error")


class UploadvalidateSchema(RequiredFieldValidator):
    portfolio_id: str = Field(..., description="Portfolio ID", example="portfolio_123")