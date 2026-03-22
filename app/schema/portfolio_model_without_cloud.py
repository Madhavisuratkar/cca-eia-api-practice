from typing import List, Optional, Union, Literal, Annotated
import uuid
from pydantic import BaseModel, EmailStr, Field, validator, BeforeValidator, constr, field_validator
from enum import Enum
from app.utils.common_utils import RequiredFieldValidator,RequiredFieldValidatorBulk
from datetime import datetime
from app.connections.custom_exceptions import CustomAPIException
from pydantic import ValidationInfo


class CloudProvider(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"

# Annotated type that accepts any case input
CloudProviderType = Annotated[
    CloudProvider,
    BeforeValidator(lambda v: v.strip().upper() if isinstance(v, str) else v)
]


class PortfolioRecordCCA(RequiredFieldValidatorBulk):
    cloud_csp: Optional[CloudProviderType] = Field(None, example="AWS")
    region: constr(strip_whitespace=True, min_length=1) = Field(..., example="af-south-1")
    instance_type: constr(strip_whitespace=True, min_length=1) = Field(..., alias="instance type", example="c5.18xlarge")
    monthly_utilization_hourly: Union[str, int, float] = Field(..., alias="monthly utilization (hourly)", example="1")
    pricingModel: constr(strip_whitespace=True, min_length=1) = Field(..., example="ondemand")
    quantity: Union[str, int, float] = Field(..., example=2)
    uuid: Optional[str] = Field(None, example="38669351-9512-6790-1652-6458")

    class Config:
        allow_population_by_field_name = True
        use_enum_values = True
        allow_population_by_alias = True
        by_alias = True

    @validator("uuid", pre=True, always=True)
    def set_uuid_if_missing(cls, v):
        return v or str(uuid.uuid4())

    @field_validator("monthly_utilization_hourly", "quantity", mode="before")
    def convert_to_float(cls, v, info: ValidationInfo):
        try:
            return float(v)
        except ValueError:
            field_name = info.field_name
            alias = field_name
            if field_name and field_name in cls.model_fields:
                field_info = cls.model_fields[field_name]
                if field_info.alias:
                    alias = field_info.alias
            
            raise CustomAPIException(
                status_code=400,
                message=f"{alias} must be a number",
                error_code=-1
            )


class PortfolioRecordEIA(RequiredFieldValidatorBulk):
    cloud_csp: Optional[CloudProviderType] = Field(None, example="AWS")
    instance_type: constr(strip_whitespace=True, min_length=1)= Field(..., alias="instance type", example="m6i.8xlarge")
    instance_name: Optional[constr(strip_whitespace=True, min_length=1)] = Field(None, alias="instance name", example="test instance")
    region: constr(strip_whitespace=True, min_length=1) = Field(..., example="eu-west-1")
    uuid: Optional[str] = Field(None, example="38669351-9512-6790-1652-6458")
    max_cpu_percent: float = Field(..., alias="max cpu%", example=10)
    max_mem_used: float = Field(..., alias="max mem used", example=1)
    max_network_bw: float = Field(..., alias="max network bw", example=5959)
    max_disk_bw_used: float = Field(..., alias="max disk bw used", example=1)
    max_iops: float = Field(..., alias="max iops", example=122.34)
    pricingModel: Optional[constr(strip_whitespace=True, min_length=1)] = Field(None, example="ondemand")
    uavg: Optional[float] = Field(None, example=12.12)
    u95: Optional[float] = Field(None, example=12.12)

    class Config:
        allow_population_by_field_name = True
        use_enum_values = True
        allow_population_by_alias = True
        by_alias = True

    @validator("uuid", pre=True, always=True)
    def set_uuid_if_missing(cls, v):
        return v or str(uuid.uuid4())

class SavePortfolioRequestCCA(RequiredFieldValidator):
    appName: Literal["CCA"] = Field(..., description="Application name discriminator", example="CCA")
    portfolioName: constr(strip_whitespace=True, min_length=1) = Field(..., description="Name of the portfolio", example="My Cloud Portfolio")
    provider: CloudProviderType = Field(..., description="Cloud provider for the portfolio", example="AWS")
    headroom: Optional[float] = Field(20, description="Headroom percentage for recommendations", example=20)
    policy_engine: Optional[str] = Field(None, description="Policy engine for recommendations", example="global")
    data: List[PortfolioRecordCCA] = Field(..., description="List of portfolio record data in CCA format")
    cloud_cred: Optional[dict] = Field(None, description="Cloud credentials for provider")

    # Hidden fields
    downloadable_link: bool = Field(False, description="Allow for download")
    password: Optional[constr(strip_whitespace=True, min_length=1)] = Field(None, description="password for file")

    created_for: Optional[str] = Field(None, description="created for organization")


    @validator('provider')
    def provider_uppercase(cls, v):
        return v.upper()

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v

    class Config:
        allow_population_by_field_name = True
        


class SavePortfolioRequestEIA(RequiredFieldValidator):
    appName: Literal["EIA"] = Field(..., description="Application name discriminator", example="EIA")
    portfolioName: constr(strip_whitespace=True, min_length=1) = Field(..., description="Name of the portfolio", example="My Cloud Portfolio")
    provider: CloudProviderType = Field(..., description="Cloud provider for the portfolio", example="AWS")
    headroom: Optional[float] = Field(20, description="Headroom percentage for recommendations", example=20)
    data: List[PortfolioRecordEIA] = Field(..., description="List of portfolio record data in EIA format")
    cloud_cred: Optional[dict] = Field(None, description="Cloud credentials for provider")
    udf: Optional[List[dict]] = Field(None, description="User defined fields (extra metadata)")

    # Hidden fields
    downloadable_link: bool = Field(False, description="Allow for download")
    password: Optional[str] = Field(None, description="password for file")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v



    @validator('provider')
    def provider_uppercase(cls, v):
        return v.upper()

    class Config:
        allow_population_by_field_name = True
        


# Discriminated union schema — Pydantic detects variant by 'appName' automatically
SavePortfolioRequest = Annotated[
    Union[SavePortfolioRequestCCA, SavePortfolioRequestEIA],
    Field(discriminator="appName")
]


#################################################################################


# list_for enum
class ListFor(str, Enum):
    ALL = "ALL"
    SELF = "SELF"
    OTHERS = "OTHERS"


ListForType = Annotated[
    ListFor,
    BeforeValidator(lambda v: v.strip().upper() if isinstance(v, str) else v)
]

class CloudProviderFilter(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    ALL = "ALL"


# Annotated type that accepts any case input
CloudProviderFilterType = Annotated[
    CloudProviderFilter,
    BeforeValidator(lambda v: v.strip().upper() if isinstance(v, str) else v)
]


# Portfolio Filter Schema
class PortfolioFilter(BaseModel):
    created_by: Optional[str] = None
    created_for: Optional[str] = None
    provider: Optional[CloudProviderFilterType] = None
    cloud_scp: Optional[str] = None
    is_billing_data: Optional[bool] = False
    name: Optional[str] = None

    # ⚡ Date Range
    created_at_from: Optional[datetime] = Field(
        None, description="Start date for created_at filter"
    )
    created_at_to: Optional[datetime] = Field(
        None, description="End date for created_at filter"
    )
    created_org: Optional[str] = None

    list_for: ListForType = ListFor.ALL
    recent_top: Optional[int] = None
    portfolio_id: Optional[str] = None

    list_all: bool = True

    # Pagination
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(10, ge=1, le=100, description="Items per page")
    app_name: Optional[str] = "CCA"
    user_email: Optional[str] = "testuser@infobellit.com"
    is_dummy_response: bool = True
    is_pagination: bool = True



class PortfolioLockRequest(BaseModel):
    is_locked: bool = Field(..., description="Set True to lock, False to unlock")
    app_name: Optional[str] = "CCA"
    user_email: Optional[str] = "testuser@infobellit.com"

