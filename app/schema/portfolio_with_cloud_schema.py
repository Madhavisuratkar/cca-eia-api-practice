from typing import Optional, Literal, Union, Annotated, Any
from pydantic import Field, field_validator, BeforeValidator
from app.utils.common_utils import RequiredFieldValidator
from enum import Enum


class CloudProvider(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"

class CloudAccountBase(RequiredFieldValidator):
    provider: CloudProvider = Field(..., description="Cloud provider name", example="AWS")
    region: str = Field(..., description="Cloud region", example="us-east-1")
    accountName: Optional[str] = Field(None, description="Account name", example="myAccount")
    user_email: str = Field(..., description="User email", example="user@example.com")
    policy_engine: Optional[str] = Field(None, description="Policy engine for recommendations", example="global")


class AWSAccount(CloudAccountBase):
    provider: Literal["AWS", "aws"] = Field("AWS", description="Cloud provider", example="AWS")
    awsAccessId: str = Field(..., description="AWS Access Key ID", example="AKIA......")
    awsAccessSecret: str = Field(..., description="AWS Secret Access Key", example="**********")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v



class AzureAccount(CloudAccountBase):
    provider: Literal["AZURE", "azure"] = Field("AZURE", description="Cloud provider", example="AZURE")
    azureClientId: str = Field(..., description="Azure Client ID", example="azure-client-id")
    azureClientSecret: str = Field(..., description="Azure Client Secret", example="azure-client-secret")
    azureTenantId: str = Field(..., description="Azure Tenant ID", example="azure-tenant-id")
    azureSubscriptionId: str = Field(..., description="Azure Subscription ID", example="azure-subscription-id")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v



class GCPAccount(CloudAccountBase):
    provider: Literal["GCP", "gcp"] = Field("GCP", description="Cloud provider", example="GCP")
    project_id: str = Field(..., description="GCP Project ID", example="gcp-project-id")
    private_key: str = Field(..., description="GCP Private Key", example="private-key-content")
    client_email: str = Field(..., description="GCP Client Email", example="gcp-client-email@example.com")
    client_id: str = Field(..., description="GCP Client ID", example="gcp-client-id")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v




def text_pre_validator(v: Any) -> Any:
    if isinstance(v, dict) and "provider" in v and isinstance(v["provider"], str):
        v["provider"] = v["provider"].upper()
    return v

CloudAccountSchema = Annotated[
    Union[AWSAccount, AzureAccount, GCPAccount],
    Field(discriminator="provider"),
    BeforeValidator(text_pre_validator)
]


class GetCloudAccountQuery(RequiredFieldValidator):
    accountName: str = Field(..., alias="accountName", description="Name of the cloud account", example="myAccount")
    provider: CloudProvider = Field(..., alias="provider", description="Cloud provider", example="AWS")
    user_email: Optional[str] = Field(None, description="User email", example="user@example.com")

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "accountName": "myAccount",
                "provider": "AWS",
                "user_email": "user@example.com"
            }
        }


class QueryTypeCloudCred(str, Enum):
    get_account = "get_account"
    sync_account = "sync_account"
    test_account = "test_account"