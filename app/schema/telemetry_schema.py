from typing import List, Union, Annotated, Optional
from enum import Enum
from pydantic import Field, field_validator
from app.utils.common_utils import RequiredFieldValidator
from app.utils.constants import DATADOG_URL
from app.connections.custom_exceptions import CustomAPIException

class CloudProvider(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"


class TelemetrySource(str, Enum):
    datadog = "datadog"
    prometheus = "prometheus"
    cloudwatch = "cloudwatch"
    azureinsights = "azureinsights"
    gcptelemetry = "gcptelemetry"


class BaseTelemetryParams(RequiredFieldValidator):
    provider: str = Field(..., description="Cloud provider")
    region: List[str] = Field(..., min_items=1, description="List of cloud regions",example=["us-east-1"])
    testFlag: bool = Field(..., description="Flag for test mode",example=False)


class BaseTelemetryParamsWithInstances(RequiredFieldValidator):
    provider: str = Field(..., description="Cloud provider")
    region: List[str] = Field(..., min_items=1, description="List of cloud regions",example=["us-east-1"])
    instances: List[str] = Field(..., min_items=1, description="Required list of instances",example=["m7i.xlarge"])
    name: str = Field(..., description="Metric name or identifier", example="my_portfolio")
    policy_engine: Optional[str] = Field(None, description="Policy engine for recommendations", example="global")


class DatadogQuery(BaseTelemetryParams):
    # sourceType: Literal["datadog"] = Field("datadog", description="Telemetry source type")
    apiKey: str = Field(..., description="Datadog API key", example="AKIAxxxxxxxxxxxxxxxx")
    appKey: str = Field(..., description="Datadog APP key", example="abcd1234efgh5678ijkl")
    apiHost: str = Field(..., description="Datadog API host URL", example=DATADOG_URL)

    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "datadog",
                "region": ["us-west-2"],
                "testFlag": False,
                "apiKey": "AKIAxxxxxxxxxxxxxxxx",
                "appKey": "abcd1234efgh5678ijkl",
                "apiHost": DATADOG_URL
            }
        }


class PrometheusQuery(BaseTelemetryParams):
    # sourceType: Literal["prometheus"] = Field("prometheus", description="Telemetry source type")
    prometheus_url: str = Field(..., description="Prometheus server URL", example="<Prometheus server URL>")
    username: str = Field(..., description="Prometheus server Username", example="prometheus_user")
    password: str = Field(..., description="Prometheus server Password", example="***")

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

    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "prometheus",
                "region": ["us-east-1"],
                "testFlag": True,
                "prometheus_url": "< Prometheus server URL >",
                "username": "prometheus_user",
                "passsword": "**********"
            }
        }


class CloudwatchQuery(BaseTelemetryParams):
    # sourceType: Literal["cloudwatch"] = Field("cloudwatch", description="Telemetry source type")
    aws_access_key_id: str = Field(..., description="AWS Access Key ID", example="ABC")
    aws_secret_access_key: str = Field(..., description="AWS Secret Access Key", example="ABC")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.AWS:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be AWS",
                error_code=-1
            )
        return v



    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "cloudwatch",
                "region": ["us-east-1"],
                "testFlag": False,
                "aws_access_key_id": "ABC",
                "aws_secret_access_key": "ABC"
            }
        }


class AzureInsightsQuery(BaseTelemetryParams):
    # sourceType: Literal["azureinsights"] = Field("azureinsights", description="Telemetry source type")
    tenant_id: str = Field(..., description="Azure Tenant ID", example="tenant-id-xxxxxx")
    client_id: str = Field(..., description="Azure Client ID", example="client-id-xxxxxx")
    client_secret: str = Field(..., description="Azure Client Secret", example="client-secret-xxxxxx")
    subscription_id: str = Field(..., description="Azure Subscription ID", example="subscription-id-xxxxxx")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.AZURE:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be AZURE",
                error_code=-1
            )
        return v
    class Config:
        schema_extra = {
            "example": {
                "provider": "AZURE",
                "sourceType": "azureinsights",
                "region": ["eastus"],
                "testFlag": True,
                "tenant_id": "tenant-id-xxxxxx",
                "client_id": "client-id-xxxxxx",
                "client_secret": "client-secret-xxxxxx",
                "subscription_id": "subscription-id-xxxxxx"
            }
        }


class GCPQuery(BaseTelemetryParams):
    # sourceType: Literal["gcptelemetry"] = Field("gcptelemetry", description="Telemetry source type")
    private_key: str = Field(..., description="GCP Private Key", example="-----BEGIN PRIVATE KEY------\n...")
    client_email: str = Field(..., description="GCP Client Email", example="client-email@example.com")
    project_id: str = Field(..., description="GCP Project ID", example="gcp-project-123")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.GCP:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be GCP",
                error_code=-1
            )
        return v
    class Config:
        schema_extra = {
            "example": {
                "provider": "GCP",
                "sourceType": "gcptelemetry",
                "region": ["us-central1"],
                "testFlag": False,
                "private_key": "_------BEGIN PRIVATE KEY-----\n...",
                "client_email": "client-email1@example.com",
                "project_id": "gcp-project-123"
            }
        }


class DatadogQueryWithInstances(BaseTelemetryParamsWithInstances):
    # sourceType: Literal["datadog"] = Field("datadog", description="Telemetry source type")
    apiKey: str = Field(..., description="Datadog API key", example="AKIAxxxxxxxxxxxxxxxx")
    appKey: str = Field(..., description="Datadog APP key", example="abcd1234efgh5678ijkl")
    apiHost: str = Field(..., description="Datadog API host URL", example=DATADOG_URL)

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v


    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "datadog",
                "region": ["us-west-2"],
                "instances": ["i-1234567890abcdef0"],
                "name": "cpu.usage",
                "apiKey": "AKIAxxxxxxxxxxxxxxxx",
                "appKey": "abcd1234efgh5678ijkl",
                "apiHost": DATADOG_URL
            }
        }


class PrometheusQueryWithInstances(BaseTelemetryParamsWithInstances):
    # sourceType: Literal["prometheus"] = Field("prometheus", description="Telemetry source type")
    prometheus_url: str = Field(..., description=" Prometheus server URL ", example=" <Prometheus server URL> ")
    username: str = Field(..., description="Prometheus server Username", example="prometheus_user")
    password: str = Field(..., description="Prometheus server Password", example="********")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
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

    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "prometheus",
                "region": ["us-east-1"],
                "instances": ["instance-1", "instance-2"],
                "name": "http_requests_total",
                "prometheus_url": "  <Prometheus server URL>  ",
                "username": "prometheus_user",
                "passsword": "**********"
            }
        }


class CloudwatchQueryWithInstances(BaseTelemetryParamsWithInstances):
    # sourceType: Literal["cloudwatch"] = Field("cloudwatch", description="Telemetry source type")
    aws_access_key_id: str = Field(..., description="AWS Access Key ID", example="ABC")
    aws_secret_access_key: str = Field(..., description="AWS Secret Access Key", example="ABC")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.AWS:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be AWS",
                error_code=-1
            )
        return v



    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("created_for", mode="before")
    def normalize_created_for(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().lower()
            return v


    class Config:
        schema_extra = {
            "example": {
                "provider": "AWS",
                "sourceType": "cloudwatch",
                "region": ["us-east-1"],
                "instances": ["i-1234567890abcdef0"],
                "name": "NetworkIn",
                "aws_access_key_id": "ABC",
                "aws_secret_access_key": "ABC"
            }
        }


class AzureInsightsQueryWithInstances(BaseTelemetryParamsWithInstances):
    # sourceType: Literal["azureinsights"] = Field("azureinsights", description="Telemetry source type")
    tenant_id: str = Field(..., description="Azure Tenant ID", example="tenant-id-xxxxxx")
    client_id: str = Field(..., description="Azure Client ID", example="client-id-xxxxxx")
    client_secret: str = Field(..., description="Azure Client Secret", example="client-secret-xxxxxx")
    subscription_id: str = Field(..., description="Azure Subscription ID", example="subscription-id-xxxxxx")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.AZURE:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be AZURE",
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

    class Config:
        schema_extra = {
            "example": {
                "provider": "AZURE",
                "sourceType": "azureinsights",
                "region": ["eastus"],
                "instances": ["vm-instance-1"],
                "name": "Percentage CPU",
                "tenant_id": "tenant-id-xxxxxx",
                "client_id": "client-id-xxxxxx",
                "client_secret": "client-secret-xxxxxx",
                "subscription_id": "subscription-id-xxxxxx"
            }
        }


class GCPQueryWithInstances(BaseTelemetryParamsWithInstances):
    # sourceType: Literal["gcptelemetry"] = Field("gcptelemetry", description="Telemetry source type")
    private_key: str = Field(..., description="GCP Private Key", example="----BEGIN PRIVATE KEY-----\n...")
    client_email: str = Field(..., description="GCP Client Email", example="client-email2@example.com")
    project_id: str = Field(..., description="GCP Project ID", example="gcp-project-123")

    created_for: Optional[str] = Field(None, description="created for organization")

    @field_validator("provider", mode="before")
    def normalize_provider(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            v = v.strip().upper()
        if v != CloudProvider.GCP:
            raise CustomAPIException(
                status_code=422,
                message=f"Invalid provider: {v}. Must be GCP",
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


    class Config:
        schema_extra = {
            "example": {
                "provider": "GCP",
                "sourceType": "gcptelemetry",
                "region": ["us-central1"],
                "instances": ["instance-1"],
                "name": "cpu_usage_total",
                "private_key": "-----BEGIN PRIVATE KEY__-----\n...",
                "client_email": "client-email3@example.com",
                "project_id": "gcp-project-123"
            }
        }


TELEMETRY_CONNECTION_SCHEMAS = {
    TelemetrySource.datadog: DatadogQuery,
    TelemetrySource.prometheus: PrometheusQuery,
    TelemetrySource.cloudwatch: CloudwatchQuery,
    TelemetrySource.azureinsights: AzureInsightsQuery,
    TelemetrySource.gcptelemetry: GCPQuery,
}

TELEMETRY_METRICS_SCHEMAS = {
    TelemetrySource.datadog: DatadogQueryWithInstances,
    TelemetrySource.prometheus: PrometheusQueryWithInstances,
    TelemetrySource.cloudwatch: CloudwatchQueryWithInstances,
    TelemetrySource.azureinsights: AzureInsightsQueryWithInstances,
    TelemetrySource.gcptelemetry: GCPQueryWithInstances,
}


# Union of all connection schemas with discriminator
TelemetryConnectionBody = Annotated[
    Union[
        DatadogQuery,
        PrometheusQuery,
        CloudwatchQuery,
        AzureInsightsQuery,
        GCPQuery,
    ],
    Field(discriminator="sourceType"),
]

# Union of all metrics schemas with discriminator
TelemetryMetricsBody = Annotated[
    Union[
        DatadogQueryWithInstances,
        PrometheusQueryWithInstances,
        CloudwatchQueryWithInstances,
        AzureInsightsQueryWithInstances,
        GCPQueryWithInstances,
    ],
    Field(discriminator="sourceType"),
]
