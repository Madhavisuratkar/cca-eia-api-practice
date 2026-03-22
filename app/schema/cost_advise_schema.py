from typing import List, Optional, Annotated, Union
from pydantic import BaseModel, Field
from app.utils.common_utils import RequiredFieldValidator
from enum import Enum
from typing_extensions import Literal


class CloudProvider(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"

class DataItem(RequiredFieldValidator):
    cloud_csp: CloudProvider = Field(
        ...,
        description="Cloud provider (e.g., AWS)",
        example="AWS"
    )
    region: str = Field(
        ...,
        description="Region of the instance",
        example="us-east-1"
    )
    instance_type: str = Field(
        ...,
        alias="instance type",
        description="Type of instance",
        example="m7i.xlarge"
    )
    quantity: float = Field(
        ...,
        description="Quantity of instances",
        example=3
    )
    monthly_utilization_hourly: float = Field(
        ...,
        alias="monthly utilization (hourly)",
        description="Monthly utilization in hours",
        example=720
    )
    pricingModel: str = Field(
        ...,
        description="Pricing model (e.g., ondemand)",
        example="ondemand"
    )
    uuid: Optional[str] = Field(
        None,
        description="Unique identifier for the instance",
        example="123e4567-e89b-12d3-a456-426614174000"
    )

    class Config:
        allow_population_by_field_name = True


class CostAdviseBase(RequiredFieldValidator):
    portfolioId: str = Field(..., description="Portfolio ID", example="portfolio_123")
    user_email: str = Field(..., description="User's email", example="user@example.com")
    headroom_percent: float = Field(
        20, alias="headroom%", description="Headroom percentage", example=15
    )


class CostAdviseRefetch(CostAdviseBase):
    is_refetch_recommendation: Literal[True] = Field(
        True, description="Flag to refetch recommendations"
    )

    class Config:
        schema_extra = {
            "example": {
                "portfolioId": "portfolio_123",
                "user_email": "user@example.com",
                "headroom%": 15,
                "is_refetch_recommendation": True
            }
        }


class CostAdvisePaginated(CostAdviseBase):
    is_refetch_recommendation: Literal[False] = Field(
        False, description="Flag to not refetch, so pagination is required"
    )
    page: int = Field(..., description="Page number", example=1)
    page_size: int = Field(..., description="Page size", example=20)

    class Config:
        schema_extra = {
            "example": {
                "portfolioId": "portfolio_123",
                "user_email": "user@example.com",
                "headroom%": 15,
                "is_refetch_recommendation": False,
                "page": 1,
                "page_size": 20
            }
        }


CostAdviseRequest = Annotated[
    Union[CostAdviseRefetch, CostAdvisePaginated],
    Field(discriminator="is_refetch_recommendation"),
]

class GetRecommendationsRequest(RequiredFieldValidator):
    portfolioName: str = Field(
        ...,
        description="Portfolio Name to get recommendation",
        example="My AWS Portfolio"
    )
    provider: CloudProvider = Field(
        ...,
        description="Cloud provider to get recommendation",
        example="AWS"
    )
    data: List[DataItem] = Field(
        ...,
        description="List of instance recommendation data",
        example=[
            {
                "cloud_csp": "AWS",
                "region": "us-east-1",
                "instance type": "m7i.xlarge",
                "quantity": 3,
                "monthly utilization (hourly)": 720,
                "pricingModel": "ondemand",
                "uuid": "123e4567-e89b-12d3-a456-426614174000"
            }
        ]
    )

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "portfolioName": "My AWS Portfolio",
                "provider": "AWS",
                "data": [
                    {
                        "cloud_csp": "AWS",
                        "region": "us-east-1",
                        "instance type": "m7i.xlarge",
                        "quantity": 3,
                        "monthly utilization (hourly)": 720,
                        "pricingModel": "ondemand",
                        "uuid": "123e4567-e89b-12d3-a456-426614174000"
                    }
                ],
                "user_email": "user@example.com",
                "page": 1,
                "page_size": 20
            }
        }
