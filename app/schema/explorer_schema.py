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

# Your models
class RegionsQueryModel(CaseInsensitiveProviderMixin):
    pass

class InstanceSizesQueryModel(CaseInsensitiveProviderMixin):
    region: str

class ExplorerQueryModel(CaseInsensitiveProviderMixin):
    region: Optional[str] = None

class InstanceSummaryQueryModel(CaseInsensitiveProviderMixin):
    pass

class CloudInstancesQueryModel(CaseInsensitiveProviderMixin):
    pass

# Dependency functions
def regions_query(
    provider: str = Query(..., example="AWS", description="Cloud provider (AWS/AZURE/GCP)")
) -> RegionsQueryModel:
    try:
        return RegionsQueryModel(provider=provider)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid inputs provided {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid inputs provided ")

def instance_sizes_query(
    provider: str = Query(..., example="AWS", description="Cloud provider (AWS/AZURE/GCP)"),
    region: str = Query(..., example="us-east-1", description="Region")
) -> InstanceSizesQueryModel:
    try:
        return InstanceSizesQueryModel(provider=provider, region=region)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid inputs provided {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid inputs provided ")

def explorer_query(
    provider: str = Query(..., example="AWS", description="Cloud provider (AWS/AZURE/GCP)"),
    region: Optional[str] = Query(None, description="Region", example="us-east-1")
) -> ExplorerQueryModel:
    try:
        return ExplorerQueryModel(provider=provider, region=region)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid inputs provided {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid inputs provided ")

def instance_summary_query(
    provider: str = Query(..., example="AWS", description="Cloud provider (AWS/AZURE/GCP)")
) -> InstanceSummaryQueryModel:
    try:
        return InstanceSummaryQueryModel(provider=provider)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid inputs provided {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid inputs provided ")

def cloud_instances_query(
    provider: str = Query(..., example="AWS", description="Cloud provider (AWS/AZURE/GCP)")
) -> CloudInstancesQueryModel:
    try:
        return CloudInstancesQueryModel(provider=provider)
    except ValidationError as err:
        log_message(LevelType.ERROR, f"Invalid inputs provided {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message="Invalid inputs provided ")
