from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class NotificationSchema(BaseModel):
    """
    Schema representing a notification document in the system.
    """
    id: Optional[str] = Field(None, alias="_id", description="Unique identifier of the notification")
    user_email: str = Field(..., description="Email of the user associated with the notification")
    app_name: str = Field(..., description="Application name, e.g., 'CCA' or 'EIA'")
    portfolio_id: Optional[str] = Field(None, description="Associated portfolio ID")
    portfolio_name: Optional[str] = Field(None, description="Portfolio name")
    job_id: Optional[str] = Field(None, description="Associated job ID")
    purpose: str = Field(..., description="Type of the notification, e.g., 'COST_ADVICE_COMPLETED'")
    message: str = Field(..., description="Notification message text")
    status: Optional[str] = Field("ACTIVE", description="Notification status, e.g., 'ACTIVE' or 'INACTIVE'")
    created_at: Optional[datetime] = Field(None, description="Timestamp when the notification was created")
    updated_at: Optional[datetime] = Field(None, description="Timestamp when the notification was last updated")
    is_seen: Optional[bool] = Field(False, description="Whether the notification has been viewed by the user")

    