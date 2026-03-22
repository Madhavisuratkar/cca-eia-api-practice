from pydantic import BaseModel, Field
from typing import Optional

class InsightsRequest(BaseModel):
    insight: str = Field(..., description="Insight type")
    app_name: Optional[str] = Field(None, description="Application name")
    providers: Optional[str] = Field(None, description="Comma-separated cloud providers (for Service Provider insight)")
    clients: Optional[str] = Field(None, description="Comma-separated client names (for Client insight)")
    portfolio_ids: Optional[str] = Field(None, description="Comma-separated portfolio IDs (for Portfolios insight)")
    user_email: Optional[str] = Field(None, description="User email to filter by user's organizations")
    
    

    def get_filter_list(self) -> Optional[list]:
        """Get the appropriate filter list based on insight type"""
        if self.insight == "Service Provider":
            return self.providers.split(',') if self.providers else None
        elif self.insight == "Client":
            return self.clients.split(',') if self.clients else None
        elif self.insight == "Portfolios":
            return self.portfolio_ids.split(',') if self.portfolio_ids else None
        return ["all"]

