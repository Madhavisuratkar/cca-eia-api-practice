from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import List

class AddSalesClientSchema(BaseModel):
    client_names: List[str] = Field(..., description="List of client names")
    user_email: EmailStr = Field(..., description="Email of the user adding the client")

    @field_validator("client_names", mode="before")
    def validate_client_names(cls, v):
        if not v:
            raise ValueError("client_names cannot be empty")

        if isinstance(v, str):
            raise ValueError("client_names must be a list of strings")

        cleaned = []
        for name in v:
            if not isinstance(name, str):
                raise ValueError("Each client name must be a string")

            name = name.strip()
            if not name:
                raise ValueError("Client name cannot be empty")
            
            cleaned.append(name)

        return cleaned

    @field_validator("user_email", mode="before")
    def strip_email(cls, v):
        if isinstance(v, str):
            v = v.strip()
        if not v:
            raise ValueError("user_email cannot be empty")
        return v