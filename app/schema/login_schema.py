from pydantic import EmailStr, Field
from app.utils.common_utils import RequiredFieldValidator


class LoginRequest(RequiredFieldValidator):
    email: EmailStr = Field(
        ...,
        description="Email for CCA / EIA authentication",
        example="user@example.com"
    )
    password: str = Field(
        ...,
        min_length=1,
        description="Password for CCA / EIA authentication",
        example="*****************"
    )
