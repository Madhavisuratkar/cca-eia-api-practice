from fastapi import HTTPException

class CustomAPIException(HTTPException):
    def __init__(self, status_code: int = 400, message: str = "Something went wrong", error_code: int = -1, data: dict | None = None):
        # Base detail
        detail = {
            "Message": message,
            "ErrorCode": error_code
        }

        # Merge data dict into detail if provided
        if data:
            detail.update(data)

        super().__init__(
            status_code=status_code,
            detail=detail
        )