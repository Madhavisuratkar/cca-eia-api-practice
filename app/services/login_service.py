import base64
from datetime import datetime, timedelta, timezone
from jwt import encode
from app.connections.env_config import emails, jwt_secret_key, TEST_USER_EMAIL, AMD_TEST_USER_PASSWORD, INFOBELL_TEST_USER_PASSWORD, EXAMPLE_TEST_USER_PASSWORD
from app.connections.custom_exceptions import CustomAPIException
from app.connections.pylogger import log_message
from app.utils.constants import LevelType

def generate_jwt(user_details):
    required_fields = ['email']
    if not all(field in user_details for field in required_fields):
        log_message(LevelType.ERROR, "Missing required user details", ErrorCode=-1)
        raise ValueError("Missing required user details")
    payload = {
        'sub': user_details['email'],
        'custom_token': 'True',
        'exp': datetime.now(timezone.utc) + timedelta(days=3)
    }
    try:
        jwt_token = encode(payload, jwt_secret_key)
        return jwt_token
    except Exception as err:
        log_message(LevelType.ERROR, f"JWT generation error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=500, message=f"JWT generation error: {str(err)}", error_code=-1)

def decode_data(data):
    try:
        data_dec = base64.b64decode(data).decode()
        return data_dec
    except Exception as err:
        log_message(LevelType.ERROR, f"Decoding error: {str(err)}", ErrorCode=-1)
        raise CustomAPIException(status_code=400, message=f"Decoding error: {str(err)}", error_code=-1)

def login_user_service(data: dict):
    """"""
    try:

        email = data['email'].strip().lower()
        password = data['password'].strip()
        if email not in emails.get("email_list", []):
            log_message(LevelType.ERROR, "Incorrect email.", ErrorCode=-1)
            raise CustomAPIException(status_code=404, message="Incorrect email.", error_code=-1)

        expected_password = None
        if email.startswith("testuser") and email.endswith("@infobellit.com"):
            expected_password = INFOBELL_TEST_USER_PASSWORD
        elif email.endswith("@example.com"):
            expected_password = EXAMPLE_TEST_USER_PASSWORD
        elif email == TEST_USER_EMAIL:
            expected_password = AMD_TEST_USER_PASSWORD
        else:
            log_message(LevelType.ERROR, "Unauthorized email.", ErrorCode=-1)
            raise CustomAPIException(status_code=403, message="Unauthorized email.", error_code=-1)

        decoded_password = decode_data(password)
        if decoded_password != expected_password:
            log_message(LevelType.ERROR, "Incorrect password.", ErrorCode=-1)
            raise CustomAPIException(status_code=404, message="Incorrect password.", error_code=-1)

        jwt_token = generate_jwt(data)
        if not jwt_token:
            log_message(LevelType.ERROR, "Failed to generate JWT token.", ErrorCode=-1)
            raise CustomAPIException(status_code=500, message="Failed to generate JWT token.", error_code=-1)

        return {
            "Message": "Login Successful",
            "email": email,
            "ErrorCode": 1,
            "jwtToken": jwt_token
        }
    except CustomAPIException:
        raise
    except Exception as err:
        log_message(LevelType.ERROR, f"error in login_user_service for user : {data}. err : {str(err)}")
        raise CustomAPIException(status_code=500, message="Unable to login", error_code=-1)
