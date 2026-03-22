import base64
import boto3
import json
from typing import Any, Dict
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os
import base64
import hashlib
from app.utils.constants import ENV_FILE_PATH, DEFAULT_ENV_PATH
from dotenv import load_dotenv


if os.path.exists(ENV_FILE_PATH):
    load_dotenv(ENV_FILE_PATH)
else:
    load_dotenv(DEFAULT_ENV_PATH)

fernet_access_key = os.getenv('FERNET_ACCESS_KEY')
fernet_secret_key = os.getenv('FERNET_SECRET_KEY')
fernet_region_name = os.getenv('FERNET_REGION_NAME')
fernet_secret_name = os.getenv('FERNET_SECRET_NAME')


key = base64.urlsafe_b64encode(hashlib.sha256("zenitsuagatsuma".encode()).digest())
fernet = Fernet(key)

AWS_ACCESS_KEY = fernet.decrypt(fernet_access_key).decode()
AWS_SECRET_KEY = fernet.decrypt(fernet_secret_key).decode()

REGION_NAME = fernet.decrypt(fernet_region_name).decode()
SECRET_NAME = fernet.decrypt(fernet_secret_name).decode()


secrets_client = boto3.client(
        "secretsmanager",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )


def fetch_secret_value(secret_name: str) -> str:
    """Fetch raw secret value (SecretString) from AWS Secrets Manager."""
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = response.get("SecretString")
    if not secret_str:
        raise ValueError("No SecretString found in AWS Secrets Manager response")
    return secret_str

def parse_main_secret() -> Dict[str, Any]:
    """Parse the main secret and return its nested JSON."""
    secret_str = fetch_secret_value(SECRET_NAME)
    main_secret = json.loads(secret_str)
    if "secrets" not in main_secret:
        raise ValueError("'secrets' key not found in main secret payload")
    return json.loads(main_secret["secrets"])

def extract_secret_section(parsed_secret: Dict[str, Any], section_key: str) -> Dict[str, Any]:
    """Extract a specific section (like 'cs_secrets') from parsed secret."""
    section = parsed_secret.get(section_key)
    if not section:
        raise ValueError(f"'{section_key}' not found in secret payload")
    return section