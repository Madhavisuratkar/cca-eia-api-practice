import os
from dotenv import load_dotenv
from app.connections.secret_manger_config import extract_secret_section, parse_main_secret
from app.utils.constants import ENV_FILE_PATH, DEFAULT_ENV_PATH

# Load .env file (fallback if secret manager unavailable)
if os.path.exists(ENV_FILE_PATH):
    load_dotenv(ENV_FILE_PATH)
else:
    load_dotenv(DEFAULT_ENV_PATH)

# === Load from secret manager ===
try:
    parsed_secret = parse_main_secret()
    secret_data = extract_secret_section(parsed_secret, "cca_secrets")
    cs_secret_data = extract_secret_section(parsed_secret, "cs_secrets")
except Exception as e:
    print(f"[env_config] Warning: Failed to load secrets from secret manager. Using .env fallback. Error: {e}")
    secret_data = {}

# === Helper ===
def get_value(key: str):
    """Priority: secret_data → os.getenv"""
    return secret_data.get(key) or os.getenv(key)

def get_cs_value(key: str):
    """Priority: cs_secret_data → os.getenv"""
    return cs_secret_data.get(key) or os.getenv(key)

# === Core Config ===
MONGO_URI = get_value("MONGO_URI")
DATABASE_NAME = get_value("DATABASE_NAME")
FERNENT_KEY = get_value("FERNENT_KEY")
API_KEY = get_value("OPENAI_API_KEY")
CLIENT_ID = get_value("AZURE_CLIENT_ID")
CLIENT_SECRET = get_value("AZURE_CLIENT_SECRET")
TENANT_ID = get_value("AZURE_TENANT_ID")
jwt_secret_key  = get_value("JWT_SECRET_KEY")
db_name = get_value("ETL_DB_NAME")
db_user = get_value("ETL_DB_USER")
db_password = get_value("ETL_DB_PASSWORD")
db_host = get_value("ETL_DB_HOST")
db_port = get_value("ETL_DB_PORT")
marketplace_db = get_value("MARKETPLACE_DB_NAME")
AWS_ACCESS_KEY = get_value("AWS_ACCESS_KEY")
AWS_SECRET_KEY = get_value("AWS_SECRET_KEY")
AMD_TEST_USER_PASSWORD = get_value("AMD_TEST_USER_PASSWORD")
INFOBELL_TEST_USER_PASSWORD = get_value("INFOBELL_TEST_USER_PASSWORD")
EXAMPLE_TEST_USER_PASSWORD = get_value("EXAMPLE_TEST_USER_PASSWORD")

# === Remaining (from .env) ===
CS_URL = os.getenv('CS_URL')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')
GET_ENV = os.getenv('GET_ENV', 'DEV')
CS_UI = os.getenv('CS_UI')
CCA_UI = os.getenv('CCA_UI')
EIA_UI = os.getenv('EIA_UI')
SENDER = os.getenv("AZURE_SENDER_EMAIL")
TO_EMAIL = os.getenv("SMTP_HEALTH_ORGANISORS")
TEST_USER_EMAIL = os.getenv("TEST_USER_EMAIL")
AI_VALIDATION = os.getenv("AI_VALIDATIONS")
SONAR_URL = os.getenv("SONAR_URL")
JENKINS_URL = os.getenv("JENKINS_URL")

# ETL Configs
etl_s3_path = os.getenv('ETL_S3_PATH')
base_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = base_dir
ini_file = os.path.join(base_dir, 'config.ini')
log_file_dir_path = os.path.join(etl_s3_path, 'Logs')
etl_userdata = os.path.join(etl_s3_path, 'userdata')

results_path = os.getenv('RESULTS_PATH')
results_path_url = os.getenv('RESULTS_PATH_URL')
results_path_url_eia = os.getenv('RESULTS_PATH_URL_EIA')

# AWS Config
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')
MAIN_FOLDER = os.getenv('MAIN_FOLDER')

#CS database
cs_db_host = get_cs_value("DB_HOST")
cs_db_port = get_cs_value("DB_PORT")
cs_database = get_cs_value("DB_DATABASE")
cs_db_user = get_cs_value("DB_USERNAME")
cs_db_password = get_cs_value("DB_PASSWORD")

# Large file config
CHUNK_SIZE = int(os.getenv('BATCH_CHUNK_SIZE', '5000'))
LARGE_FILE_ROW_THRESHOLD = int(os.getenv('LARGE_FILE_ROW_THRESHOLD', '3000'))

app_env = os.getenv("APP_ENV", "dev").upper()

# Automation testing
emails = {
    "email_list": ["testuser@infobellit.com", "testuser@amd.com"]
    + [f"testuser{i}@infobellit.com" for i in range(1, 101)]
    + [f"testuser{i}@example.com" for i in range(1, 21)]
}
