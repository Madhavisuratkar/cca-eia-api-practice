import asyncio
import io
import boto3
import asyncio
from app.connections.custom_exceptions import CustomAPIException
import aiohttp
from starlette.datastructures import UploadFile as StarletteUploadFile
from app.connections.env_config import AWS_ACCESS_KEY, AWS_REGION, AWS_SECRET_KEY, BUCKET_NAME, MAIN_FOLDER
from app.connections.pylogger import log_message
from app.utils.constants import LevelType

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)


def build_s3_key(app_name: str, user_name: str, file_name: str, sub_folder : str) -> str:
    """
    Build S3 object key in format:
    main_folder/app_name/user_name/input/<file_name>_input
    """
    return f"{MAIN_FOLDER}/{app_name}/{user_name}/{sub_folder}/{file_name}"


def generate_upload_presigned_url(app_name: str, user_name: str, file_name: str, sub_folder: str, file_type, expires_in: int = 360):
    """
    Generate presigned URL for uploading a file.
    Returns both URL and object key.
    """
    s3_key = build_s3_key(app_name, user_name, file_name, sub_folder)

    url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": BUCKET_NAME, "Key": s3_key, "ContentType": file_type },
        ExpiresIn=expires_in,
    )
    return url, s3_key

def email_to_name_org(value):
    # Return as-is if not a usable string
    if not isinstance(value, str) or not value.strip():
        return value
    # Quick structural guard: exactly one '@' and characters on both sides
    local, sep, domain = value.strip().partition("@")
    if sep != "@" or not local or not domain:
        return value
    # Take organization as the part before first dot; fallback to entire domain
    org = domain.split(".", 1)[0] or domain
    # Build name_organization
    return f"{local}_{org}"


def generate_download_presigned_url(s3_key: str, expires_in: int = 3600):
    """
    Generate presigned URL for downloading a file.
    """
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": s3_key},
        ExpiresIn=expires_in,
    )
    return url


async def put_to_presigned_url(put_url: str, content: bytes, content_type: str):
    async with aiohttp.ClientSession() as session:
        async with session.put(put_url, data=content, headers={"Content-Type": content_type}) as resp:
            if resp.status not in (200, 201):
                text = await resp.text()
                raise CustomAPIException(status_code=400, message=f"S3 upload failed: {resp.status} {text}", error_code=-1)

async def upload_csv_to_s3(csv_bytes: bytes, app_name: str, user_email: str, file_name: str, folder : str) -> str:
    """
    Upload CSV bytes to S3 asynchronously using a thread pool.
    Returns the S3 key.
    """
    s3_key = build_s3_key(app_name, user_email, file_name, folder)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_bytes,
            ContentType="text/csv"
        )
    )
    return s3_key

async def upload_dataframe_to_s3(df, app_name: str, user_name: str, file_name: str, folder : str):
    """
    Upload a pandas DataFrame to S3 as a CSV
    """
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_buffer.seek(0)
    await asyncio.sleep(0)
    s3_key = build_s3_key(app_name, user_name, file_name, folder)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    return s3_key


async def check_file_exists_in_s3(s3_key: str) -> bool:
    """
    Check if a file exists in S3.
    Returns True if exists, False if not found.
    Raises CustomAPIException for other S3 errors.
    """
    loop = asyncio.get_running_loop()

    def head_object():
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            return True
        except s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == "404":
                return False  # File does not exist
            # For any other error, raise a CustomAPIException
            raise CustomAPIException(
                status_code=500,
                message=f"Error checking S3 file '{s3_key}': {e}",
                error_code=-1
            )

    return await loop.run_in_executor(None, head_object)


def read_data_s3(s3_key):
    """"""
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    return obj


def fetch_s3_file(key):
    """"""
    if not key:
        return None
    s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    file_bytes = s3_obj["Body"].read()
    filename = key.split("/")[-1]
    return StarletteUploadFile(file=io.BytesIO(file_bytes), filename=filename)


async def upload_file_to_s3(file_bytes: bytes, app_name: str, user_email: str, file_name: str, sub_folder: str) -> str:
        """
        Upload file bytes to S3 synchronously.
        Returns the S3 key.
        """
        user_name = email_to_name_org(user_email)
        s3_key = build_s3_key(app_name, user_name, file_name, sub_folder)
        await asyncio.sleep(0)
        
        # Determine the correct ContentType based on file extension
        content_type = "application/octet-stream"  # default
        if file_name.lower().endswith('.xlsx'):
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif file_name.lower().endswith('.pptx'):
            content_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        elif file_name.lower().endswith('.csv'):
            content_type = "text/csv"
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=file_bytes,
                ContentType=content_type
            )
            return s3_key
        except Exception as e:
            log_message(LevelType.ERROR, f"Error uploading to S3:  '{s3_key}': {e}", ErrorCode=-1)
            raise CustomAPIException(
                status_code=500,
                message="Error while saving recommondations",
                error_code=-1
            )