import requests
from msal import ConfidentialClientApplication
import json
from app.connections.env_config import CLIENT_ID, CLIENT_SECRET, TENANT_ID, SENDER
from app.connections.pylogger import log_message
from app.utils.constants import LevelType
import asyncio

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_ENDPOINT = f"https://graph.microsoft.com/v1.0/users/{SENDER}/sendMail"

def get_access_token():
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    token_response = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in token_response:
        raise Exception(f"Failed to get token: {token_response.get('error_description')}")
    return token_response['access_token']

async def send_email(subject, to_email, body, content_type="HTML"):
    try:
        await asyncio.sleep(0)
        token = get_access_token()
        if isinstance(to_email, str):
            recipients = [email.strip() for email in to_email.split(",")]
        else:
            recipients = to_email

        if to_email:
            message_payload = {
                "message": {
                    "subject": subject,
                    "body": {
                        "contentType": content_type,
                        "content": body
                    },
                    "toRecipients": [
                        {"emailAddress": {"address": email}} for email in recipients
                    ]
                },
                "saveToSentItems": "false"
            }
        else:
            log_message(LevelType.ERROR, f"Exception during email send: {e}", ErrorCode=-1)
            return f"No user emails specified to send: {e}", False        

        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        response = requests.post(GRAPH_ENDPOINT, headers=headers, data=json.dumps(message_payload))

        if response.status_code == 202:
            log_message(LevelType.INFO, f"Email sent to {to_email}", ErrorCode=1)
            return f"Email sent to {to_email}", True
        else:
            log_message(LevelType.ERROR, f"Failed to send email: {response.text}", ErrorCode=-1)
            return f"Failed to send email: {response.text}", False

    except Exception as e:
        log_message(LevelType.ERROR, f"Exception during email send: {e}", ErrorCode=-1)
        return f"Exception during email send: {e}", False