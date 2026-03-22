from cryptography.fernet import Fernet
from app.connections.env_config import FERNENT_KEY

fernet = Fernet(FERNENT_KEY)

def encrypt_dict(data: dict, parent_key: str = "") -> dict:
    """Encrypt values in a dict, with skip rules and nested dict support"""
    encrypted = {}

    # Global skip list
    skip_list = ["provider", "region", "accountName", "cloud_csp"]

    # Special skip list only for service_account_key_data
    service_account_skip = ["project_id", "client_email", "universe_domain"]

    for k, v in data.items():
        if isinstance(v, dict):
            # Recursively encrypt nested dicts
            encrypted[k] = encrypt_dict(v, parent_key=k)

        elif isinstance(v, str):
            # Apply skip rules
            if k in skip_list:
                encrypted[k] = v
            elif parent_key == "service_account_key_data" and k in service_account_skip:
                encrypted[k] = v
            else:
                encrypted[k] = fernet.encrypt(v.encode()).decode()
        else:
            encrypted[k] = v

    return encrypted


def decrypt_dict(data: dict, parent_key: str = "") -> dict:
    """Decrypt values in a dict, with skip rules and nested dict support"""
    decrypted = {}

    # Global skip list (never decrypt these)
    skip_list = ["provider", "region", "accountName", "cloud_csp"]

    # Special skip list only for service_account_key_data
    service_account_skip = ["project_id", "client_email", "universe_domain"]

    for k, v in data.items():
        if isinstance(v, dict):
            # Recursively decrypt nested dicts
            decrypted[k] = decrypt_dict(v, parent_key=k)

        elif isinstance(v, str):
            # Apply skip rules
            if k in skip_list:
                decrypted[k] = v
            elif parent_key == "service_account_key_data" and k in service_account_skip:
                decrypted[k] = v
            else:
                try:
                    decrypted[k] = fernet.decrypt(v.encode()).decode()
                except Exception:
                    decrypted[k] = v
        else:
            decrypted[k] = v

    return decrypted
