
import os
import json
import keyring
from google.oauth2 import service_account

SCOPES = [
    'https://www.googleapis.com/auth/drive',        # file upload/download
    'https://www.googleapis.com/auth/spreadsheets',  # Sheets API read/write
]

_UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))
_SERVICE_ACCOUNT_PATH = os.path.join(_UTILITIES_DIR, 'service_account.json')
_KEYRING_SERVICE = 'bos_gdrive'
_KEYRING_USERNAME = 'service_account'

def store_credentials_in_keyring(service_account_json_path):
    """
    Store service account credentials in KDE Keyring.
    
    Args:
        service_account_json_path: Path to the service account JSON file
    """
    if not os.path.exists(service_account_json_path):
        raise FileNotFoundError(f"Service account file not found: {service_account_json_path}")
    
    with open(service_account_json_path, 'r') as f:
        credentials_json = f.read()
    
    keyring.set_password(_KEYRING_SERVICE, _KEYRING_USERNAME, credentials_json)
    print(f"Credentials stored in KDE Keyring under service '{_KEYRING_SERVICE}'")

def authenticate_gdrive():
    """
    Authenticates using service account credentials.
    
    Priority:
    1. Try to load from KDE Keyring (secure)
    2. Fall back to service_account.json file
    
    The service account email must be shared on any Google Drive folders/Sheets
    that the app needs to access.
    """
    # Try loading from keyring first
    try:
        credentials_json = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
        if credentials_json:
            credentials_dict = json.loads(credentials_json)
            creds = service_account.Credentials.from_service_account_info(
                credentials_dict, scopes=SCOPES
            )
            print("✓ Loaded credentials from KDE Keyring")
            return creds
    except Exception as e:
        print(f"Could not load from keyring: {e}")
    
    # Fall back to file method
    if os.path.exists(_SERVICE_ACCOUNT_PATH):
        creds = service_account.Credentials.from_service_account_file(
            _SERVICE_ACCOUNT_PATH, scopes=SCOPES
        )
        print("✓ Loaded credentials from service_account.json file")
        print(f"💡 Consider storing credentials in keyring: python -c \"from utilities.gdrive_auth import store_credentials_in_keyring; store_credentials_in_keyring('{_SERVICE_ACCOUNT_PATH}')\"")
        return creds
    
    raise FileNotFoundError(
        f"No credentials found. Either:\n"
        f"1. Store in keyring: python -c \"from utilities.gdrive_auth import store_credentials_in_keyring; store_credentials_in_keyring('path/to/service_account.json')\"\n"
        f"2. Place service_account.json in {_UTILITIES_DIR}"
    )

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "store":
        if len(sys.argv) > 2:
            store_credentials_in_keyring(sys.argv[2])
        else:
            store_credentials_in_keyring(_SERVICE_ACCOUNT_PATH)
    else:
        user_creds = authenticate_gdrive()
        print("Google Drive authentication successful.")
