
import os
import json
import keyring
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

SCOPES = [
    'https://www.googleapis.com/auth/drive',        # file upload/download
    'https://www.googleapis.com/auth/spreadsheets',  # Sheets API read/write
]

_UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))
_service_account_PATH = os.path.join(_UTILITIES_DIR, 'bos_service_account.json')
_KEYRING_SERVICE = 'bos_gdrive'
_KEYRING_USERNAME = 'bos_service_account'


def _describe_keyring_backend():
    """Return a compact description of the active keyring backend."""
    backend = keyring.get_keyring()
    backend_name = backend.__class__.__name__
    backend_module = backend.__class__.__module__
    return f"{backend_module}.{backend_name}"

def store_credentials_in_keyring(bos_service_account_json_path):
    """
    Store service account credentials in the system keyring.
    
    Args:
        bos_service_account_json_path: Path to the service account JSON file
    """
    if not os.path.exists(bos_service_account_json_path):
        raise FileNotFoundError(f"Service account file not found: {bos_service_account_json_path}")
    
    with open(bos_service_account_json_path, 'r', encoding='utf-8') as f:
        credentials_json = f.read()
    
    keyring.set_password(_KEYRING_SERVICE, _KEYRING_USERNAME, credentials_json)
    print(f"Credentials stored in system keyring under service '{_KEYRING_SERVICE}'")


def authenticate_gdrive():
    """
    Authenticate for Google Drive API using OAuth (preferred for My Drive) or service account (for Shared Drives).
    Priority:
    1. Try OAuth user credentials (token.pickle)
    2. Try service account from keyring
    3. Try bos_service_account.json file
    """
    # Try OAuth first
    creds = None
    token_path = os.path.join(_UTILITIES_DIR, 'token.pickle')
    client_secrets_path = os.path.join(_UTILITIES_DIR, 'client_secrets.json')
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    # If no valid creds, do OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        elif os.path.exists(client_secrets_path):
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        if creds:
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
    if creds and creds.valid:
        print("✓ Loaded OAuth user credentials (token.pickle)")
        return creds

    # Fallback: service account (for Shared Drives only)
    print(f"Using keyring backend: {_describe_keyring_backend()}")
    try:
        credentials_json = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
        if credentials_json:
            credentials_dict = json.loads(credentials_json)
            from google.oauth2.service_account import Credentials as ServiceAccountCredentials
            creds = ServiceAccountCredentials.from_service_account_info(
                credentials_dict, scopes=SCOPES
            )
            print("✓ Loaded credentials from system keyring")
            return creds
    except Exception as e:
        print(f"Could not load from keyring: {e}")
    if os.path.exists(_service_account_PATH):
        from google.oauth2.service_account import Credentials as ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_service_account_file(
            _service_account_PATH, scopes=SCOPES
        )
        print("✓ Loaded credentials from bos_service_account.json file")
        print(f"💡 Consider storing credentials in keyring: python -c \"from utilities.gdrive_auth import store_credentials_in_keyring; store_credentials_in_keyring('{_service_account_PATH}')\"")
        return creds
    raise FileNotFoundError(
        f"No credentials found. Either:\n"
        f"1. Complete OAuth flow (place client_secrets.json in {_UTILITIES_DIR})\n"
        f"2. Store service account in keyring or bos_service_account.json in {_UTILITIES_DIR}"
    )

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "store":
        if len(sys.argv) > 2:
            store_credentials_in_keyring(sys.argv[2])
        else:
            store_credentials_in_keyring(_service_account_PATH)
    else:
        user_creds = authenticate_gdrive()
        print("Google Drive authentication successful.")
