import os
import json
import keyring
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import pickle

from utilities.auth_logger import auth_ok, auth_info, auth_warn, auth_critical  # ← added

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets',
]

_UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))
_SERVICE_ACCOUNT_PATHS = [
    os.path.join(_UTILITIES_DIR, 'bos_service_account.json'),
    os.path.join(_UTILITIES_DIR, 'service_account.json'),
]
_KEYRING_SERVICE   = 'bos_gdrive'
_KEYRING_USERNAME  = 'bos_service_account'


def _get_service_account_path():
    for path in _SERVICE_ACCOUNT_PATHS:
        if os.path.exists(path):
            return path
    return _SERVICE_ACCOUNT_PATHS[0]


def _describe_keyring_backend():
    backend = keyring.get_keyring()
    return f"{backend.__class__.__module__}.{backend.__class__.__name__}"


def store_credentials_in_keyring(bos_service_account_json_path):
    if not os.path.exists(bos_service_account_json_path):
        raise FileNotFoundError(f"Service account file not found: {bos_service_account_json_path}")
    with open(bos_service_account_json_path, 'r', encoding='utf-8') as f:
        credentials_json = f.read()
    keyring.set_password(_KEYRING_SERVICE, _KEYRING_USERNAME, credentials_json)
    print(f"Credentials stored in system keyring under service '{_KEYRING_SERVICE}'")
    auth_ok("keyring", f"Credentials stored under service '{_KEYRING_SERVICE}'")  # ← added


def authenticate_gdrive():
    """
    Authenticate for Google Drive API.
    Priority:
    1. OAuth user credentials (token.pickle)
    2. Service account from keyring
    3. Service account JSON file
    """
    auth_info("gdrive", "authenticate_gdrive() called")  # ← added

    creds = None
    token_path          = os.path.join(_UTILITIES_DIR, 'token.pickle')
    client_secrets_path = os.path.join(_UTILITIES_DIR, 'client_secrets.json')

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                auth_ok("gdrive", "OAuth token refreshed successfully")  # ← added
            except RefreshError as error:
                print(f"OAuth token refresh failed: {error}")
                auth_warn("gdrive", "OAuth token refresh failed", detail=str(error))  # ← added
                creds = None
                if os.path.exists(token_path):
                    os.remove(token_path)
                    print("Removed stale token.pickle; falling back to other credential sources.")
                    auth_warn("gdrive", "Removed stale token.pickle — falling back")  # ← added
        elif os.path.exists(client_secrets_path):
            flow  = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
            creds = flow.run_local_server(port=0)
            auth_info("gdrive", "OAuth flow completed via client_secrets.json")  # ← added
        if creds:
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)

    if creds and creds.valid:
        print("✓ Loaded OAuth user credentials (token.pickle)")
        auth_ok("gdrive", "Loaded OAuth user credentials from token.pickle")  # ← added
        return creds

    # Fallback: service account
    backend = _describe_keyring_backend()
    print(f"Using keyring backend: {backend}")
    auth_info("keyring", f"Active backend: {backend}")  # ← added

    try:
        credentials_json = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
        if credentials_json:
            credentials_dict = json.loads(credentials_json)
            from google.oauth2.service_account import Credentials as ServiceAccountCredentials
            creds = ServiceAccountCredentials.from_service_account_info(
                credentials_dict, scopes=SCOPES
            )
            print("✓ Loaded credentials from system keyring")
            auth_ok("gdrive", "Loaded service account credentials from keyring")  # ← added
            return creds
    except Exception as e:
        print(f"Could not load from keyring: {e}")
        auth_warn("keyring", "Could not load service account from keyring", detail=str(e))  # ← added

    service_account_path = _get_service_account_path()
    if os.path.exists(service_account_path):
        from google.oauth2.service_account import Credentials as ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_service_account_file(
            service_account_path, scopes=SCOPES
        )
        fname = os.path.basename(service_account_path)
        print(f"✓ Loaded credentials from {fname} file")
        print(f"💡 Consider storing credentials in keyring: python -c \"from utilities.gdrive_auth import store_credentials_in_keyring; store_credentials_in_keyring('{service_account_path}')\"")
        auth_ok("gdrive", f"Loaded service account credentials from {fname}")           # ← added
        auth_warn("gdrive", "Credentials loaded from file — consider moving to keyring")  # ← added
        return creds

    auth_critical("gdrive", "No credentials found — authentication failed")  # ← added
    raise FileNotFoundError(
        f"No credentials found. Either:\n"
        f"1. Complete OAuth flow (place client_secrets.json in {_UTILITIES_DIR})\n"
        f"2. Store service account in keyring or a supported service account json in {_UTILITIES_DIR}"
    )


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "store":
        if len(sys.argv) > 2:
            store_credentials_in_keyring(sys.argv[2])
        else:
            store_credentials_in_keyring(_get_service_account_path())
    else:
        user_creds = authenticate_gdrive()
        print("Google Drive authentication successful.")