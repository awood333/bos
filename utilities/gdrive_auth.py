
import os
from google.oauth2 import service_account

SCOPES = [
    'https://www.googleapis.com/auth/drive',        # file upload/download
    'https://www.googleapis.com/auth/spreadsheets',  # Sheets API read/write
]

_UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))
_SERVICE_ACCOUNT_PATH = os.path.join(_UTILITIES_DIR, 'service_account.json')

def authenticate_gdrive():
    """
    Authenticates using a service account key file (never expires).
    The service account email must be shared on any Google Drive folders/Sheets
    that the app needs to access.
    """
    creds = service_account.Credentials.from_service_account_file(
        _SERVICE_ACCOUNT_PATH, scopes=SCOPES
    )
    return creds

if __name__ == "__main__":
    user_creds = authenticate_gdrive()
    print("Google Drive authentication successful.")
