
import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv

SCOPES = [
    'https://www.googleapis.com/auth/drive',        # file upload/download
    'https://www.googleapis.com/auth/spreadsheets',  # Sheets API read/write
]


_UTILITIES_DIR = os.path.dirname(os.path.abspath(__file__))

def authenticate_gdrive(token_path=None):
    """
    Authenticates and returns Google Drive API credentials.

    Security & Usage Notes:
    - The path to your Google OAuth2 credentials.json file is loaded from a .env file (see project root).
    - The .env file should contain a line like:
        GOOGLE_CLIENT_SECRET_PATH=d:/Git_repos/bos/utilities/client_secret_258465643277-3vkitd5dl9lusuatg8tukcnfc9lcpjip.apps.googleusercontent.com.json
    - Do NOT commit .env or your credentials file to version control. .env is listed in .gitignore.
    - The token.pickle file stores your access/refresh token and is safe to keep locally, but should not be shared.
    - This script uses python-dotenv to load environment variables.

    - token_path: Path to store/retrieve the user's access/refresh token (defaults to utilities/token.pickle).
    """
    load_dotenv()
    if token_path is None:
        token_path = os.path.join(_UTILITIES_DIR, 'token.pickle')
    creds_path = os.getenv('GOOGLE_CLIENT_SECRET_PATH', 'credentials.json')
    creds = None
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            login_hint = os.getenv('GOOGLE_ACCOUNT_EMAIL', '')
            creds = flow.run_local_server(port=0, login_hint=login_hint)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    return creds

if __name__ == "__main__":
    user_creds = authenticate_gdrive()
    print("Google Drive authentication successful.")
