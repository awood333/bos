"""
list_gsheet_tabs.py

Diagnostic script: Lists all sheet/tab names in a Google Sheet as seen by the Google Sheets API.
Place your service account JSON in the same folder or update the path below.
"""

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import os

# --- CONFIGURE THESE ---
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), 'service_account.json')  # Path to your credentials file
SHEET_ID = '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'  # Your Google Sheet ID

# --- DO NOT EDIT BELOW ---
def main():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    service = build('sheets', 'v4', credentials=creds)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', '')

    print('Sheet/tab names as seen by the API:')
    for s in sheets:
        print('-', s['properties']['title'])

if __name__ == '__main__':
    main()
