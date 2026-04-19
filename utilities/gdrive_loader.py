"""
gdrive_loader.py

Utilities to fetch CSV/Excel files directly from Google Drive (cloud),
bypassing the local synced Q:/G: drive folder.

Usage:
    from utilities.gdrive_loader import gdrive_read_csv, gdrive_read_excel

    df = gdrive_read_csv("COWS/basic_data/live_births.csv")
    df = gdrive_read_excel("COWS/milk_data/daily_milk/daily_milk.xlsx")

The path is relative to 'My Drive' (e.g., "COWS/basic_data/live_births.csv").
Google Drive authentication uses credentials in .env (see gdrive_auth.py).
"""

import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utilities.gdrive_auth import authenticate_gdrive

_service = None
_sheets_service = None
_cache = {}  # keyed by gdrive_path, stores downloaded DataFrames


def _get_sheets_service():
    """Return a cached Google Sheets API v4 service instance."""
    global _sheets_service
    if _sheets_service is None:
        creds = authenticate_gdrive()
        _sheets_service = build('sheets', 'v4', credentials=creds)
    return _sheets_service


def gdrive_read_sheet_tab(spreadsheet_id, tab_name, **kwargs):
    """
    Read a single tab from a native Google Sheet by spreadsheet ID and tab name.
    Returns a pandas DataFrame. Results are cached per (spreadsheet_id, tab_name).

    Args:
        spreadsheet_id: The ID from the Sheet URL (between /d/ and /edit)
        tab_name:       The tab name, e.g. 'AM_wy', 'AM_liters'
        **kwargs:       Passed to pd.DataFrame() construction (rarely needed)

    Example:
        df = gdrive_read_sheet_tab('1mCgtSbT_2OVbA469Wg6n0ne4EHu20RXbPZ6gKD9JTuw', 'AM_wy')
    """
    cache_key = ('sheet_tab', spreadsheet_id, tab_name)
    if cache_key in _cache:
        print(f"[gdrive] Cache hit: {tab_name} in {spreadsheet_id[:12]}...", flush=True)
        return _cache[cache_key].copy()

    print(f"[gdrive] Reading Sheet tab: '{tab_name}' ...", flush=True)
    service = _get_sheets_service()
    result = (
        service.spreadsheets().values()
        .get(spreadsheetId=spreadsheet_id, range=tab_name)
        .execute()
    )
    values = result.get('values', [])
    if not values:
        raise ValueError(f"No data found in tab '{tab_name}' of spreadsheet {spreadsheet_id}")

    # First row = headers, first column = index
    # Sheets API omits trailing empty cells — pad all rows to header length
    header = values[0]
    n = len(header)
    padded = [row + [''] * (n - len(row)) for row in values[1:]]
    df = pd.DataFrame(padded, columns=header)
    df = df.set_index(df.columns[0])
    print(f"[gdrive] Done: '{tab_name}' ({df.shape[0]} rows x {df.shape[1]} cols)", flush=True)
    _cache[cache_key] = df
    return df.copy()


def gdrive_write_sheet_tab(spreadsheet_id, tab_name, df):
    """
    Write a DataFrame to a single tab in a native Google Sheet via the Sheets API.
    Clears the tab first, then writes header + data rows.
    NaN values are written as empty strings.

    Args:
        spreadsheet_id: The ID from the Sheet URL (between /d/ and /edit)
        tab_name:       The tab name, e.g. 'AM_wy', 'stats'
        df:             DataFrame to write (index is written as the first column)
    """
    import math

    service = _get_sheets_service()
    print(f"[gdrive] Writing Sheet tab: '{tab_name}' ({df.shape[0]} rows x {df.shape[1]} cols)...", flush=True)

    def _clean(v):
        if v is None:
            return ''
        if isinstance(v, float) and math.isnan(v):
            return ''
        return v

    header = [df.index.name or 'index'] + list(df.columns)
    rows   = [[_clean(idx)] + [_clean(v) for v in row]
              for idx, row in zip(df.index, df.values)]
    values = [header] + rows

    # Clear existing content then write fresh
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=tab_name
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=tab_name,
        valueInputOption='RAW',
        body={'values': values},
    ).execute()
    print(f"[gdrive] Done writing '{tab_name}'.", flush=True)

    # Invalidate read cache for this tab
    key = ('sheet_tab', spreadsheet_id, tab_name)
    _cache.pop(key, None)


def _get_service():
    """Return a cached Google Drive API service instance."""
    global _service
    if _service is None:
        print("  [gdrive] Authenticating...", flush=True)
        creds = authenticate_gdrive()
        print("  [gdrive] Building Drive service...", flush=True)
        _service = build('drive', 'v3', credentials=creds)
        print("  [gdrive] Drive service ready.", flush=True)
    return _service


def _resolve_folder_id(path_parts, parent_id='root'):
    """
    Recursively resolve a list of folder name parts to the final folder's Drive ID.
    e.g. ['COWS', 'basic_data'] starting from 'root'.
    """
    service = _get_service()
    current_parent = parent_id
    for folder_name in path_parts:
        print(f"  [gdrive] Resolving folder: '{folder_name}'...", flush=True)
        query = (
            f"name='{folder_name}' and "
            f"'{current_parent}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            raise FileNotFoundError(f"Google Drive folder not found: '{folder_name}' under parent '{current_parent}'")
        current_parent = items[0]['id']
    return current_parent


def _get_file_id(file_name, folder_id):
    """Find a file by name within a specific folder and return its (Drive file ID, mimeType)."""
    service = _get_service()
    print(f"  [gdrive] Locating file: '{file_name}'...", flush=True)
    query = (
        f"name='{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    if not items:
        raise FileNotFoundError(f"Google Drive file not found: '{file_name}' in folder ID '{folder_id}'")
    return items[0]['id'], items[0]['mimeType']


def _download_file_bytes(file_id, mime_type):
    """Download a file from Google Drive and return its contents as bytes.
    For Google Sheets, exports as CSV. For other files, downloads directly."""
    service = _get_service()
    print(f"  [gdrive] Downloading file ID: {file_id}...", flush=True)
    SHEET_MIME = 'application/vnd.google-apps.spreadsheet'
    if mime_type == SHEET_MIME:
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')
    else:
        request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    print(f"  [gdrive] Download complete.", flush=True)
    buffer.seek(0)
    return buffer


def gdrive_read_csv(gdrive_path, **kwargs):
    """
    Read a CSV file (or Google Sheet) from Google Drive and return a pandas DataFrame.
    Results are cached in-memory — each file is only downloaded once per session.
    If a Google Sheet is found alongside or instead of a CSV, it is exported live.

    Args:
        gdrive_path: Path relative to 'My Drive', e.g. "COWS/basic_data/live_births.csv"
        **kwargs:    Passed directly to pd.read_csv()

    Returns:
        pd.DataFrame
    """
    cache_key = ('csv', gdrive_path, tuple(sorted(kwargs.items())))
    if cache_key in _cache:
        print(f"[gdrive] Cache hit: {gdrive_path}", flush=True)
        return _cache[cache_key].copy()

    print(f"[gdrive] Reading CSV: {gdrive_path}", flush=True)
    parts = gdrive_path.replace('\\', '/').split('/')
    folder_parts = parts[:-1]
    file_name = parts[-1]

    folder_id = _resolve_folder_id(folder_parts)

    # Try Sheet name (without extension) first — always live data
    # Fall back to exact file name (CSV) if no Sheet found
    file_name_no_ext = file_name.rsplit('.', 1)[0]
    file_id, mime_type = None, None
    for name in [file_name_no_ext, file_name]:
        try:
            file_id, mime_type = _get_file_id(name, folder_id)
            break
        except FileNotFoundError:
            continue
    if file_id is None:
        raise FileNotFoundError(f"Google Drive file not found: '{file_name}' in {'/'.join(folder_parts)}")

    buffer = _download_file_bytes(file_id, mime_type)
    df = pd.read_csv(buffer, **kwargs)
    print(f"[gdrive] Done: {file_name} ({len(df)} rows)", flush=True)
    _cache[cache_key] = df
    return df.copy()


def gdrive_read_excel(gdrive_path, **kwargs):
    """
    Read an Excel file from Google Drive and return a pandas DataFrame.
    Results are cached in-memory — each file is only downloaded once per session.

    Args:
        gdrive_path: Path relative to 'My Drive', e.g. "COWS/milk_data/daily_milk/daily_milk.xlsx"
        **kwargs:    Passed directly to pd.read_excel()

    Returns:
        pd.DataFrame
    """
    cache_key = ('excel', gdrive_path, tuple(sorted(kwargs.items())))
    if cache_key in _cache:
        print(f"[gdrive] Cache hit: {gdrive_path}", flush=True)
        return _cache[cache_key].copy()

    print(f"[gdrive] Reading Excel: {gdrive_path}", flush=True)
    parts = gdrive_path.replace('\\', '/').split('/')
    folder_parts = parts[:-1]
    file_name = parts[-1]

    folder_id = _resolve_folder_id(folder_parts)
    file_id, mime_type = _get_file_id(file_name, folder_id)
    buffer = _download_file_bytes(file_id, mime_type)
    df = pd.read_excel(buffer, **kwargs)
    print(f"[gdrive] Done: {file_name} ({len(df)} rows)", flush=True)
    _cache[cache_key] = df
    return df.copy()


def gdrive_write_excel(gdrive_path, excel_bytes):
    """
    Upload Excel bytes to Google Drive, replacing the existing file if it exists
    or creating it if it doesn't.

    Args:
        gdrive_path: Path relative to 'My Drive', e.g. "COWS/milk_data/daily_milk/daily_milk.xlsx"
        excel_bytes: Raw bytes of the Excel file (e.g. from an io.BytesIO buffer)
    """
    service = _get_service()
    parts = gdrive_path.replace('\\', '/').split('/')
    folder_parts = parts[:-1]
    file_name = parts[-1]

    EXCEL_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

    folder_id = _resolve_folder_id(folder_parts)
    print(f"[gdrive] Uploading '{file_name}'...", flush=True)

    media = MediaIoBaseUpload(
        io.BytesIO(excel_bytes),
        mimetype=EXCEL_MIME,
        resumable=True,
    )

    try:
        file_id, _ = _get_file_id(file_name, folder_id)
        # File exists — update content in place
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"[gdrive] Updated existing file: {file_name}", flush=True)
    except FileNotFoundError:
        # File doesn't exist — create it
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"[gdrive] Created new file: {file_name}", flush=True)

    # Invalidate any cached reads for this path
    keys_to_remove = [k for k in _cache if isinstance(k, tuple) and k[1] == gdrive_path]
    for k in keys_to_remove:
        del _cache[k]
