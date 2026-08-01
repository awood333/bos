"""
update_daily_milk.py (revamped)

Builds daily_milk.xlsx in Google Drive from its individual source sheets.

Manual data for sale total, heldback AM/PM comes from manual_entries sheet.
Raw data comes from 4 tabs in milk_data/raw/.
Only appends new dates after the last date in the current stats tab.
Safety: Halts if last date in manual_entries and raw data do not match.
Outputs: full stats and a 'tenday' (last 10 days) version.
"""
import io
import os
import sys
from pathlib import Path

# pylint: disable=wrong-import-position
# Add project root to Python path for cross-platform module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


import numpy as np
import pandas as pd
from googleapiclient.discovery import build
from utilities.gdrive_auth import authenticate_gdrive
from utilities.gdrive_loader import gdrive_read_sheet_tab, gdrive_write_sheet_tab
# pylint: enable=wrong-import-position

# ---------------------------------------------------------------------------
# GDrive IDs  (set in .env or override here)
# ---------------------------------------------------------------------------
DAILY_MILK_SHEET_ID   = os.getenv('DAILY_MILK_SHEET_ID',   '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk')
RAW_MILK_MASTER_ID    = os.getenv('RAW_MILK_MASTER_ID',    '1mCgtSbT_2OVbA469Wg6n0ne4EHu20RXbPZ6gKD9JTuw')
WB_GROUPS_SHEET_ID    = os.getenv('WB_GROUPS_SHEET_ID',    '1TkNebQXkY_TXOYikCbLjqKJfEFDawPEFbgCGDNhe2u4')

RAW_TABS = ['AM_liters', 'PM_liters', 'AM_wy', 'PM_wy']
GROUP_TABS = ['fresh', 'group_A', 'group_B', 'group_C', 'sick']
MANUAL_ENTRIES_TAB = 'manual_entries'
STATS_ROW_ORDER = [
    'sale total',
    'spacer_1',
    'heldback AM',
    'heldback PM',
    'heldback total',
    'spacer_2',
    'AM',
    'PM',
    'fullday - WY',
    'spacer_3',
    'fullday net  WY',
    'spacer_4',
    'gap %diff',
    'spacer_5',
    'gap, liters total',
]


class DailyMilkUpdater:
    def __init__(self):
        self.manual_df = None
        self.manual = None
        self.raw = {}
        self.groups = {}
        self.stats = None
        self.stats_out = None
        self.last_10_dates = None

    @staticmethod
    def _sheet_url(spreadsheet_id):
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    @staticmethod
    def _drive_file_url(file_id):
        return f"https://drive.google.com/file/d/{file_id}/view"


    def load_data(self):
        # [1] manual_entries drives the authoritative date range
        try:
            self.manual_df = gdrive_read_sheet_tab(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'), MANUAL_ENTRIES_TAB)
            print(f"[1] manual_entries shape: {self.manual_df.shape}", flush=True)
        except Exception as e:
            print(f"[ERROR] Could not read manual_entries tab: {e}", flush=True)
            sys.exit(1)

        # Determine last 10 dates from manual_entries columns (the authoritative source)
        self.last_10_dates = list(self.manual_df.columns)[-10:]
        print(f"[2] Last 10 dates from manual_entries: {self.last_10_dates}", flush=True)

        self.manual = self._load_manual_entries(self.manual_df)

        # [3] Read raw tabs directly from the raw milk master Google Sheet
        print(f"[3] Reading raw tabs from raw milk master Sheet ({RAW_MILK_MASTER_ID})...", flush=True)
        for tab in RAW_TABS:
            df = gdrive_read_sheet_tab(RAW_MILK_MASTER_ID, tab)
            available = [d for d in self.last_10_dates if d in df.columns]
            missing = [d for d in self.last_10_dates if d not in df.columns]
            if missing:
                print(f"  [warn] '{tab}' missing dates: {missing}", flush=True)
            self.raw[tab] = df[available]
            print(f"    {tab} (last 10 cols): {self.raw[tab].shape}", flush=True)

        # [4] Read group tabs directly from the WB_groups master Google Sheet
        print(f"[4] Reading group tabs from WB_groups master Sheet ({WB_GROUPS_SHEET_ID})...", flush=True)
        for tab in GROUP_TABS:
            df = gdrive_read_sheet_tab(WB_GROUPS_SHEET_ID, tab)
            available = [d for d in self.last_10_dates if d in df.columns]
            missing = [d for d in self.last_10_dates if d not in df.columns]
            if missing:
                print(f"  [warn] '{tab}' missing dates: {missing}", flush=True)
            self.groups[tab] = df[available]
            print(f"    {tab} (last 10 cols): {self.groups[tab].shape}", flush=True)

    def _load_manual_entries(self, manual_df: pd.DataFrame) -> dict:
        df = manual_df.copy()
        df.index = df.index.map(lambda x: str(x).strip().lower())
        manual_rows = {}
        for label in ('sale total', 'heldback am', 'heldback pm'):
            if label in df.index:
                manual_rows[label] = df.loc[label]
            else:
                print(f"  [warn] '{label}' not found in manual_entries; will default to 0.", flush=True)
                manual_rows[label] = pd.Series(dtype=float)
        return manual_rows

    def build_stats(self):
        am_liters = self.raw['AM_liters']
        pm_liters = self.raw['PM_liters']
        manual = self.manual
        dates = self.last_10_dates

        am_num = am_liters[dates].apply(pd.to_numeric, errors='coerce')
        pm_num = pm_liters[dates].apply(pd.to_numeric, errors='coerce')

        def get_manual(label):
            s = manual.get(label.lower(), pd.Series(dtype=float))
            if not s.empty:
                missing = [d for d in dates if d not in s.index]
                if missing:
                    print(f"  [warn] manual '{label}' missing dates: {missing[:3]}... (manual cols sample: {list(s.index[:3])})", flush=True)
            s = pd.to_numeric(s, errors='coerce').fillna(0)
            return s.reindex(dates, fill_value=0).astype(float)

        sale_total   = get_manual('sale total')
        heldback_am  = get_manual('heldback am')
        heldback_pm  = get_manual('heldback pm')

        heldback_total = heldback_am + heldback_pm
        am_row         = am_num.sum(axis=0).reindex(dates, fill_value=0)
        pm_row         = pm_num.sum(axis=0).reindex(dates, fill_value=0)
        fullday_wy     = am_row + pm_row
        fullday_net_wy = fullday_wy - heldback_total

        gap_liters  = sale_total - fullday_net_wy
        gap_pct     = np.where(fullday_net_wy != 0, gap_liters / fullday_net_wy, np.nan)
        gap_pct_s   = pd.Series(gap_pct, index=dates)

        computed = {
            'sale total':       sale_total,
            'heldback AM':      heldback_am,
            'heldback PM':      heldback_pm,
            'heldback total':   heldback_total,
            'AM':               am_row,
            'PM':               pm_row,
            'fullday - WY':     fullday_wy,
            'fullday net  WY':  fullday_net_wy,
            'gap %diff':        gap_pct_s,
            'gap, liters total': gap_liters,
        }

        rows = []
        labels = []
        for label in STATS_ROW_ORDER:
            if label.startswith('spacer_'):
                rows.append(pd.Series([None] * len(dates), index=dates))
                labels.append(label)
            else:
                rows.append(computed[label])
                labels.append(label)

        stats_df = pd.DataFrame(rows, index=labels)
        stats_df.index.name = 'index'
        self.stats = stats_df
        # For output: blank out spacers
        stats_out = stats_df.copy()
        stats_out.index = ["" if str(idx).startswith("spacer_") else idx for idx in stats_out.index]
        self.stats_out = stats_out

    def save_outputs(self):
        print(f"[5a] Target Sheet: {self._sheet_url(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'))}", flush=True)
        # Write raw tabs (last 10 dates) to daily_milk sheet
        print("[5] Writing raw tabs to daily_milk Sheet (last 10 days)...", flush=True)
        for tab in RAW_TABS:
            try:
                gdrive_write_sheet_tab(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'), tab, self.raw[tab])
                print(f"    wrote '{tab}'", flush=True)
            except Exception as e:
                print(f"[ERROR] Could not write '{tab}' tab: {e}", flush=True)

        # Write group tabs
        print("[6] Writing group tabs to daily_milk Sheet (last 10 days)...", flush=True)
        for tab in GROUP_TABS:
            try:
                gdrive_write_sheet_tab(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'), tab, self.groups[tab])
                print(f"    wrote '{tab}'", flush=True)
            except Exception as e:
                print(f"[ERROR] Could not write '{tab}' tab: {e}", flush=True)

        # Write stats tab
        print("[7] Writing 'stats' tab to daily_milk Sheet...", flush=True)
        try:
            gdrive_write_sheet_tab(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'), 'stats', self.stats_out)
        except Exception as e:
            print(f"[ERROR] Could not write 'stats' tab: {e}", flush=True)
        self.verify_outputs()
        print("[8] Done.", flush=True)

    def verify_outputs(self):
        """Read back a small slice so the script confirms the live Sheet changed."""
        print("[7a] Verifying live Sheet contents...", flush=True)
        for tab in ('AM_liters', 'stats'):
            verify_df = gdrive_read_sheet_tab(os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'), tab)
            actual_dates = [str(col) for col in verify_df.columns[-10:]]
            if actual_dates != list(map(str, self.last_10_dates)):
                raise ValueError(
                    f"Verification failed for '{tab}': expected last dates {self.last_10_dates}, got {actual_dates}"
                )
            print(f"    verified '{tab}' dates through {actual_dates[-1]}", flush=True)

    def export_sheet_as_xlsx(self, out_gdrive_path):
        """
        Export the daily_milk Google Sheet as XLSX and upload to Google Drive at out_gdrive_path.
        """
        # Import _resolve_folder_id from gdrive_loader
        from utilities.gdrive_loader import _resolve_folder_id
        service = build('drive', 'v3', credentials=authenticate_gdrive())
        print(f"[9] Exporting daily_milk Sheet as XLSX to {out_gdrive_path} ...", flush=True)
        # Export as XLSX
        request = service.files().export_media(
            fileId=os.getenv('DAILY_MILK_SHEET_ID', '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk'),
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        data = request.execute()
        # Find parent folder and file name
        folder_parts = out_gdrive_path.replace('\\', '/').split('/')
        file_name = folder_parts[-1]
        folder_id = None
        if len(folder_parts) > 1:
            # Resolve folder
            folder_id = _resolve_folder_id(folder_parts[:-1])
        # Find existing file and UPDATE it — service accounts cannot create new files (no storage quota)
        from googleapiclient.http import MediaIoBaseUpload
        XLSX_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=XLSX_MIME)
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        if items:
            file_id = items[0]['id']
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[10] XLSX updated in Drive (file ID: {file_id})", flush=True)
            print(f"[10a] XLSX link: {self._drive_file_url(file_id)}", flush=True)
            # Also save a local copy so it's accessible without a GDrive mount
            local_dir = Path.home() / "cows_data" / "milk_data" / "daily_milk"
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / file_name
            local_path.write_bytes(data)
            print(f"[10b] Local copy saved: {local_path}", flush=True)
        else:
            print(f"[ERROR] '{file_name}' not found in Drive folder. Cannot create via service account. Please create it manually once.", flush=True)

    def load_and_process(self):
        self.load_data()
        self.build_stats()
        self.save_outputs()


if __name__ == "__main__":
    updater = DailyMilkUpdater()
    updater.load_and_process()
    # Export to GDrive as XLSX (path relative to My Drive)
    updater.export_sheet_as_xlsx('COWS/milk_data/daily_milk/daily_milk.xlsx')



