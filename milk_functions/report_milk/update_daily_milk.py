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
import sys
import os
import pandas as pd
import numpy as np

# Resolve repo root (2 levels up from milk_functions/report_milk/)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')), '.env'))
from utilities.gdrive_loader import gdrive_read_sheet_tab, gdrive_write_sheet_tab, gdrive_write_excel, gdrive_read_csv
from config_path import RAW_DIR, GROUP_DATA_DIR, gdrive_rel

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


    def load_data(self):
        import pandas as pd
        # Load manual_entries as before (still from Google Sheets)
        try:
            self.manual_df = gdrive_read_sheet_tab(DAILY_MILK_SHEET_ID, MANUAL_ENTRIES_TAB)
            print(f"[1] manual_entries shape: {self.manual_df.shape}", flush=True)
        except Exception as e:
            print(f"[ERROR] Could not read manual_entries tab: {e}", flush=True)
            sys.exit(1)
        self.manual = self._load_manual_entries(self.manual_df)

        # Load raw tabs from GDrive (full file, then take last 10 date columns)
        print("[2] Reading raw CSV files from GDrive...", flush=True)
        for tab in RAW_TABS:
            df = gdrive_read_csv(gdrive_rel(RAW_DIR / f'{tab}.csv'), header=0, index_col=0)
            self.raw[tab] = df.iloc[:, -10:]
            print(f"    {tab}.csv (last 10 cols): {self.raw[tab].shape}", flush=True)

        # Determine last 10 dates (from columns of AM_liters)
        all_dates = list(self.raw['AM_liters'].columns)
        self.last_10_dates = all_dates
        print(f"[3] Last 10 dates: {self.last_10_dates}", flush=True)

        # Load group tabs from GDrive (full file, then take last 10 date columns)
        print("[4] Reading group CSV files from GDrive...", flush=True)
        for tab in GROUP_TABS:
            df = gdrive_read_csv(gdrive_rel(GROUP_DATA_DIR / f'{tab}.csv'), header=0, index_col=0)
            self.groups[tab] = df.iloc[:, -10:]
            print(f"    {tab}.csv (last 10 cols): {self.groups[tab].shape}", flush=True)

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
        print("[5] Writing only 'stats' tab to daily_milk Sheet (last 10 days)...", flush=True)
        try:
            gdrive_write_sheet_tab(DAILY_MILK_SHEET_ID, 'stats', self.stats_out)
        except Exception as e:
            print(f"[ERROR] Could not write 'stats' tab: {e}", flush=True)
        print("[6] Done. Only 'stats' tab updated in Google Drive.", flush=True)

    def load_and_process(self):
        self.load_data()
        self.build_stats()
        self.save_outputs()


if __name__ == "__main__":
    updater = DailyMilkUpdater()
    updater.load_and_process()



