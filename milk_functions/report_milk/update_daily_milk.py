"""
update_daily_milk.py

Rebuilds daily_milk.xlsx in Google Drive from its individual source sheets.

Source sheets pulled from GDrive:
  COWS/milk_data/raw/  → AM_liters, PM_liters, AM_wy, PM_wy
  COWS/wb_groups/      → fresh, group_A, group_B, group_C, sick

Stats sheet logic:
  - Manual rows preserved from existing xlsx: sale total, heldback AM, heldback PM
  - Computed rows:
      heldback total  = heldback AM + heldback PM
      AM              = column sum of AM_liters
      PM              = column sum of PM_liters
      fullday - WY    = AM + PM
      fullday net  WY = fullday - WY  -  heldback total
      gap %diff       = (sale total - fullday net WY) / fullday net WY  [as %]
      gap, liters total = sale total - fullday net WY

Usage:
    python update_daily_milk.py

First run after changing gdrive_auth.py scope: delete utilities/token.pickle so
Google re-prompts for permission with the new (write) scope.
"""

import io
import sys
import os
import pandas as pd
import numpy as np

# Resolve repo root (3 levels up from milk_functions/report_milk/)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')), '.env'))
from utilities.gdrive_loader import gdrive_read_sheet_tab, gdrive_write_sheet_tab, gdrive_write_excel

# ---------------------------------------------------------------------------
# GDrive IDs  (set in .env or override here)
# ---------------------------------------------------------------------------
DAILY_MILK_SHEET_ID   = os.getenv('DAILY_MILK_SHEET_ID',   '1ouQoDXxKjmIZ1XFH0_Ga4oISNXoKi60MmuKyHNYlONk')
RAW_MILK_MASTER_ID    = os.getenv('RAW_MILK_MASTER_ID',    '1mCgtSbT_2OVbA469Wg6n0ne4EHu20RXbPZ6gKD9JTuw')
WB_GROUPS_SHEET_ID    = os.getenv('WB_GROUPS_SHEET_ID',    '1TkNebQXkY_TXOYikCbLjqKJfEFDawPEFbgCGDNhe2u4')

# Tabs to pull from raw_milk_master
RAW_TABS = ['AM_liters', 'PM_liters', 'AM_wy', 'PM_wy']

# Tabs to pull from wb_groups sheet (or fallback to CSV if no sheet ID set)
GROUP_TABS = ['fresh', 'group_A', 'group_B', 'group_C', 'sick']

# Row order and spacing in the stats sheet (None = blank spacer row)
STATS_ROW_ORDER = [
    'sale total',
    None,
    'heldback AM',
    'heldback PM',
    'heldback total',
    None,
    'AM',
    'PM',
    'fullday - WY',
    None,
    None,
    None,
    None,
    'fullday net  WY',
    None,
    None,
    None,
    'gap %diff',
    None,
    None,
    None,
    'gap, liters total',
]


def load_manual_entries(existing_stats: pd.DataFrame) -> dict[str, pd.Series]:
    """
    Extract manually entered rows (sale total, heldback AM, heldback PM)
    from the existing stats DataFrame.

    The stats sheet is stored with the index in the first column and dates
    as subsequent columns, e.g.:

        index          | 2026-03-30 | 2026-03-31 | ...
        sale total     | 646.9      | 651.8      | ...
        heldback AM    | 31         | 28         | ...
        ...

    Returns a dict mapping row label → pd.Series indexed by date strings.
    """
    # gdrive_read_sheet_tab already sets the first column as index
    df = existing_stats.copy()
    manual_rows = {}
    for label in ('sale total', 'heldback AM', 'heldback PM'):
        if label in df.index:
            manual_rows[label] = df.loc[label]
        else:
            print(f"  [warn] '{label}' not found in existing stats; will default to 0.", flush=True)
            manual_rows[label] = pd.Series(dtype=float)
    return manual_rows


def build_stats_df(
    am_liters: pd.DataFrame,
    pm_liters: pd.DataFrame,
    manual: dict,
) -> pd.DataFrame:
    """
    Build the stats DataFrame.

    Parameters
    ----------
    am_liters, pm_liters : DataFrames with cows as rows, date strings as columns.
    manual : dict with keys 'sale total', 'heldback AM', 'heldback PM',
             each a pd.Series indexed by date string.

    Returns
    -------
    DataFrame with metric names as index, date strings as columns.
    """
    dates = am_liters.columns  # shared date index

    # Sheets API returns all values as strings — convert to numeric
    am_num = am_liters.apply(pd.to_numeric, errors='coerce')
    pm_num = pm_liters.apply(pd.to_numeric, errors='coerce')

    def get_manual(label):
        s = manual.get(label, pd.Series(dtype=float))
        s = pd.to_numeric(s, errors='coerce').fillna(0)
        return s.reindex(dates, fill_value=0).astype(float)

    sale_total   = get_manual('sale total')
    heldback_am  = get_manual('heldback AM')
    heldback_pm  = get_manual('heldback PM')

    heldback_total = heldback_am + heldback_pm
    am_row         = am_num.sum(axis=0).reindex(dates, fill_value=0)
    pm_row         = pm_num.sum(axis=0).reindex(dates, fill_value=0)
    fullday_wy     = am_row + pm_row
    fullday_net_wy = fullday_wy - heldback_total

    # Avoid divide-by-zero
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

    # Build DataFrame with blank spacer rows matching the original layout
    rows = []
    labels = []
    for label in STATS_ROW_ORDER:
        if label is None:
            rows.append(pd.Series([None] * len(dates), index=dates))
            labels.append('')
        else:
            rows.append(computed[label])
            labels.append(label)

    stats_df = pd.DataFrame(rows, index=labels)
    stats_df.index.name = 'index'
    return stats_df


def build_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    """
    Write all sheets into an in-memory Excel workbook and return the bytes.
    Header row (row 1) gets rotated 90° and vertically centered on every sheet.

    Parameters
    ----------
    sheets : ordered dict of {sheet_name: DataFrame}
    """
    from openpyxl.styles import Alignment

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            # Row 1 is the header written by to_excel (index label + date columns)
            for cell in ws[1]:
                cell.alignment = Alignment(
                    text_rotation=90,
                    vertical='center',
                    horizontal='center',
                    wrap_text=False,
                )
            ws.row_dimensions[1].height = 80  # pixels height for rotated dates
    return buffer.getvalue()


def main():
    print("=== update_daily_milk.py ===", flush=True)

    # -----------------------------------------------------------------------
    # 1. Fetch existing stats sheet to preserve manual entries
    # -----------------------------------------------------------------------
    print("\n[1] Reading manual entries from daily_milk Sheet (stats tab)...", flush=True)
    try:
        existing_stats = gdrive_read_sheet_tab(DAILY_MILK_SHEET_ID, 'stats')
        manual = load_manual_entries(existing_stats)
        print(f"    Manual entry dates found: {len(next(iter(manual.values())))}", flush=True)
    except Exception as e:
        print(f"    [warn] Could not read existing stats ({e}); manual entries will be 0.", flush=True)
        manual = {}

    # -----------------------------------------------------------------------
    # 2. Read raw tabs from raw_milk_master (live Google Sheet)
    # -----------------------------------------------------------------------
    print("\n[2] Reading raw tabs from raw_milk_master Sheet...", flush=True)
    raw = {}
    for tab in RAW_TABS:
        raw[tab] = gdrive_read_sheet_tab(RAW_MILK_MASTER_ID, tab)
        print(f"    {tab}: {raw[tab].shape}", flush=True)

    # -----------------------------------------------------------------------
    # 2b. Read group tabs from Master_groups Sheet
    # -----------------------------------------------------------------------
    print("\n[2b] Reading group tabs from Master_groups Sheet...", flush=True)
    groups = {}
    for tab in GROUP_TABS:
        groups[tab] = gdrive_read_sheet_tab(WB_GROUPS_SHEET_ID, tab)
        print(f"    {tab}: {groups[tab].shape}", flush=True)

    # -----------------------------------------------------------------------
    # 3. Build stats sheet
    # -----------------------------------------------------------------------
    print("\n[3] Computing stats sheet...", flush=True)
    stats_df = build_stats_df(raw['AM_liters'], raw['PM_liters'], manual)
    print(f"    Stats shape: {stats_df.shape}", flush=True)

    # -----------------------------------------------------------------------
    # 4. Write all tabs to the native daily_milk Sheet
    # -----------------------------------------------------------------------
    all_tabs = {
        'PM_liters': raw['PM_liters'],
        'PM_wy':     raw['PM_wy'],
        'AM_liters': raw['AM_liters'],
        'AM_wy':     raw['AM_wy'],
        'stats':     stats_df,
        'fresh':     groups['fresh'],
        'group_A':   groups['group_A'],
        'group_B':   groups['group_B'],
        'group_C':   groups['group_C'],
        'sick':      groups['sick'],
    }

    print("\n[4] Writing all tabs to daily_milk Sheet...", flush=True)
    for tab_name, df in all_tabs.items():
        gdrive_write_sheet_tab(DAILY_MILK_SHEET_ID, tab_name, df)

    print("\nDone. daily_milk Sheet updated in Google Drive.", flush=True)

    # -----------------------------------------------------------------------
    # 5. Also save as daily_milk.xlsx in GDrive
    # -----------------------------------------------------------------------
    print("\n[5] Writing daily_milk.xlsx to GDrive...", flush=True)
    excel_bytes = build_excel(all_tabs)
    gdrive_write_excel('COWS/milk_data/daily_milk/daily_milk.xlsx', excel_bytes)
    print("Done. daily_milk.xlsx updated in Google Drive.", flush=True)


if __name__ == '__main__':
    main()
