"""
config_path.py
Central configuration for the BOS project.
  - Paths       : OS-detected, no env var needed
  - Secrets     : loaded from .env (never hardcoded, never in GitHub)
  - App settings: CORS, debug flags, etc.

Usage in any module:
    from config_path import PLOTS_DIR, DATABASE_URL, S3_BUCKET
"""

import os
import platform
from pathlib import Path

from dotenv import load_dotenv

# ── Load .env (silently ignored if not present, e.g. on a cloud platform) ──
load_dotenv()


# ══════════════════════════════════════════════════════════════════
# 1. ENVIRONMENT DETECTION
# ══════════════════════════════════════════════════════════════════
IS_WINDOWS = platform.system() == "Windows"
IS_LINUX   = platform.system() == "Linux"
DEBUG      = os.getenv("DEBUG", "false").lower() == "true"


# ══════════════════════════════════════════════════════════════════
# 2. PATHS  (OS-detected — no env var required for paths)
# ══════════════════════════════════════════════════════════════════
if IS_WINDOWS:
    DATA_ROOT = Path(r"Q:/My Drive/COWS")
else:
    # rclone mount point on Ubuntu — adjust if yours differs
    DATA_ROOT = Path("/mnt/gdrive/COWS")

# ── GDrive-relative path helper (for gdrive_read_csv which expects "COWS/...") ──
# On Windows: Q:/My Drive/COWS  →  relative to "My Drive" = "COWS"
# On Linux:   /mnt/gdrive/COWS  →  relative to gdrive root = "COWS"
GDRIVE_REL_ROOT = "COWS"

def gdrive_rel(path: Path) -> str:
    """Convert a DATA_ROOT-based Path to a gdrive_read_csv-compatible relative string.
    Accepts either an absolute path under DATA_ROOT or a path relative to DATA_ROOT.
    """
    path = Path(path)
    try:
        rel = path.relative_to(DATA_ROOT)
    except ValueError:
        rel = path  # already relative
    return GDRIVE_REL_ROOT + "/" + rel.as_posix()

# ── Sub-directories (GDrive mount — used for reading raw source files) ──

BASIC_DATA_DIR  = DATA_ROOT / "basic_data"
FEED_DATA_DIR   = DATA_ROOT / "feed_data"
MILK_DATA_DIR   = DATA_ROOT / "milk_data"
GROUP_DATA_DIR  = DATA_ROOT / "wb_groups"

GDRIVE_FEED_INVOICE_DATA  = FEED_DATA_DIR / "feed_invoice_data"
GDRIVE_FEED_DAILY_AMT_DATA = FEED_DATA_DIR / "feed_daily_amt_data"
GDRIVE_FEED_CSV           = FEED_DATA_DIR / "feed_csv"

RAW_DIR         = MILK_DATA_DIR / "raw"
TOTALS_DIR      = MILK_DATA_DIR / "totals" / "milk_aggregates"


# ── Finance data files on GDrive (cross-platform) ──
GDRIVE_BKKBANK_DIR = DATA_ROOT / "finance" / "BKKbank"
GDRIVE_CAPEX_DIR = DATA_ROOT / "finance" / "capex" / "depreciation_schedule"
GDRIVE_ASG_MILK_INCOME_DIR = DATA_ROOT / "finance" / "ASG_milk_income"

GDRIVE_BKKBANK_ACCOUNT = GDRIVE_BKKBANK_DIR / "BKKBankFarmAccount.csv"
GDRIVE_DEPRECIATION_SCHEDULE = GDRIVE_CAPEX_DIR / "depreciation_schedule.csv"
GDRIVE_MILK_INCOME_DATA = GDRIVE_ASG_MILK_INCOME_DIR / "milk_income_data.csv"


# ══════════════════════════════════════════════════════════════════
# 2b. LOCAL STORAGE PATHS  (OS-detected local disk, not GDrive)
#     Windows : E:/COWS/data
#     Linux   : ~/cows_data  (adjust to your mount point)
# ══════════════════════════════════════════════════════════════════
if IS_WINDOWS:
    LOCAL_DATA_ROOT = Path(r"D:/Cow_backup")
else:
    LOCAL_DATA_ROOT = Path.home() / "cows_data"
    
LOCAL_BASIC_DATA = LOCAL_DATA_ROOT / "basic_data"
BIRTH_DEATH_DIR         = LOCAL_BASIC_DATA / "birth_death"
HEIFER_BIRTH_DEATH_DIR  = LOCAL_BASIC_DATA / "heifer_birth_death"
INSEM_DIR               = LOCAL_BASIC_DATA / "insem"
LIVE_BIRTHS_DIR         = LOCAL_BASIC_DATA / "live_births"
STOP_DATES_DIR          = LOCAL_BASIC_DATA / "stop_dates"
ULTRA_DIR               = LOCAL_BASIC_DATA / "ultra"


LOCAL_MILK_DIR = LOCAL_DATA_ROOT / "milk_data"
LOCAL_FULLDAY_DIR       = LOCAL_MILK_DIR  / "fullday"
LOCAL_TOTALS_DIR        = LOCAL_MILK_DIR  / "totals" / "milk_aggregates"
LOCAL_LACTS_DIR         = LOCAL_MILK_DIR / "lactations" / "daily"
LOCAL_THIS_LACT_DIR     = LOCAL_MILK_DIR / "lactations" / "weekly"

LOCAL_FEED_DATA = LOCAL_DATA_ROOT / "feed_data"
LOCAL_FEED_COMPOSITION      = LOCAL_FEED_DATA / "composition_of_feed"
LOCAL_FEED_CONSUMPTION      = LOCAL_FEED_DATA / "feed_consumption"
LOCAL_FEED_CSV              = LOCAL_FEED_DATA / "feed_csv"
LOCAL_FEED_INVOICE_DATA     = LOCAL_FEED_DATA / "feed_invoice_data"
LOCAL_FEED_DAILY_AMT_DATA   = LOCAL_FEED_DATA / "feed_daily_amt_data"
LOCAL_FEED_RATIONS_ASG_CP   = LOCAL_FEED_DATA / "feed_rations_from_ASG_CP"
LOCAL_FEEDCOST_BY_GROUP     = LOCAL_FEED_DATA / "feedcost_by_group"
LOCAL_HEIFERS               = LOCAL_FEED_DATA / "heifers"
LOCAL_INVENTORY             = LOCAL_FEED_DATA / "inventory"
LOCAL_NUTRITION_DATA        = LOCAL_FEED_DATA / "nutrition_data"

LOCAL_FINANCE_DATA  = LOCAL_DATA_ROOT / "finance_data"
LOCAL_BKKBANK       = LOCAL_FINANCE_DATA / "BKKbank"
LOCAL_CAPEX         = LOCAL_FINANCE_DATA / "capex"
LOCAL_PROJECTS      = LOCAL_FINANCE_DATA / "projects"
LOCAL_ASG_MILK_INCOME=LOCAL_FINANCE_DATA / "ASG_milk_income"

LOCAL_STATUS        = LOCAL_DATA_ROOT / "status"
LOCAL_WET_DRY       = LOCAL_STATUS / "wet_dry"
LOCAL_REPORTS_DIR   = LOCAL_DATA_ROOT / "reports"

LOCAL_INSEM_DATA    = LOCAL_DATA_ROOT / "insem_data"
LOCAL_IU_MERGE_DIR  = LOCAL_INSEM_DATA / "IU_merge"

LOCAL_PLOTS_DIR     = LOCAL_DATA_ROOT / "plots"













# ══════════════════════════════════════════════════════════════════
# 3. DATABASE  (Neon / Postgres)
# ══════════════════════════════════════════════════════════════════
DATABASE_URL = os.getenv("DATABASE_URL")          # required — must be in .env


# ══════════════════════════════════════════════════════════════════
# 4. S3 / OBJECT STORAGE
# ══════════════════════════════════════════════════════════════════
S3_BUCKET          = os.getenv("S3_BUCKET")
S3_ACCESS_KEY      = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY      = os.getenv("S3_SECRET_KEY")
S3_REGION          = os.getenv("S3_REGION", "ap-southeast-1")


# ══════════════════════════════════════════════════════════════════
# 5. GOOGLE SERVICE ACCOUNT
# ══════════════════════════════════════════════════════════════════
GOOGLE_SA_JSON     = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")  # path to JSON key file


# ══════════════════════════════════════════════════════════════════
# 6. FASTAPI / APP SETTINGS
# ══════════════════════════════════════════════════════════════════
if IS_WINDOWS:
    CORS_ORIGINS = ["http://localhost:3000", "http://127.0.0.1:3000"]
else:
    CORS_ORIGINS = [
        "http://localhost:3000",
        os.getenv("FRONTEND_URL", "https://your-vercel-domain.vercel.app"),
    ]

API_TITLE   = "BOS Local API"
API_VERSION = "0.1.0"


# ══════════════════════════════════════════════════════════════════
# 7. SANITY CHECK  (run `python config_path.py` to verify)
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # .env diagnostic
    env_path = Path(__file__).parent / ".env"
    print(f"\n.env path  : {env_path}")
    print(f".env exists: {env_path.exists()}")
    with open(env_path, 'rb') as f:
        first_bytes = f.read(20)
    print(f"First bytes: {first_bytes}")
    has_bom = first_bytes[:3] == b'\xef\xbb\xbf'
    print(f"BOM present: {has_bom}")
    if has_bom:
        print("  --> Stripping BOM and re-saving...")
        with open(env_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print("  --> Done. Re-loading...")
        load_dotenv(dotenv_path=env_path, override=True)

    from dotenv import dotenv_values
    vals = dotenv_values(env_path)
    print(f"\nVariables found in .env: {len(vals)}")
    for k in vals:
        print(f"  {k}")

    print(f"\nPlatform   : {platform.system()}")
    print(f"DEBUG      : {DEBUG}")
    print(f"DATA_ROOT  : {DATA_ROOT}  (exists: {DATA_ROOT.exists()})")
    print(f"PLOTS_DIR  : {NET_REV_PLOTS}  (exists: {NET_REV_PLOTS.exists()})")
    print(f"DATABASE   : {'SET' if DATABASE_URL else '*** MISSING ***'}")
    print(f"S3_BUCKET  : {'SET' if S3_BUCKET else '*** MISSING ***'}")
    print(f"GOOGLE_SA  : {'SET' if GOOGLE_SA_JSON else '*** MISSING ***'}")
    print(f"CORS       : {CORS_ORIGINS}")