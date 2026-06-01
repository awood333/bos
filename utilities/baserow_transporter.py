"""
baserow_transporter.py
Reads last 5 rows from each Neon milk table and pushes to Baserow.
Run daily before data entry to refresh the Baserow context window.

Flow:
    Neon (am_wy, am_liters, pm_wy, pm_liters)
        ↓ SELECT last 5 rows
    Baserow tables  (clears then repopulates each run)
        ↓ farm worker adds today's row
    baserow_collector.py pushes new row back to Neon

Requires in keyring:
    keyring.set_password('neon',    'database_url', 'postgresql://...')
    keyring.set_password('baserow', 'email',        'your@email.com')
    keyring.set_password('baserow', 'jwt_password', 'your-password')
"""

import keyring
import psycopg2
import psycopg2.extras
import requests
from datetime import date
from decimal import Decimal

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
DATABASE_URL = keyring.get_password("neon", "database_url")
BR_EMAIL     = keyring.get_password("baserow", "email")
BR_PASSWORD  = keyring.get_password("baserow", "jwt_password")
BR_BASE      = "https://api.baserow.io"

TABLES = {
    "am_liters": 1002658,
    "am_wy":     1003450,
    "pm_wy":     1003452,
    "pm_liters": 1003454,
}

LAST_N = 5

# ─────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────
def get_jwt() -> str:
    resp = requests.post(
        f"{BR_BASE}/api/user/token-auth/",
        json={"username": BR_EMAIL, "password": BR_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]

# ─────────────────────────────────────────────
# Neon
# ─────────────────────────────────────────────
def fetch_last_n(db_conn, table_name: str, n: int) -> list[dict]:
    with db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {table_name} ORDER BY date DESC LIMIT %s", (n,)
        )
        fetched = cur.fetchall()
    return [dict(r) for r in fetched]

# ─────────────────────────────────────────────
# Baserow
# ─────────────────────────────────────────────
def get_all_row_ids(hdrs: dict, table_id: int) -> list[int]:
    """Return all Baserow row IDs in a table."""
    ids = []
    url = f"{BR_BASE}/api/database/rows/table/{table_id}/?size=200"
    while url:
        resp = requests.get(url, headers=hdrs, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        ids.extend(r["id"] for r in data["results"])
        url = data.get("next")
    return ids


def delete_row(hdrs: dict, table_id: int, row_id: int):
    """Delete a single Baserow row by ID."""
    resp = requests.delete(
        f"{BR_BASE}/api/database/rows/table/{table_id}/{row_id}/",
        headers=hdrs,
        timeout=10,
    )
    resp.raise_for_status()


def clear_table(hdrs: dict, table_id: int):
    """Delete all rows in a Baserow table one by one."""
    row_ids = get_all_row_ids(hdrs, table_id)
    for rid in row_ids:
        delete_row(hdrs, table_id, rid)
    if row_ids:
        print(f"   🗑️  cleared {len(row_ids)} existing row(s)")


def push_row(hdrs: dict, table_id: int, neon_row: dict):
    """Push one row to Baserow."""
    payload = {
        k: v.isoformat() if isinstance(v, date) else float(v) if isinstance(v, Decimal) else v
        for k, v in neon_row.items()
    }
    resp = requests.post(
        f"{BR_BASE}/api/database/rows/table/{table_id}/?user_field_names=true",
        headers=hdrs,
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if not DATABASE_URL:
        print("❌ DATABASE_URL missing from keyring.")
        raise SystemExit(1)

    if not BR_EMAIL or not BR_PASSWORD:
        print("❌ Baserow credentials missing from keyring.")
        raise SystemExit(1)

    print("🔑 Authenticating to Baserow...")
    br_headers = {
        "Authorization": f"JWT {get_jwt()}",
        "Content-Type":  "application/json",
    }
    print("✅ Authenticated\n")

    print("🔌 Connecting to Neon...")
    conn = psycopg2.connect(DATABASE_URL)
    print("✅ Connected\n")

    try:
        for tname, br_table_id in TABLES.items():
            print(f"── {tname} ──────────────────────────────")

            neon_rows = fetch_last_n(conn, tname, LAST_N)
            print(f"   📥 {len(neon_rows)} rows fetched from Neon")

            clear_table(br_headers, br_table_id)

            for neon_row in reversed(neon_rows):
                push_row(br_headers, br_table_id, neon_row)

            print(f"   ✅ {len(neon_rows)} rows pushed to Baserow\n")

    finally:
        conn.close()
        print("🔌 Neon connection closed")

    print("✅ Transport complete")