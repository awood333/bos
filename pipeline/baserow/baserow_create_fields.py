"""
baserow_create_fields.py
Creates 71 fields in each of the 4 Baserow milk tables via JWT auth.
Safe to rerun — skips fields that already exist.

Requires in keyring:
    keyring.set_password('baserow', 'email',        'your@email.com')
    keyring.set_password('baserow', 'jwt_password', 'your-baserow-password')

Usage:
    python utilities/baserow_create_fields.py
"""

import keyring
import requests

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
EMAIL    = keyring.get_password("baserow", "email")
PASSWORD = keyring.get_password("baserow", "jwt_password")
BASE_URL = "https://api.baserow.io"

TABLES = {
    "am_liters": 1002658,
    "am_wy":     1003450,
    "pm_wy":     1003452,
    "pm_liters": 1003454,
}

FIELD_DEFS = (
    [{"name": "date", "type": "date", "date_format": "ISO"}]
  + [{"name": f"c{n}", "type": "number", "number_decimal_places": 2}
     for n in range(1, 70)]
)

# ─────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────
def get_jwt() -> str:
    resp = requests.post(
        f"{BASE_URL}/api/user/token-auth/",
        json={"username": EMAIL, "password": PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]

# ─────────────────────────────────────────────
# Field helpers
# ─────────────────────────────────────────────
def fetch_existing(auth_hdrs: dict, tid: int) -> set:
    resp = requests.get(
        f"{BASE_URL}/api/database/fields/table/{tid}/",
        headers=auth_hdrs,
        timeout=10,
    )
    resp.raise_for_status()
    return {f["name"] for f in resp.json()}


def post_field(auth_hdrs: dict, tid: int, fdef: dict) -> dict:
    resp = requests.post(
        f"{BASE_URL}/api/database/fields/table/{tid}/",
        headers=auth_hdrs,
        json=fdef,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if not EMAIL or not PASSWORD:
        print("❌ Credentials missing. Run:")
        print("   python -c \"import keyring; keyring.set_password('baserow', 'email', 'your@email.com')\"")
        print("   python -c \"import keyring; keyring.set_password('baserow', 'jwt_password', 'yourpassword')\"")
        raise SystemExit(1)

    print("🔑 Getting JWT token...")
    auth_headers = {"Authorization": f"JWT {get_jwt()}", "Content-Type": "application/json"}
    print("✅ Authenticated\n")

    for tname, table_id in TABLES.items():
        print(f"── {tname} (id {table_id}) ──────────────────")
        existing = fetch_existing(auth_headers, table_id)
        print(f"   📋 {len(existing)} existing field(s): {existing}")

        created = 0
        skipped = 0

        for field_def in FIELD_DEFS:
            if field_def["name"] in existing:
                skipped += 1
                continue
            result = post_field(auth_headers, table_id, field_def)
            print(f"   ✅ created '{result['name']}' ({result['type']})")
            created += 1

        print(f"   Done: {created} created, {skipped} skipped\n")

    print("✅ All tables complete")