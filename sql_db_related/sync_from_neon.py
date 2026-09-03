# sql_db_related/sync_from_neon.py

import os
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

load_dotenv()

neon_engine = create_engine(os.getenv("DATABASE_URL"))
local_engine = create_engine("postgresql://al:localdev@localhost:5432/neondb")

print("=" * 60)
print(f"Sync started: {datetime.now()}")
print("=" * 60)

inspector = inspect(neon_engine)
tables = inspector.get_table_names()
views = inspector.get_view_names()
print(f"\nFound on Neon: {len(tables)} tables, {len(views)} views")

# ── Step 1: drop existing local views first ─────────────────────────
print("\n--- Step 1: clearing local views (so table drops won't be blocked) ---")
local_inspector = inspect(local_engine)
existing_local_views = local_inspector.get_view_names()
print(f"Local views found before sync: {existing_local_views or '(none)'}")

with local_engine.begin() as conn:
    for view in existing_local_views:
        print(f"  Dropping local view {view}...")
        conn.execute(text(f'DROP VIEW IF EXISTS "{view}" CASCADE'))

# ── Step 2: sync tables ──────────────────────────────────────────────
print(f"\n--- Step 2: syncing {len(tables)} tables ---")
table_row_counts = {}

for table in tables:
    df = pd.read_sql_table(table, neon_engine)
    row_count = len(df)
    table_row_counts[table] = row_count

    dtype_map = {}
    for col in df.columns:
        sample = df[col].dropna()
        if len(sample) and isinstance(sample.iloc[0], (dict, list)):
            df[col] = df[col].apply(lambda v: json.dumps(v) if v is not None else None)
            dtype_map[col] = JSONB

    df.to_sql(table, local_engine, if_exists="replace", index=False, dtype=dtype_map or None)
    print(f"  {table}: {row_count} rows")

total_rows = sum(table_row_counts.values())
print(f"\nDone. Synced {len(tables)} tables, {total_rows} total rows.")

# ── Step 3: recreate views ──────────────────────────────────────────
print(f"\n--- Step 3: recreating {len(views)} views ---")
with local_engine.begin() as conn:
    for view in views:
        definition = inspector.get_view_definition(view)
        conn.execute(text(f'DROP VIEW IF EXISTS "{view}" CASCADE'))
        conn.execute(text(f'CREATE VIEW "{view}" AS {definition}'))
        print(f"  Recreated view {view}")

print(f"Done. Synced {len(views)} views.")

# ── Step 4: verify against local DB (sanity check, not just "no error") ─
print("\n--- Step 4: verifying local DB actually reflects the sync ---")
verify_inspector = inspect(local_engine)
local_tables_after = set(verify_inspector.get_table_names())
local_views_after = set(verify_inspector.get_view_names())

missing_tables = set(tables) - local_tables_after
missing_views = set(views) - local_views_after

if missing_tables:
    print(f"  ⚠ WARNING — tables missing locally after sync: {missing_tables}")
else:
    print(f"  ✓ All {len(tables)} tables present locally")

if missing_views:
    print(f"  ⚠ WARNING — views missing locally after sync: {missing_views}")
else:
    print(f"  ✓ All {len(views)} views present locally")

empty_tables = [t for t, n in table_row_counts.items() if n == 0]
if empty_tables:
    print(f"  ⚠ Note — synced but empty on Neon too (not a sync bug): {empty_tables}")

# ── Step 5: stamp sync metadata ─────────────────────────────────────
with local_engine.begin() as conn:
    conn.execute(text('''
        CREATE TABLE IF NOT EXISTS _sync_metadata (
            id INT PRIMARY KEY DEFAULT 1,
            last_synced_at TIMESTAMP,
            tables_synced INT,
            views_synced INT,
            total_rows INT
        )
    '''))
    conn.execute(text('''
        INSERT INTO _sync_metadata (id, last_synced_at, tables_synced, views_synced, total_rows)
        VALUES (1, :ts, :t, :v, :r)
        ON CONFLICT (id) DO UPDATE SET
            last_synced_at = :ts, tables_synced = :t, views_synced = :v, total_rows = :r
    '''), {"ts": datetime.now(), "t": len(tables), "v": len(views), "r": total_rows})

print("\n" + "=" * 60)
print(f"Sync complete: {datetime.now()}")
print("=" * 60)