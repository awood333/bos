import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd

load_dotenv()

neon_engine = create_engine(os.getenv("DATABASE_URL"))
local_engine = create_engine("postgresql://al:localdev@localhost:5432/neondb")

inspector = inspect(neon_engine)
tables = inspector.get_table_names()

for table in tables:
    print(f"Syncing {table}...")
    df = pd.read_sql_table(table, neon_engine)

    dtype_map = {}
    for col in df.columns:
        # detect any column holding dict/list values (JSON-ish) and serialize it
        sample = df[col].dropna()
        if len(sample) and isinstance(sample.iloc[0], (dict, list)):
            df[col] = df[col].apply(lambda v: json.dumps(v) if v is not None else None)
            dtype_map[col] = JSONB

    df.to_sql(table, local_engine, if_exists="replace", index=False, dtype=dtype_map or None)

print(f"Done. Synced {len(tables)} tables.")
