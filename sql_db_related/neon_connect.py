''' sql_db_related/neon_connect.py '''
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine as _create_engine

load_dotenv()

_engine = None

LOCAL_URL = "postgresql://al:localdev@localhost:5432/neondb"

def get_engine():
    global _engine
    if _engine is None:
        use_local = os.getenv("BOS_LOCAL", "1") == "1"
        url = LOCAL_URL if use_local else os.getenv("DATABASE_URL")
        _engine = _create_engine(url)
    return _engine