from sqlalchemy import create_engine as _create_engine
from config_path import DATABASE_URL

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = _create_engine(DATABASE_URL)
    return _engine