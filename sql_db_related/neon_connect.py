import os
from dotenv import load_dotenv
from sqlalchemy import create_engine as _create_engine

load_dotenv()

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = _create_engine(os.getenv("DATABASE_URL"))
    return _engine