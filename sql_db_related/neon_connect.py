''' sql_db_related/neon_connect.py '''
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine as _create_engine

load_dotenv()

_engine = None

LOCAL_URL = "postgresql://al:localdev@localhost:5432/neondb"

#os.environ["BOS_LOCAL"] = "0" writes into the current Python process's environment table — 
# a dict-like structure the OS gives every process. It's not global, not the actual OS/shell 
# environment, not persisted anywhere — it exists only for the lifetime of that one python 
# daily_modal.py invocation and disappears the moment it exits. os.getenv("BOS_LOCAL", "1") 
# in neon_connect.py reads from that same table because it's running inside the same process 
# — Python modules within one script share one process-wide environment, 
# so setting it anywhere before get_engine() is called is visible everywhere downstream in that run. 
# Next time you run a different script, it starts fresh with no memory of this — 
# that's why daily_modal.py and occasional_modal.py each need their own copy of that line, rather than it being "set once."




def get_engine():
    global _engine
    if _engine is None:
        use_local = os.getenv("BOS_LOCAL", "1") == "1"
        url = LOCAL_URL if use_local else os.getenv("DATABASE_URL")
        _engine = _create_engine(url)
    return _engine