# utils/db_retry.py
import time
import sys
from functools import wraps
from psycopg2 import OperationalError
from pandas.errors import DatabaseError

SPINNER = ['|', '/', '-', '\\']

def retry_db(max_retries=3, backoff_base=2, spinner=True):
    """Decorator: retry on DB connection errors with animated cursor."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (OperationalError, DatabaseError) as e:
                    last_exc = e
                    if attempt == max_retries:
                        print(f"\n✗ All {max_retries} attempts failed: {e}")
                        raise
                    delay = backoff_base ** attempt
                    if spinner:
                        for _ in range(int(delay * 10)):
                            sym = SPINNER[(attempt + _) % 4]
                            sys.stdout.write(f"\r⏳ Retry {attempt}/{max_retries} {sym} ")
                            sys.stdout.flush()
                            time.sleep(0.1)
                    else:
                        sys.stdout.write(f"\r⏳ Retry {attempt}/{max_retries} in {delay}s... ")
                        sys.stdout.flush()
                        time.sleep(delay)
            raise last_exc  # never reached but safe
        return wrapper
    return decorator