# runtime.py — lives at bos_backend root
import os
from pathlib import Path

def get_bos_root() -> Path:
    """
    Returns the project root regardless of whether we're running
    locally or inside a Modal container.
    """
    if os.environ.get("MODAL_ENVIRONMENT"):  # set automatically inside any Modal container
        return Path("/root/bos")
    return Path(__file__).resolve().parent