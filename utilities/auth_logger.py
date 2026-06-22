"""
auth_logger.py
BOS-side auth event emitter.

Writes auth events as JSON lines to the shared Diotima log,
so Diotima can observe BOS authentication activity in real time.

Usage — drop one line wherever BOS touches a credential:

    from utilities.auth_logger import auth_event

    auth_event("gdrive",   "INFO",  "Drive service ready")
    auth_event("keyring",  "OK",    "Loaded token from WinVaultKeyring")
    auth_event("gdrive",   "WARN",  "Token expired — refreshing")
    auth_event("s3",       "CRITICAL", "Failed to authenticate", detail=str(e))

The log file is the same one Diotima writes to — one shared log,
two writers (Diotima = OS landscape, BOS = app auth events).
"""

import json
from datetime import datetime
from pathlib import Path

# ───────────────────────────────────────────────
# Shared log root — must match diotima_logger.py
# ───────────────────────────────────────────────
LOG_ROOT = Path.home() / "diotima_log"


def _today_path() -> Path:
    """Today's shared log file — same file Diotima writes to."""
    stamp = datetime.now().strftime("%Y-%m-%d")
    return LOG_ROOT / f"vaultmap_{stamp}.log"


def auth_event(
    source:  str,
    level:   str,
    message: str,
    detail:  str | None = None,
) -> None:
    """
    Write one auth event to the shared Diotima log.

    Args:
        source:  which BOS subsystem is speaking
                 e.g. "gdrive", "keyring", "neon", "s3", "baserow"
        level:   OK | INFO | WARN | CRITICAL
        message: what happened
        detail:  optional extra context (exception text, token type, etc.)

    Fails silently — BOS should never crash because logging failed.
    """
    try:
        LOG_ROOT.mkdir(parents=True, exist_ok=True)
        line = json.dumps({
            "ts":      datetime.now().isoformat(),
            "level":   level,
            "source":  f"bos:{source}",      # prefixed so Diotima knows origin
            "message": message,
            "detail":  detail,
        })
        with open(_today_path(), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass            # logging must never crash the app


# ─────────────────────────────────────────────
# Convenience wrappers — less typing at call sites
# ─────────────────────────────────────────────
def auth_ok(source: str, message: str, detail: str | None = None) -> None:
    auth_event(source, "OK", message, detail)

def auth_info(source: str, message: str, detail: str | None = None) -> None:
    auth_event(source, "INFO", message, detail)

def auth_warn(source: str, message: str, detail: str | None = None) -> None:
    auth_event(source, "WARN", message, detail)

def auth_critical(source: str, message: str, detail: str | None = None) -> None:
    auth_event(source, "CRITICAL", message, detail)