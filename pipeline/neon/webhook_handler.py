'''pipeline/neon/webhook_handler.py NOTE: no need to reg in Container'''
import json
from pathlib import Path
from pipeline.modal.occasional_modal import OccasionalModal

MAP_PATH = Path(__file__).resolve().parent / "table_to_tasks.json"
TABLE_TO_TASKS = json.loads(MAP_PATH.read_text())

def handle_table_changed(changed_tables: list[str]):
    """Called by whatever receives the Apps Script webhook payload,
    e.g. {"table": "live_births"} -> changed_tables=["live_births"]."""
    targets = set()
    for t in changed_tables:
        targets |= set(TABLE_TO_TASKS.get(t, []))
    if not targets:
        print(f"No pipeline tasks depend on {changed_tables} — nothing to do")
        return
    OccasionalModal(targets=targets, branch="production").load_and_process()