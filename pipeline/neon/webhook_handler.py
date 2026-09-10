'''pipeline/neon/webhook_handler.py'''
import json
from pathlib import Path
from pipeline.modal.occasional_modal import OccasionalModal
from pipeline.modal.daily_modal import DailyModal

MAP_PATH = Path(__file__).resolve().parent / "table_to_tasks.json"
TABLE_TO_TASKS = json.loads(MAP_PATH.read_text())

ORCHESTRATOR_CLASSES = {
    "occasional_modal": OccasionalModal,
    "daily_modal": DailyModal,
}

def handle_table_changed(changed_tables: list[str]):
    per_orch_targets = {}
    for t in changed_tables:
        for orch_name, tasks in TABLE_TO_TASKS.get(t, {}).items():
            per_orch_targets.setdefault(orch_name, set()).update(tasks)

    if not per_orch_targets:
        print(f"No pipeline tasks depend on {changed_tables} — nothing to do")
        return

    for orch_name, targets in per_orch_targets.items():
        cls = ORCHESTRATOR_CLASSES[orch_name]
        cls(targets=targets, branch="production").load_and_process()