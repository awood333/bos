'''pipeline/neon/dump_table_map.py'''
import json
import inspect
from pathlib import Path
import networkx as nx
from container import container
from pipeline.modal.occasional_modal import OccasionalModal
from pipeline.modal.daily_modal import DailyModal   # placeholder import — confirm actual class/module name

# Registry: orchestrator key (used in table_to_tasks.json and by
# webhook_handler.py to route) -> (class, {task_name: container_dep_name})
ORCHESTRATORS = {
    "occasional_modal": (
        OccasionalModal,
        {
            "next_ultra_check":      "next_ultra_check",
            "i_u_merge":              "i_u_merge",
            "allx":                   "insem_ultra_data",
            "ipiv_data":               "ipiv_data",
            "feed_cost_pivot":        "finance_basics",
            "cost_xfeed_pivot":       "finance_basics",
            "ipiv_pivot_table":       "ipiv_pivot_table",
            "net_revenue":            "net_revenue",
            "daily_milk_vs_fullday":  "daily_milk_vs_fullday",
        },
    ),
    "daily_modal": (
        DailyModal,
        {
            # TODO — fill in once DailyModal's actual TASK_NAMES / targets
            # pattern (or equivalent) is confirmed
        },
    ),
}

IGNORED_TABLES = {"pg_catalog", "information_schema"}

# Run every orchestrator once, against dev-testing, so the shared
# graph accumulates edges from all of them before we read it.
for name, (cls, task_to_dep) in ORCHESTRATORS.items():
    print(f"--- running {name} ---")
    kwargs = {"branch": "dev-testing"} if "branch" in inspect.signature(cls).parameters else {}
    cls(**kwargs).load_and_process()

G = container._dependency_graph
table_to_tasks = {}

for node in G.nodes:
    if not node.startswith("table:"):
        continue
    table = node[len("table:"):]
    if table in IGNORED_TABLES:
        continue

    upstream = nx.ancestors(G, node)
    entry = {}
    for name, (cls, task_to_dep) in ORCHESTRATORS.items():
        tasks = sorted(t for t, dep in task_to_dep.items() if dep in upstream)
        if tasks:
            entry[name] = tasks
    if entry:
        table_to_tasks[table] = entry

OUT_PATH = Path(__file__).resolve().parent / "table_to_tasks.json"
with open(OUT_PATH, "w") as f:
    json.dump(table_to_tasks, f, indent=2)

print(f"Wrote {OUT_PATH}")
print(json.dumps(table_to_tasks, indent=2))