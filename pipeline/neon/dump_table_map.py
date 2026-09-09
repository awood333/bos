'''pipeline/neon/dump_table_map.py — NEW FILE'''
import json
import networkx as nx
from container import container
from pipeline.modal.occasional_modal import OccasionalModal

# Run every task once, against dev-testing, so the graph reflects
# every dependency->dependency and dependency->table edge across
# the whole pipeline, not just whichever subset ran previously.
OccasionalModal(branch='dev-testing').load_and_process()

G = container._dependency_graph

# task name -> the container dependency name that OccasionalModal.load_and_process()
# asks for on that task's behalf (read straight off occasional_modal.py)
TASK_TO_DEP = {
    "next_ultra_check":      "next_ultra_check",
    "i_u_merge":              "i_u_merge",
    "allx":                   "insem_ultra_data",
    "ipiv_data":               "ipiv_data",
    "feed_cost_pivot":        "finance_basics",
    "cost_xfeed_pivot":       "finance_basics",
    "ipiv_pivot_table":       "ipiv_pivot_table",
    "net_revenue":            "net_revenue",
    "daily_milk_vs_fullday":  "daily_milk_vs_fullday",
}

table_to_tasks = {}
IGNORED_TABLES = {"pg_catalog", "information_schema"}

for node in G.nodes:
    if not node.startswith("table:"):
        continue
    table = node[len("table:"):]
    if table in IGNORED_TABLES:
        continue
    upstream = nx.ancestors(G, node)  # every dependency that transitively reads this table
    table_to_tasks[table] = sorted(
        task for task, dep in TASK_TO_DEP.items() if dep in upstream
    )

with open("table_to_tasks.json", "w") as f:
    json.dump(table_to_tasks, f, indent=2)

print(json.dumps(table_to_tasks, indent=2))