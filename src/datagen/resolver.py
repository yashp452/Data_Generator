"""Resolves table generation order via topological sort of FK dependencies."""
from collections import defaultdict, deque


def get_table_order(tables_config: dict) -> list[str]:
    """Return table names sorted so every FK's referenced table is generated first."""
    deps: dict[str, set] = defaultdict(set)

    for table_name, table_cfg in tables_config.items():
        for col_cfg in table_cfg["columns"].values():
            if col_cfg["type"] == "fk":
                ref_table = col_cfg["references"].split(".")[0]
                deps[table_name].add(ref_table)

    # Kahn's algorithm
    in_degree = {t: 0 for t in tables_config}
    adj: dict[str, list] = defaultdict(list)
    for table, predecessors in deps.items():
        for pred in predecessors:
            adj[pred].append(table)
            in_degree[table] += 1

    queue = deque(t for t in tables_config if in_degree[t] == 0)
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in adj[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(order) != len(tables_config):
        raise ValueError("Circular FK dependency detected in configuration.")

    return order
