"""
Manages state.json per domain — tracks dim and fact PK offsets across runs.

state.json structure:
{
  "domain": "retail",
  "run_count": 3,
  "last_run": "2026-03-22T14:00:00",
  "dims_directory": "./output/retail/dims",
  "format": "csv",
  "dim_offsets": {
    "dim_customer": 2000,
    "dim_product": 1000,
    "dim_store": 200
  },
  "fact_offsets": {
    "fact_orders": 150000,
    "fact_order_line_items": 450000
  }
}
"""
import json
import os
from datetime import datetime

_STATE_FILE = "state.json"


def state_path(domain_dir: str) -> str:
    return os.path.join(domain_dir, _STATE_FILE)


def read_state(domain_dir: str) -> dict | None:
    path = state_path(domain_dir)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def write_state(domain_dir: str, state: dict) -> None:
    os.makedirs(domain_dir, exist_ok=True)
    state["last_run"] = datetime.now().isoformat()
    with open(state_path(domain_dir), "w") as f:
        json.dump(state, f, indent=2)
    print(f"  State saved → {state_path(domain_dir)}")
