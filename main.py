"""
CLI entry-point for the config-driven mock data generation framework.

- Pass one config to run a single domain.
- Pass multiple configs to run domains sequentially.
- Mode is auto-detected: no state.json → first run; state.json exists → subsequent run.

First run  : generates dims + facts, saves state.json
Subsequent : grows dims (new IDs appended) + adds new fact batch, updates state.json

Usage:
    python main.py --config configs/retail.yaml
    python main.py --config configs/retail.yaml --format csv
    python main.py --config configs/retail.yaml configs/cpg.yaml
"""
import argparse
import os
import time

import pandas as pd

from src.datagen.config_loader import load_config
from src.datagen.engine import generate_table, resolve_post_aggregates
from src.datagen.exporter import export, SUPPORTED_FORMATS
from src.datagen.resolver import get_table_order
from src.datagen.state import read_state, write_state


def main() -> None:
    parser = argparse.ArgumentParser(description="Config-driven mock data generator")
    parser.add_argument("--config", required=True, nargs="+", help="One or more domain YAML config files")
    parser.add_argument("--format", choices=list(SUPPORTED_FORMATS), help="Output format — overrides config")
    args = parser.parse_args()

    for config_path in args.config:
        print(f"\n{'═' * 60}")
        print(f"Config: {config_path}")
        print(f"{'═' * 60}")
        _run_domain(config_path, fmt_override=args.format)


def _run_domain(config_path: str, fmt_override: str | None) -> None:
    cfg = load_config(config_path)

    domain = cfg.get("domain", os.path.splitext(os.path.basename(config_path))[0])
    output_cfg = cfg.get("output", {})
    fmt = fmt_override or output_cfg.get("format", "parquet")
    base_dir = output_cfg.get("base_directory", "./output")

    domain_dir = os.path.join(base_dir, domain)
    dims_dir = os.path.join(domain_dir, "dims")
    facts_dir = os.path.join(domain_dir, "facts")

    tables_cfg: dict = cfg["tables"]
    generation_order = get_table_order(tables_cfg)

    state = read_state(domain_dir)
    is_first_run = state is None

    if is_first_run:
        print(f"\nDomain : {domain}  |  First run — generating dims + facts")
        state = {
            "domain": domain,
            "run_count": 0,
            "dims_directory": dims_dir,
            "format": fmt,
            "dim_offsets": {},
            "fact_offsets": {},
        }
    else:
        run_num = state["run_count"] + 1
        print(f"\nDomain : {domain}  |  Run #{run_num} — growing dims + adding facts")

    print(f"Format : {fmt}")
    print(f"Order  : {' → '.join(generation_order)}\n" + "─" * 60)

    generated: dict[str, pd.DataFrame] = {}
    total_start = time.time()

    # ── Dimensions ─────────────────────────────────────────────────────────────
    print("\n[Dimensions]")
    for table_name in generation_order:
        table_cfg = tables_cfg[table_name]
        if table_cfg.get("table_type") != "dimension":
            continue

        growable = table_cfg.get("growable", True)

        if not is_first_run and not growable:
            # Non-growable dims (e.g. dim_date): just load from disk, skip generation
            dim_file = os.path.join(dims_dir, f"{table_name}.{state['format']}")
            generated[table_name] = _read_file(dim_file, state["format"])
            print(f"  {table_name}: loaded {len(generated[table_name]):,} rows (fixed)")
            continue

        offset = state["dim_offsets"].get(table_name, 0)
        t0 = time.time()
        new_df = generate_table(table_name, table_cfg, generated, pk_offset=offset)

        if not is_first_run:
            # Append new rows to existing dim file
            existing_file = os.path.join(dims_dir, f"{table_name}.{fmt}")
            existing_df = _read_file(existing_file, fmt)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            export(combined_df, table_name, dims_dir, fmt)
            generated[table_name] = combined_df
            print(f"  {table_name}: +{len(new_df):,} new → {len(combined_df):,} total ({time.time()-t0:.2f}s)")
        else:
            export(new_df, table_name, dims_dir, fmt)
            generated[table_name] = new_df
            print(f"  {table_name}: {len(new_df):,} rows ({time.time()-t0:.2f}s)")

        state["dim_offsets"][table_name] = offset + len(new_df)

    # ── Facts ───────────────────────────────────────────────────────────────────
    run_number = state["run_count"] + 1
    run_label = f"run_{run_number:03d}"
    run_facts_dir = os.path.join(facts_dir, run_label)

    print(f"\n[Facts → {run_label}]")
    for table_name in generation_order:
        table_cfg = tables_cfg[table_name]
        if table_cfg.get("table_type") != "fact":
            continue

        offset = state["fact_offsets"].get(table_name, 0)
        t0 = time.time()
        df = generate_table(table_name, table_cfg, generated, pk_offset=offset)
        export(df, table_name, run_facts_dir, fmt)
        generated[table_name] = df
        state["fact_offsets"][table_name] = offset + len(df)
        print(f"  {table_name}: {len(df):,} rows, offset {offset:,} → {state['fact_offsets'][table_name]:,} ({time.time()-t0:.2f}s)")

    # ── Post-aggregate resolution (e.g. total_amount = sum of line_totals) ──────
    updated_tables = resolve_post_aggregates(tables_cfg, generated)
    if updated_tables:
        print(f"\n[Post-aggregates → re-exporting: {', '.join(updated_tables)}]")
        for table_name in updated_tables:
            table_type = tables_cfg[table_name].get("table_type", "fact")
            out_dir = dims_dir if table_type == "dimension" else run_facts_dir
            export(generated[table_name], table_name, out_dir, fmt)

    # ── Save state ──────────────────────────────────────────────────────────────
    state["run_count"] = run_number
    state["format"] = fmt
    write_state(domain_dir, state)

    print(f"\n{'─' * 60}")
    print(f"Domain '{domain}' done in {time.time() - total_start:.2f}s")


def _read_file(path: str, fmt: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Expected file not found: {path}")
    if fmt == "parquet":
        return pd.read_parquet(path)
    elif fmt == "csv":
        return pd.read_csv(path)
    elif fmt == "json":
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported format: {fmt}")


if __name__ == "__main__":
    main()
