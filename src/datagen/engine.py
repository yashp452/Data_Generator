"""Core generation engine: walks resolved table order and builds each DataFrame."""
import pandas as pd

from . import generators as gen


def generate_table(table_name: str, table_cfg: dict, generated_tables: dict[str, pd.DataFrame], pk_offset: int = 0) -> pd.DataFrame:
    """
    Generate a single table as a DataFrame.

    Supports two generation modes:
    - Standard:          generates exactly row_count rows independently.
    - items_per_parent:  generates N rows per parent PK (guarantees no orphan parents).

    Columns of type post_aggregate are skipped here and resolved in a second pass
    via resolve_post_aggregates() after all tables are generated.
    """
    columns_cfg: dict = table_cfg["columns"]
    items_per_parent_cfg = table_cfg.get("items_per_parent")

    # ── items_per_parent mode ──────────────────────────────────────────────────
    if items_per_parent_cfg:
        fk_col = items_per_parent_cfg["fk_column"]
        parent_table = items_per_parent_cfg["parent_table"]
        min_items = items_per_parent_cfg["min"]
        max_items = items_per_parent_cfg["max"]

        parent_pks = generated_tables[parent_table][fk_col]
        fk_series = gen.generate_items_per_parent(parent_pks, min_items, max_items)
        row_count = len(fk_series)

        df = pd.DataFrame({fk_col: fk_series.values})
    else:
        # ── Standard mode ──────────────────────────────────────────────────────
        row_count = _resolve_row_count(table_cfg)
        df = pd.DataFrame()

    # ── Table-level Lookups ────────────────────────────────────────────────────
    table_lookups = {}
    for col_cfg in columns_cfg.values():
        if col_cfg.get("type") == "lookup":
            seed_file = col_cfg["seed_file"]
            if seed_file not in table_lookups:
                table_lookups[seed_file] = gen.get_seed_sample(
                    seed_file, row_count, weight_column=col_cfg.get("weight_column")
                )

    # ── Column generation ──────────────────────────────────────────────────────
    for col_name, col_cfg in columns_cfg.items():
        col_type = col_cfg["type"]

        # Skip FK column already set by items_per_parent
        if items_per_parent_cfg and col_name == items_per_parent_cfg["fk_column"]:
            continue

        # Skip post_aggregate — resolved in a second pass after all tables exist
        if col_type == "post_aggregate":
            continue

        if col_type == "pk":
            df[col_name] = gen.generate_pk(row_count, col_cfg.get("prefix", ""), pk_offset)

        elif col_type == "fk":
            ref_table, ref_col = col_cfg["references"].split(".")
            pk_series = generated_tables[ref_table][ref_col]
            df[col_name] = gen.generate_fk(row_count, pk_series)

        elif col_type == "fk_pareto":
            ref_table, ref_col = col_cfg["references"].split(".")
            pk_series = generated_tables[ref_table][ref_col]
            alpha = col_cfg.get("alpha", 1.2)
            df[col_name] = gen.generate_fk_pareto(row_count, pk_series, alpha=alpha)

        elif col_type == "lookup":
            seed_file = col_cfg["seed_file"]
            seed_col = col_cfg.get("seed_column", col_name)
            df[col_name] = table_lookups[seed_file][seed_col].values

        elif col_type == "faker":
            df[col_name] = gen.generate_faker(row_count, col_cfg["method"])

        elif col_type == "choice":
            df[col_name] = gen.generate_choice(row_count, col_cfg["values"], col_cfg.get("weights"))

        elif col_type == "random_int":
            df[col_name] = gen.generate_random_int(row_count, col_cfg["min"], col_cfg["max"])

        elif col_type == "random_float":
            df[col_name] = gen.generate_random_float(
                row_count, col_cfg["min"], col_cfg["max"], col_cfg.get("round", 2)
            )

        elif col_type == "date_range":
            dates = gen.generate_date_range(
                row_count, col_cfg["start"], col_cfg["end"], col_cfg.get("sequential", False)
            )
            df[col_name] = dates
            row_count = len(df[col_name])

        elif col_type == "derived":
            df[col_name] = gen.generate_derived(df[col_cfg["source"]], col_cfg["extract"])

        elif col_type == "formula":
            df[col_name] = gen.generate_formula(df, col_cfg["expression"], col_cfg.get("round"))

        else:
            raise ValueError(f"[{table_name}.{col_name}] Unknown column type: '{col_type}'")

    return df


def resolve_post_aggregates(tables_cfg: dict, generated: dict[str, pd.DataFrame]) -> list[str]:
    """
    Second pass: compute post_aggregate columns that depend on other generated tables.
    Returns list of table names that were updated (need to be re-exported).
    """
    updated_tables = []

    for table_name, table_cfg in tables_cfg.items():
        for col_name, col_cfg in table_cfg["columns"].items():
            if col_cfg["type"] != "post_aggregate":
                continue

            source_table = col_cfg["source_table"]
            source_col = col_cfg["source_column"]
            group_by = col_cfg["group_by"]
            func = col_cfg["func"]
            round_digits = col_cfg.get("round", 2)

            agg = (
                generated[source_table]
                .groupby(group_by)[source_col]
                .agg(func)
                .round(round_digits)
                .reset_index()
                .rename(columns={source_col: col_name})
            )

            df = generated[table_name].drop(columns=[col_name], errors="ignore")
            generated[table_name] = df.merge(agg, on=group_by, how="left")

            if table_name not in updated_tables:
                updated_tables.append(table_name)

    return updated_tables


def _resolve_row_count(table_cfg: dict) -> int:
    from datetime import date, timedelta

    for col_cfg in table_cfg["columns"].values():
        if col_cfg.get("type") == "date_range" and col_cfg.get("sequential", False):
            start_dt = date.fromisoformat(col_cfg["start"])
            end_dt = date.fromisoformat(col_cfg["end"])
            return (end_dt - start_dt).days + 1

    return table_cfg["row_count"]
