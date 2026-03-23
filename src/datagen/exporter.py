"""Exports a DataFrame to the requested file format."""
import os

import pandas as pd

SUPPORTED_FORMATS = {"parquet", "csv", "json"}


def export(df: pd.DataFrame, table_name: str, output_dir: str, fmt: str) -> None:
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Choose from: {', '.join(SUPPORTED_FORMATS)}")

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{table_name}.{fmt}")

    if fmt == "parquet":
        df.to_parquet(path, index=False)
    elif fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records", lines=True)

    print(f"  Exported {len(df):>10,} rows  →  {path}")
