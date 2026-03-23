"""Per-column data generation functions."""
import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

_fake = Faker()


def generate_pk(count: int, prefix: str = "", offset: int = 0) -> list:
    """Sequential prefixed IDs: CUST_000001, CUST_000002, …
    offset ensures IDs never repeat across incremental runs.
    """
    if prefix:
        return [f"{prefix}_{str(i + offset + 1).zfill(6)}" for i in range(count)]
    return [f"{i + offset + 1}" for i in range(count)]


def generate_faker(count: int, method: str) -> list:
    """Call any zero-argument Faker method `count` times."""
    fn = getattr(_fake, method)
    return [fn() for _ in range(count)]


def generate_choice(count: int, values: list, weights: list | None = None) -> list:
    return random.choices(values, weights=weights, k=count)


def generate_random_int(count: int, min_val: int, max_val: int) -> list:
    return np.random.randint(min_val, max_val + 1, size=count).tolist()


def generate_random_float(count: int, min_val: float, max_val: float, round_digits: int = 2) -> list:
    vals = np.random.uniform(min_val, max_val, size=count)
    return np.round(vals, round_digits).tolist()


def generate_date_range(count: int, start: str, end: str, sequential: bool = False) -> list:
    """
    Returns a list of date objects.
    - sequential=True  → one date per calendar day from start to end (count is ignored).
    - sequential=False → `count` random dates sampled uniformly from [start, end].
    """
    start_dt = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)

    if sequential:
        total_days = (end_dt - start_dt).days + 1
        return [start_dt + timedelta(days=i) for i in range(total_days)]

    delta_days = (end_dt - start_dt).days
    return [start_dt + timedelta(days=random.randint(0, delta_days)) for _ in range(count)]


def generate_items_per_parent(parent_pks: pd.Series, min_items: int, max_items: int) -> pd.Series:
    """For each parent PK generate between min and max child rows.
    Returns a Series of FK values — length is sum of all per-parent counts.
    Guarantees every parent has at least one child row.
    """
    counts = np.random.randint(min_items, max_items + 1, size=len(parent_pks))
    return pd.Series(np.repeat(parent_pks.values, counts), name=parent_pks.name)


def generate_fk(count: int, pk_series: pd.Series) -> list:
    """Sample `count` values (with replacement) from an already-generated PK column."""
    return pk_series.sample(n=count, replace=True).tolist()


def generate_derived(source_series: pd.Series, extract: str) -> list:
    """Derive a date component or weekend flag from an existing date column."""
    dt = pd.to_datetime(source_series)
    match extract:
        case "day":
            return dt.dt.day.tolist()
        case "month":
            return dt.dt.month.tolist()
        case "quarter":
            return dt.dt.quarter.tolist()
        case "year":
            return dt.dt.year.tolist()
        case "is_weekend":
            return (dt.dt.dayofweek >= 5).tolist()
        case _:
            raise ValueError(f"Unknown extract value: '{extract}'")


def generate_formula(df: pd.DataFrame, expression: str, round_digits: int | None = None) -> list:
    """Evaluate a pandas-style arithmetic expression against the current DataFrame columns."""
    result = df.eval(expression)
    if round_digits is not None:
        result = result.round(round_digits)
    return result.tolist()
