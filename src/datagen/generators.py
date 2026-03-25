"""Per-column data generation functions."""
import os
import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

_fake = Faker()
_LOOKUP_CACHE = {}

def get_seed_sample(seed_file: str, count: int, weight_column: str | None = None) -> pd.DataFrame:
    """Load a seed CSV and sample `count` rows with replacement. Cached for performance."""
    if seed_file not in _LOOKUP_CACHE:
        path = os.path.join("seeds", seed_file)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing seed dataset: {path}")
        _LOOKUP_CACHE[seed_file] = pd.read_csv(path)
        
    df = _LOOKUP_CACHE[seed_file]
    
    weights = df[weight_column] if weight_column and weight_column in df.columns else None
    
    # Sample and reset index to act as a direct alignable array
    return df.sample(n=count, replace=True, weights=weights).reset_index(drop=True)

def generate_pk(count: int, prefix: str = "", offset: int = 0) -> list:
    """Sequential prefixed IDs."""
    if prefix:
        return [f"{prefix}_{str(i + offset + 1).zfill(6)}" for i in range(count)]
    return [f"{i + offset + 1}" for i in range(count)]


def generate_faker(count: int, method: str) -> list:
    """Generates a small faker pool and vectorizes selection for high volume."""
    # To prevent massive purely python loop overhead, pool unique values up to 5k
    pool_size = min(count, 5000)
    fn = getattr(_fake, method)
    
    # Generate pool
    pool = list(set(fn() for _ in range(int(pool_size * 1.5))))[:pool_size]
    if not pool:
        pool = ["Unknown"]
        
    # High-speed numpy vectorized random selection
    return np.random.choice(pool, size=count).tolist()


def generate_choice(count: int, values: list, weights: list | None = None) -> list:
    return random.choices(values, weights=weights, k=count)


def generate_random_int(count: int, min_val: int, max_val: int) -> list:
    return np.random.randint(min_val, max_val + 1, size=count).tolist()


def generate_random_float(count: int, min_val: float, max_val: float, round_digits: int = 2) -> list:
    vals = np.random.uniform(min_val, max_val, size=count)
    return np.round(vals, round_digits).tolist()


def generate_date_range(count: int, start: str, end: str, sequential: bool = False) -> list:
    """Returns a list of date objects. Vectorized using pandas/numpy for speed."""
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    if sequential:
        total_days = (end_dt - start_dt).days + 1
        return [start_dt.date() + timedelta(days=i) for i in range(total_days)]

    delta_days = (end_dt - start_dt).days
    random_days = np.random.randint(0, delta_days + 1, size=count)
    # Vectorized fast addition and conversion back to python date objects
    return (start_dt + pd.to_timedelta(random_days, unit='D')).date.tolist()


def generate_items_per_parent(parent_pks: pd.Series, min_items: int, max_items: int) -> pd.Series:
    """For each parent PK generate between min and max child rows."""
    counts = np.random.randint(min_items, max_items + 1, size=len(parent_pks))
    return pd.Series(np.repeat(parent_pks.values, counts), name=parent_pks.name)


def generate_fk(count: int, pk_series: pd.Series) -> list:
    """Sample `count` values (with replacement) uniformly from an already-generated PK column."""
    return pk_series.sample(n=count, replace=True).tolist()


def generate_fk_pareto(count: int, pk_series: pd.Series, alpha: float = 1.2) -> list:
    """
    Sample using a Zipf (discrete Pareto) distribution to mimic an 80/20 rule.
    A small fraction of IDs will appear very frequently.
    """
    if len(pk_series) == 0:
        return []
    
    # zipf starts at 1, so subtract 1 for 0-based array indexing
    # We clip to avoid out-of-bounds error since Zipf has an infinite tail
    indices = np.random.zipf(alpha, size=count) - 1
    indices = np.clip(indices, 0, len(pk_series) - 1)
    
    # We don't shuffle pk_series by default, so ID #1 is always the biggest power-user.
    # To make it random, we could shuffle, but keeping it stable is easier to debug.
    return pk_series.iloc[indices].tolist()


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
