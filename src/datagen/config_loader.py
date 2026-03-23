"""Loads and returns the YAML configuration file."""
import yaml


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
