from __future__ import annotations
import os
from pathlib import Path

SUPPORTED_REGIONS = ("us", "fr", "eu")
SUPPORTED_METHODS = ("nestlon", "modularity")

# assets/_lib/config.py -> assets/_lib -> assets -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def region() -> str:
    r = os.environ.get("REGION", "us").lower()
    if r not in SUPPORTED_REGIONS:
        raise ValueError(f"Unsupported REGION={r!r}. Expected one of {SUPPORTED_REGIONS}.")
    return r


def clustering_method() -> str:
    m = os.environ.get("CLUSTERING_METHOD", "nestlon").lower()
    if m not in SUPPORTED_METHODS:
        raise ValueError(f"Unsupported CLUSTERING_METHOD={m!r}. Expected one of {SUPPORTED_METHODS}.")
    return m


def output_dir() -> Path:
    d = REPO_ROOT / "outputs" / region()
    d.mkdir(parents=True, exist_ok=True)
    return d


def duckdb_path() -> Path:
    override = os.environ.get("BRUIN_DUCKDB_PATH")
    return Path(override) if override else output_dir() / "pipeline.duckdb"


def data_dir() -> Path:
    override = os.environ.get("BRUIN_RAW_DIR")
    output_dir()  # ensure the run's output directory exists before ingestr writes the DuckDB
    return Path(override) if override else REPO_ROOT / "data" / region()


def figures_dir(subdir: str = "") -> Path:
    override = os.environ.get("BRUIN_FIGURES_DIR")
    base = Path(override) if override else output_dir() / "figures"
    return base / subdir if subdir else base
