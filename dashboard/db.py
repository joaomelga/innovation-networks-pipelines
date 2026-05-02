"""Shared DuckDB connection and query utilities for Streamlit dashboards."""

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = REPO_ROOT / "outputs"

# Consistent color scheme: late-stage (set 0) = blue, early-stage (set 1) = red
SET_0_COLOR = "#3498db"
SET_1_COLOR = "#e74c3c"
SET_COLORS = {0: SET_0_COLOR, 1: SET_1_COLOR}
SET_LABELS = {0: "Late-stage (Set 0)", 1: "Early-stage (Set 1)"}

# Tables exported to HF Datasets as {experiment}/{schema}_{table}.parquet
HF_TABLES = [
    ("raw", "companies"),
    ("raw", "investors"),
    ("raw", "investments"),
    ("raw", "funding_rounds"),
    ("staging", "companies_clean"),
    ("staging", "investments_clean"),
    ("staging", "investments_funded"),
    ("core", "vc_investments"),
    ("core", "investment_pairs"),
    ("graph", "network"),
    ("graph", "edges"),
    ("experiment", "johnson_nestedness"),
]


def _hf_repo() -> str | None:
    """Return the HuggingFace dataset repo ID if HF mode is active."""
    return os.environ.get("HF_DATASET_REPO")


def discover_experiments() -> dict[str, str]:
    """Return a dict mapping experiment name -> DuckDB path or HF sentinel path.

    In HF mode, values are "hf://{name}" strings. In local mode, values are
    absolute paths to .duckdb files.
    """
    repo = _hf_repo()
    if repo:
        return _discover_experiments_hf(repo)
    return _discover_experiments_local()


def _discover_experiments_local() -> dict[str, Path]:
    experiments = {}
    if not OUTPUTS_DIR.exists():
        return experiments
    for run_dir in sorted(OUTPUTS_DIR.iterdir()):
        if not run_dir.is_dir():
            continue
        db_file = run_dir / "pipeline.duckdb"
        if db_file.exists():
            experiments[run_dir.name] = db_file
    return experiments


def _discover_experiments_hf(repo: str) -> dict[str, str]:
    from huggingface_hub import list_repo_tree, RepoFolder

    experiments = {}
    for item in list_repo_tree(repo, repo_type="dataset"):
        if isinstance(item, RepoFolder) and "/" not in item.path:
            experiments[item.path] = f"hf://{item.path}"
    return experiments


def get_selected_experiment() -> str | None:
    """Return the currently selected experiment name from session state."""
    return st.session_state.get("selected_experiment")


def get_db_path() -> str | None:
    """Return the DuckDB path (or HF sentinel) for the currently selected experiment."""
    exp_name = get_selected_experiment()
    if not exp_name:
        return None
    experiments = discover_experiments()
    db_path = experiments.get(exp_name)
    return str(db_path) if db_path else None


def experiment_selector(sidebar: bool = True) -> str | None:
    """Render an experiment selector widget. Returns selected experiment name."""
    experiments = discover_experiments()
    if not experiments:
        st.warning("No experiments found. Run a pipeline first.")
        return None

    container = st.sidebar if sidebar else st
    selected = container.selectbox(
        "Experiment",
        options=list(experiments.keys()),
        key="selected_experiment",
        help="Select which experiment pipeline's results to view",
    )
    return selected


def _setup_hf_views(conn: duckdb.DuckDBPyConnection, experiment: str, hf_repo: str) -> None:
    """Create schema+view aliases in an in-memory connection pointing to HF Parquet files."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    base = f"https://huggingface.co/datasets/{hf_repo}/resolve/main/{experiment}"
    for schema, table in HF_TABLES:
        url = f"{base}/{schema}_{table}.parquet"
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(
            f"CREATE VIEW IF NOT EXISTS {schema}.{table} AS "
            f"SELECT * FROM read_parquet('{url}')"
        )


@st.cache_resource
def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection cached per path/experiment.

    In HF mode (db_path starts with "hf://"), returns an in-memory connection
    with views pointing to Parquet files on HuggingFace. In local mode, returns
    a read-only connection to the .duckdb file.
    """
    if db_path.startswith("hf://"):
        experiment = db_path[len("hf://"):]
        conn = duckdb.connect()
        _setup_hf_views(conn, experiment, _hf_repo())
        return conn
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(ttl=600)
def _query_df_cached(sql: str, db_path: str) -> pd.DataFrame:
    """Cached query execution. Both arguments are part of the cache key."""
    con = get_connection(db_path)
    return con.execute(sql).fetchdf()


def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL against the currently selected experiment's DuckDB.

    Works in both local and HF mode — callers use schema.table SQL unchanged.
    """
    path = get_db_path()
    if not path:
        return pd.DataFrame()
    return _query_df_cached(sql, path)


def query_df_by_experiment(sql: str, exp_name: str) -> pd.DataFrame:
    """Execute SQL against a specific experiment by name, bypassing session state."""
    experiments = discover_experiments()
    db_path = experiments.get(exp_name)
    if not db_path:
        return pd.DataFrame()
    return _query_df_cached(sql, str(db_path))


def format_number(n: int | float) -> str:
    """Format large numbers with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
