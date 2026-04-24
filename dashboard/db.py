"""Shared DuckDB connection and query utilities for Streamlit dashboards."""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
EXPERIMENTS_DIR = REPO_ROOT / "experiments"

# Consistent color scheme: late-stage (set 0) = blue, early-stage (set 1) = red
SET_0_COLOR = "#3498db"
SET_1_COLOR = "#e74c3c"
SET_COLORS = {0: SET_0_COLOR, 1: SET_1_COLOR}
SET_LABELS = {0: "Late-stage (Set 0)", 1: "Early-stage (Set 1)"}


def discover_experiments() -> dict[str, Path]:
    """Scan experiments/ for directories containing a DuckDB file.

    Returns a dict mapping experiment name -> DuckDB file path.
    """
    experiments = {}
    if not EXPERIMENTS_DIR.exists():
        return experiments
    for exp_dir in sorted(EXPERIMENTS_DIR.iterdir()):
        if not exp_dir.is_dir():
            continue
        # Look for any .duckdb file in the experiment directory
        duckdb_files = list(exp_dir.glob("*.duckdb"))
        if duckdb_files:
            experiments[exp_dir.name] = duckdb_files[0]
    return experiments


def get_selected_experiment() -> str | None:
    """Return the currently selected experiment name from session state."""
    return st.session_state.get("selected_experiment")


def get_db_path() -> str | None:
    """Return the DuckDB file path for the currently selected experiment."""
    exp_name = get_selected_experiment()
    if not exp_name:
        return None
    experiments = discover_experiments()
    db_path = experiments.get(exp_name)
    return str(db_path) if db_path else None


def experiment_selector(sidebar: bool = True) -> str | None:
    """Render an experiment selector widget. Returns selected experiment name.

    Call this at the top of each page to ensure the experiment is selected.
    """
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


@st.cache_resource
def get_connection(db_path: str):
    """Return a read-only DuckDB connection (cached per path)."""
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(ttl=600)
def _query_df_cached(sql: str, db_path: str) -> pd.DataFrame:
    """Cached query execution. Both arguments are part of the cache key."""
    con = get_connection(db_path)
    return con.execute(sql).fetchdf()


def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL against the currently selected experiment's DuckDB.

    Resolves the active experiment path before hitting the cache, so switching
    experiments always queries the correct database.
    """
    path = get_db_path()
    if not path:
        return pd.DataFrame()
    return _query_df_cached(sql, path)


def format_number(n: int | float) -> str:
    """Format large numbers with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
