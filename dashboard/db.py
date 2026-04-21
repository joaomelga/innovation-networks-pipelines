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
def query_df(sql: str, _db_path: str | None = None) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame (cached for 10 min).

    Uses the currently selected experiment's DuckDB unless _db_path is given.
    The _db_path parameter is prefixed with _ so Streamlit doesn't hash it.
    """
    path = _db_path or get_db_path()
    if not path:
        return pd.DataFrame()
    con = get_connection(path)
    return con.execute(sql).fetchdf()


def format_number(n: int | float) -> str:
    """Format large numbers with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
