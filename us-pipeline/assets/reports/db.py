"""Shared DuckDB connection and query utilities for Streamlit dashboards."""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DB_PATH = str(Path(__file__).resolve().parent.parent.parent / "us_pipeline.duckdb")

# Consistent color scheme: late-stage (set 0) = blue, early-stage (set 1) = red
SET_0_COLOR = "#3498db"
SET_1_COLOR = "#e74c3c"
SET_COLORS = {0: SET_0_COLOR, 1: SET_1_COLOR}
SET_LABELS = {0: "Late-stage (Set 0)", 1: "Early-stage (Set 1)"}


@st.cache_resource
def get_connection():
    """Return a read-only DuckDB connection (cached per session)."""
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(ttl=600)
def query_df(sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame (cached for 10 min)."""
    con = get_connection()
    return con.execute(sql).fetchdf()


def format_number(n: int | float) -> str:
    """Format large numbers with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))
