import os

import duckdb
import pandas as pd


def get_db_path(default: str = "pipeline.duckdb") -> str:
    """Get DuckDB path from BRUIN_DUCKDB_PATH env var or default."""
    return os.environ.get("BRUIN_DUCKDB_PATH", default)


def read_table(table_name: str, db_path: str | None = None) -> pd.DataFrame:
    """Read a DuckDB table into a DataFrame (read-only connection)."""
    path = db_path or get_db_path()
    con = duckdb.connect(path, read_only=True)
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    con.close()
    return df
