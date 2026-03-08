"""@bruin
name: raw.funding_rounds
image: python:3.13
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

columns:
  - name: uuid
    type: string
    checks:
      - name: not_null
  - name: investment_type
    type: string
  - name: money_raised_usd
    type: float
  - name: announced_on
    type: string

@bruin"""

import pandas as pd
import os
from pathlib import Path

# Resolve data dir relative to this script's original location
RAW_DIR = os.environ.get("BRUIN_RAW_DIR", str(Path(__file__).resolve().parent.parent.parent / "data"))

def materialize():
    filepath = os.path.join(RAW_DIR, "funding_rounds.csv.gz")
    if not os.path.exists(filepath):
        filepath = os.path.join(RAW_DIR, "funding_rounds.csv")
    df = pd.read_csv(filepath, compression="gzip" if filepath.endswith(".gz") else None, encoding="utf-8")
    print(f"Loaded funding_rounds: {len(df)} rows, {len(df.columns)} columns")
    return df
