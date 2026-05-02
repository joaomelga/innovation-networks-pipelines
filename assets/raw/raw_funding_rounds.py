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
import sys
from pathlib import Path

_ASSETS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))
from _lib.config import data_dir
RAW_DIR = str(data_dir())


def materialize():
    filepath = os.path.join(RAW_DIR, "funding_rounds.csv.gz")

    if not os.path.exists(filepath):
        filepath = os.path.join(RAW_DIR, "funding_rounds.csv")

    df = pd.read_csv(filepath, compression="gzip" if filepath.endswith(".gz") else None, encoding="utf-8")
    print(f"Loaded funding_rounds: {len(df)} rows, {len(df.columns)} columns")
    return df
