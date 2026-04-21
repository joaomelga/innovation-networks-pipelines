"""@bruin
name: raw.funding_rounds
image: python:3.13
connection: duckdb-us-modularity

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
import yaml
from pathlib import Path

EXPERIMENT_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG = yaml.safe_load(open(EXPERIMENT_DIR / "config.yml"))
RAW_DIR = os.environ.get("BRUIN_RAW_DIR", str(
    (EXPERIMENT_DIR / CONFIG["data_dir"]).resolve()))


def materialize():
    filepath = os.path.join(RAW_DIR, "funding_rounds.csv.gz")

    if not os.path.exists(filepath):
        filepath = os.path.join(RAW_DIR, "funding_rounds.csv")

    df = pd.read_csv(filepath, compression="gzip" if filepath.endswith(".gz") else None, encoding="utf-8")
    print(f"Loaded funding_rounds: {len(df)} rows, {len(df.columns)} columns")
    return df
