"""@bruin
name: raw.investments
image: python:3.13
connection: duckdb-us-modularity

materialization:
  type: table
  strategy: create+replace

columns:
  - name: org_uuid
    type: string
    checks:
      - name: not_null
  - name: investor_uuid
    type: string
    checks:
      - name: not_null
  - name: investor_name
    type: string
  - name: investment_type
    type: string
  - name: announced_year
    type: float
  - name: total_funding_usd
    type: float
  - name: raised_amount_usd
    type: float
  - name: investor_types
    type: string
  - name: investor_country
    type: string
  - name: investor_region
    type: string
  - name: company_country
    type: string
  - name: funding_round_uuid
    type: string
  - name: category
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
    filepath = os.path.join(RAW_DIR, "investments.csv.gz")

    if not os.path.exists(filepath):
        filepath = os.path.join(RAW_DIR, "investments.csv")

    df = pd.read_csv(filepath, compression="gzip" if filepath.endswith(".gz") else None, encoding="utf-8")
    print(f"Loaded investments: {len(df)} rows, {len(df.columns)} columns")
    return df
