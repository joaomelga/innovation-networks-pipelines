"""@bruin
name: raw.companies
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
      - name: unique
  - name: name
    type: string
  - name: founded_year
    type: float
  - name: status
    type: string
  - name: country_code
    type: string
  - name: category_groups_list
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
    print(f"RAW_DIR resolved to: {RAW_DIR}")
    filepath = os.path.join(RAW_DIR, "companies.csv.gz")

    if not os.path.exists(filepath):
        filepath = os.path.join(RAW_DIR, "companies.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Could not find companies data at {filepath} (RAW_DIR={RAW_DIR})")

    df = pd.read_csv(filepath, compression="gzip" if filepath.endswith(
        ".gz") else None, encoding="utf-8")

    print(f"Loaded companies: {len(df)} rows, {len(df.columns)} columns")
    return df
