"""@bruin
name: experiment.johnson_nestedness
image: python:3.13
connection: duckdb-us-modularity

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Calculate Johnson et al. (2013) nestedness for the target communities.
  Produces per-node local nestedness metrics (g_norm, g_raw) alongside degree.

columns:
  - name: node
    type: string
  - name: community
    type: string
  - name: set
    type: integer
  - name: degree
    type: integer
  - name: local_g_norm
    type: float
  - name: local_g_raw
    type: float
  - name: g_norm
    type: float
  - name: g_raw
    type: float
  - name: g_conf
    type: float

@bruin"""

import pandas as pd
import duckdb
import os
import sys
import yaml
from pathlib import Path

# Add repo root to path so lib/ is importable
EXPERIMENT_DIR = Path(__file__).resolve().parent.parent.parent.parent
REPO_ROOT = EXPERIMENT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from lib.utils.bipartite import compute_community_nestedness

CONFIG = yaml.safe_load(open(EXPERIMENT_DIR / "config.yml"))
DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", str(EXPERIMENT_DIR / "us_modularity.duckdb"))
TARGET_COMMUNITIES = CONFIG.get("target_communities", [0, 1, 2])


def materialize():
    con = duckdb.connect(DB_PATH, read_only=True)
    nodes_df = con.execute("SELECT * FROM graph.network").fetchdf()
    edges_df = con.execute("SELECT * FROM graph.edges").fetchdf()
    con.close()

    # Calculate degree for each node
    source_degrees = edges_df["Source"].value_counts()
    target_degrees = edges_df["Target"].value_counts()
    all_degrees = pd.concat([source_degrees, target_degrees]).groupby(level=0).sum()
    degree_map = all_degrees.to_dict()

    all_rows = []
    for comm_id in TARGET_COMMUNITIES:
        print(f"\nProcessing Community {comm_id}...")
        result = compute_community_nestedness(nodes_df, edges_df, comm_id)
        if result is None:
            continue

        for i, node in enumerate(result["node_labels"]):
            all_rows.append({
                "node": node,
                "community": f"Community {comm_id}",
                "set": result["node_sets"][i],
                "degree": degree_map.get(node, 0),
                "local_g_norm": float(result["local_g_norm"][i]),
                "local_g_raw": float(result["local_g_raw"][i]),
                "g_norm": float(result["g_norm"]),
                "g_raw": float(result["g_raw"]),
                "g_conf": float(result["g_conf"]),
            })

    df = pd.DataFrame(all_rows)
    print(f"\nJohnson nestedness table: {len(df)} rows across {df['community'].nunique()} communities")
    return df
