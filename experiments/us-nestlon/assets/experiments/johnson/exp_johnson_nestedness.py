"""@bruin
name: experiment.johnson_nestedness
image: python:3.13
connection: duckdb-us-nestlon

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
from pathlib import Path

# Add repo root to path so lib/ is importable
EXPERIMENT_DIR = Path(__file__).resolve().parent.parent.parent.parent
REPO_ROOT = EXPERIMENT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from lib.utils.bipartite import compute_community_nestedness

DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", str(EXPERIMENT_DIR / "us_nestlon.duckdb"))


def materialize():
    con = duckdb.connect(DB_PATH, read_only=True)
    nodes_df = con.execute("SELECT * FROM graph.network").fetchdf()
    edges_df = con.execute("SELECT * FROM graph.edges").fetchdf()
    con.close()

    source_degrees = edges_df["Source"].value_counts()
    target_degrees = edges_df["Target"].value_counts()
    all_degrees = pd.concat([source_degrees, target_degrees]).groupby(level=0).sum()
    degree_map = all_degrees.to_dict()

    all_comm_ids = sorted(nodes_df["community_id"].unique())
    all_rows = []
    found_community = None
    
    count_comm_below_1 = 0

    # For test purposes: filtered_comm_ids = [comm_id for comm_id in all_comm_ids if str(comm_id) != "0"]
    for comm_id in all_comm_ids:
        result = compute_community_nestedness(nodes_df, edges_df, comm_id)
        if result is None:
            print(f"Community {comm_id}: skipped (empty)")
            continue

        g_norm = float(result["g_norm"])
        print(f"Community {comm_id}: g_norm={g_norm:.4f}")

        found_community = comm_id
        for i, node in enumerate(result["node_labels"]):
            all_rows.append({
                "node": node,
                "community": f"Community {comm_id}",
                "set": result["node_sets"][i],
                "degree": degree_map.get(node, 0),
                "local_g_norm": float(result["local_g_norm"][i]),
                "local_g_raw": float(result["local_g_raw"][i]),
                "g_norm": g_norm,
                "g_raw": float(result["g_raw"]),
                "g_conf": float(result["g_conf"]),
            })

        if g_norm < 1.0:
            count_comm_below_1 += 1
    
        if count_comm_below_1 >= 3:
            break

    df = pd.DataFrame(all_rows)
    if found_community is None:
        print("\nNo community found with g_norm > 1.0")
    else:
        print(f"\nFirst community below random expectation: Community {found_community - count_comm_below_1} ({len(df)} nodes)")
    return df
