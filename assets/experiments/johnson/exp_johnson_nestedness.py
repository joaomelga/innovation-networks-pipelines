"""@bruin
name: experiment.johnson_nestedness
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Calculate Johnson et al. (2013) nestedness for all clustering methods stored in graph.network.
  Produces per-node local nestedness metrics (g_norm, g_raw) alongside degree,
  with a clustering_method column to distinguish results.

columns:
  - name: clustering_method
    type: string
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

_ASSETS_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))

from _lib.config import duckdb_path
from lib.utils.bipartite import compute_community_nestedness

DB_PATH = str(duckdb_path())


def materialize():
    con = duckdb.connect(DB_PATH, read_only=True)
    nodes_df = con.execute("SELECT * FROM graph.network").fetchdf()
    edges_df = con.execute("SELECT * FROM graph.edges").fetchdf()
    con.close()

    all_methods = sorted(nodes_df["clustering_method"].unique())
    all_rows = []

    for method in all_methods:
        print(f"\n--- Processing method: {method} ---")
        method_nodes = nodes_df[nodes_df["clustering_method"] == method].copy()
        method_edges = edges_df[edges_df["clustering_method"] == method].copy()

        source_degrees = method_edges["Source"].value_counts()
        target_degrees = method_edges["Target"].value_counts()
        all_degrees = pd.concat([source_degrees, target_degrees]).groupby(level=0).sum()
        degree_map = all_degrees.to_dict()

        all_comm_ids = sorted(method_nodes["community_id"].unique())

        if method == "modularity":
            # target_communities = [c for c in [0, 1, 2] if c in all_comm_ids]

            for comm_id in all_comm_ids:
                print(f"\nProcessing Community {comm_id}...")
                result = compute_community_nestedness(method_nodes, method_edges, comm_id)
                if result is None:
                    continue

                for i, node in enumerate(result["node_labels"]):
                    all_rows.append({
                        "clustering_method": method,
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

        else:  # nestlon: iterate all communities, stop after 3 fall below random
            count_comm_below_1 = 0
            for comm_id in all_comm_ids:
                result = compute_community_nestedness(method_nodes, method_edges, comm_id)
                if result is None:
                    print(f"Community {comm_id}: skipped (empty)")
                    continue

                g_norm = float(result["g_norm"])
                print(f"Community {comm_id}: g_norm={g_norm:.4f}")

                for i, node in enumerate(result["node_labels"]):
                    all_rows.append({
                        "clustering_method": method,
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
    if not df.empty:
        for method in all_methods:
            m_df = df[df["clustering_method"] == method]
            print(f"\n{method}: {len(m_df)} rows across {m_df['community'].nunique()} communities")
    return df
