"""@bruin
name: experiment.johnson_nestedness_weighted
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Weighted Johnson et al. (2013) nestedness per community and clustering method.
  Edge weight = number of distinct portfolio companies per (early, late) pair.

  The math is the k → s generalization of binary Johnson:
    g^W_conf = (n1 <s²>_2 + n2 <s²>_1) / (<s>_1 <s>_2 N²)
    g^W_norm = g^W_raw / g^W_conf

  This is the static (snapshot) weighted version. See
  exp_johnson_nestedness_weighted_temporal for the year-by-year variant.

columns:
  - name: clustering_method
    type: string
  - name: community
    type: string
  - name: node
    type: string
  - name: set
    type: integer
  - name: degree
    type: integer
  - name: strength
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

import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

_ASSETS_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))

from _lib.config import duckdb_path
from lib.nestedness.johnson import JohnsonNestednessCalculator
from lib.utils.bipartite import filter_zero_rows_cols

DB_PATH = str(duckdb_path())


def materialize() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    nodes_df = con.execute(
        "SELECT node, community_id, set, clustering_method FROM graph.network"
    ).fetchdf()
    edges_df = con.execute(
        'SELECT clustering_method, community, "Source", "Target" FROM graph.edges '
        "WHERE community >= 0"
    ).fetchdf()
    con.close()

    all_rows = []
    for method in sorted(nodes_df["clustering_method"].unique()):
        method_nodes = nodes_df[nodes_df["clustering_method"] == method]
        method_edges = edges_df[edges_df["clustering_method"] == method]

        for comm_id in sorted(method_nodes["community_id"].unique()):
            comm_nodes = method_nodes[method_nodes["community_id"] == comm_id]
            left_nodes = comm_nodes[comm_nodes["set"] == 0]["node"].tolist()
            right_nodes = comm_nodes[comm_nodes["set"] == 1]["node"].tolist()

            if not left_nodes or not right_nodes:
                continue

            comm_edges = method_edges[method_edges["community"] == comm_id]
            if comm_edges.empty:
                continue

            left_index = {n: i for i, n in enumerate(left_nodes)}
            right_index = {n: j for j, n in enumerate(right_nodes)}

            pair_counts = comm_edges.groupby(["Source", "Target"]).size().reset_index(name="weight")
            W = np.zeros((len(left_nodes), len(right_nodes)), dtype=np.int32)
            for _, row in pair_counts.iterrows():
                s, t, w = row["Source"], row["Target"], int(row["weight"])
                if s in left_index and t in right_index:
                    W[left_index[s], right_index[t]] = w
                elif t in left_index and s in right_index:
                    W[left_index[t], right_index[s]] = w

            W_f, row_mask, col_mask = filter_zero_rows_cols(W)
            if W_f.shape[0] == 0 or W_f.shape[1] == 0:
                continue

            calc = JohnsonNestednessCalculator(W_f)
            result = calc.nestedness(return_local=True)

            left_filtered = [n for n, m in zip(left_nodes, row_mask) if m]
            right_filtered = [n for n, m in zip(right_nodes, col_mask) if m]
            node_labels = left_filtered + right_filtered
            node_sets = [0] * len(left_filtered) + [1] * len(right_filtered)
            strengths = np.concatenate([W_f.sum(axis=1), W_f.sum(axis=0)])

            degree_map = {}
            for _, r in comm_nodes.iterrows():
                degree_map[r["node"]] = 0
            for _, r in comm_edges.iterrows():
                for col in ("Source", "Target"):
                    degree_map[r[col]] = degree_map.get(r[col], 0) + 1

            label = f"Community {comm_id}"
            for i, node in enumerate(node_labels):
                all_rows.append({
                    "clustering_method": method,
                    "community": label,
                    "node": node,
                    "set": node_sets[i],
                    "degree": int(degree_map.get(node, 0)),
                    "strength": int(strengths[i]),
                    "local_g_norm": float(result["local_norm"][i]),
                    "local_g_raw": float(result["local_raw"][i]),
                    "g_norm": float(result["g_norm"]),
                    "g_raw": float(result["g_raw"]),
                    "g_conf": float(result["g_conf"]),
                })
            print(f"  {method} / {label}: g_norm={result['g_norm']:.4f}")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    print(f"\nWeighted Johnson nestedness: {len(df)} rows")
    return df
