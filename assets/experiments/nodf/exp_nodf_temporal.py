"""@bruin
name: experiment.nodf_temporal
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Almeida-Neto et al. (2008) NODF computed temporally, per clustering method
  and community.

  For every (clustering_method, community, year) combination the asset computes
  NODF on two snapshots:
    - "cumulative": all edges with year <= Y
    - "rolling_5y": edges in [Y-4, Y]

  Use the "whole_network" method for whole-network temporal analysis.

columns:
  - name: clustering_method
    type: string
  - name: community
    type: string
  - name: year
    type: integer
  - name: window_type
    type: string
  - name: n_left
    type: integer
  - name: n_right
    type: integer
  - name: n_edges
    type: integer
  - name: fill
    type: float
  - name: nodf
    type: float
  - name: nodf_rows
    type: float
  - name: nodf_cols
    type: float

@bruin"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import scipy.sparse as sp

_ASSETS_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))

from _lib.config import duckdb_path
from lib.nestedness.nodf import compute_nodf

DB_PATH = str(duckdb_path())
ROLLING_WINDOW = 5
MAX_WORKERS = min(8, (os.cpu_count() or 2))


def _build_biadj(rows: np.ndarray, cols: np.ndarray, shape: tuple) -> sp.csr_matrix:
    if rows.size == 0:
        return sp.csr_matrix(shape, dtype=np.int8)
    data = np.ones(rows.size, dtype=np.int8)
    M = sp.coo_matrix((data, (rows, cols)), shape=shape).tocsr()
    if M.nnz:
        M.data = np.minimum(M.data, 1)
    return M


def _trim_and_compute(rows: np.ndarray, cols: np.ndarray, shape: tuple) -> dict:
    M = _build_biadj(rows, cols, shape)
    row_mask = np.asarray(M.sum(axis=1)).ravel() > 0
    col_mask = np.asarray(M.sum(axis=0)).ravel() > 0
    M = M[row_mask][:, col_mask]
    n_left, n_right = M.shape
    n_edges = int(M.sum())
    base = {"n_left": n_left, "n_right": n_right, "n_edges": n_edges}
    if n_left < 2 or n_right < 2:
        return {**base, "fill": np.nan, "nodf": np.nan, "nodf_rows": np.nan, "nodf_cols": np.nan}
    return {**base, **compute_nodf(M)}


def _compute_community_temporal(
    method: str, comm_label: str,
    left_nodes: list, right_nodes: list, comm_edges: pd.DataFrame,
) -> list[dict]:
    left_index = {n: i for i, n in enumerate(left_nodes)}
    right_index = {n: j for j, n in enumerate(right_nodes)}
    shape = (len(left_nodes), len(right_nodes))

    src = comm_edges["Source"].map(left_index).to_numpy()
    tgt = comm_edges["Target"].map(right_index).to_numpy()
    yrs = comm_edges["year"].to_numpy()
    valid = ~pd.isna(src) & ~pd.isna(tgt) & ~pd.isna(yrs)
    src = src[valid].astype(np.int64)
    tgt = tgt[valid].astype(np.int64)
    yrs = yrs[valid].astype(np.int64)

    if src.size == 0:
        return []

    order = np.argsort(yrs, kind="stable")
    src, tgt, yrs = src[order], tgt[order], yrs[order]
    years = np.unique(yrs)
    cum_end = np.searchsorted(yrs, years, side="right")

    def _job(idx: int, y: int, window_type: str) -> dict:
        if window_type == "cumulative":
            sub_src, sub_tgt = src[:cum_end[idx]], tgt[:cum_end[idx]]
        else:
            start = np.searchsorted(yrs, y - ROLLING_WINDOW, side="right")
            sub_src, sub_tgt = src[start:cum_end[idx]], tgt[start:cum_end[idx]]
        row = _trim_and_compute(sub_src, sub_tgt, shape)
        return {"clustering_method": method, "community": comm_label,
                "year": int(y), "window_type": window_type, **row}

    tasks = [(idx, int(y), wt)
             for idx, y in enumerate(years)
             for wt in ("cumulative", f"rolling_{ROLLING_WINDOW}y")]

    if MAX_WORKERS > 1 and len(tasks) > 4:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            return list(ex.map(lambda t: _job(*t), tasks))
    return [_job(*t) for t in tasks]


def materialize() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    nodes_df = con.execute(
        "SELECT node, community_id, set, clustering_method FROM graph.network"
    ).fetchdf()
    edges_df = con.execute(
        'SELECT clustering_method, community, "Source", "Target", year FROM graph.edges '
        "WHERE year IS NOT NULL"
    ).fetchdf()
    con.close()

    all_rows = []
    for method in sorted(nodes_df["clustering_method"].unique()):
        method_nodes = nodes_df[nodes_df["clustering_method"] == method]
        method_edges = edges_df[
            (edges_df["clustering_method"] == method) & (edges_df["community"] >= 0)
        ]

        for comm_id in sorted(method_nodes["community_id"].unique()):
            comm_nodes = method_nodes[method_nodes["community_id"] == comm_id]
            left_nodes = comm_nodes[comm_nodes["set"] == 0]["node"].tolist()
            right_nodes = comm_nodes[comm_nodes["set"] == 1]["node"].tolist()

            if not left_nodes or not right_nodes:
                continue

            comm_edges = method_edges[method_edges["community"] == comm_id]
            if comm_edges.empty:
                continue

            label = f"Community {comm_id}"
            rows = _compute_community_temporal(method, label, left_nodes, right_nodes, comm_edges)
            all_rows.extend(rows)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).sort_values(
        ["clustering_method", "community", "window_type", "year"]
    ).reset_index(drop=True)
    print(f"\nNODF temporal: {len(df)} rows")
    return df
