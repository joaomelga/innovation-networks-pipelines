"""@bruin
name: experiment.johnson_nestedness_temporal
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Johnson et al. (2013) nestedness computed temporally, per clustering method
  and community.

  For every (clustering_method, community, year) combination the asset computes
  global Johnson nestedness (g_raw, g_conf, g_norm, g_norm_left, g_norm_right)
  on two network snapshots:
    - "cumulative": all edges with edge.year <= Y
    - "rolling_5y": edges in [Y-4, Y]

  Use the "whole_network" clustering method to get whole-network temporal
  analysis (single community containing all nodes).

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
  - name: g_raw
    type: float
  - name: g_conf
    type: float
  - name: g_norm
    type: float
  - name: g_norm_left
    type: float
  - name: g_norm_right
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

DB_PATH = str(duckdb_path())
ROLLING_WINDOW = 5
MAX_WORKERS = min(8, (os.cpu_count() or 2))


def compute_johnson(biadj: sp.csr_matrix) -> dict:
    """Johnson et al. (2013) global nestedness on a binary or weighted biadjacency."""
    nan_out = {
        "g_raw": np.nan, "g_conf": np.nan, "g_norm": np.nan,
        "g_norm_left": np.nan, "g_norm_right": np.nan,
    }
    n_rows, n_cols = biadj.shape
    if n_rows < 1 or n_cols < 1:
        return nan_out

    k_left = np.asarray(biadj.sum(axis=1)).ravel().astype(np.float64)
    k_right = np.asarray(biadj.sum(axis=0)).ravel().astype(np.float64)
    if np.any(k_left == 0) or np.any(k_right == 0):
        return nan_out

    inv_kl = sp.diags(1.0 / k_left)
    inv_kr = sp.diags(1.0 / k_right)
    Mf = biadj.astype(np.float64)

    G_top = inv_kl @ (Mf @ Mf.T) @ inv_kl
    G_top = G_top - sp.diags(G_top.diagonal())
    G_bot = inv_kr @ (Mf.T @ Mf) @ inv_kr
    G_bot = G_bot - sp.diags(G_bot.diagonal())

    sum_top = float(G_top.sum())
    sum_bot = float(G_bot.sum())
    N = n_rows + n_cols
    num_pairs = N * (N - 1)
    if num_pairs <= 0:
        return nan_out

    g_raw = (sum_top + sum_bot) / num_pairs
    n1, n2 = n_rows, n_cols
    k_mean_l = k_left.mean()
    k_mean_r = k_right.mean()
    k_sq_mean_l = float(np.mean(k_left ** 2))
    k_sq_mean_r = float(np.mean(k_right ** 2))
    if k_mean_l <= 0 or k_mean_r <= 0:
        return {**nan_out, "g_raw": float(g_raw)}

    g_conf = (n1 * k_sq_mean_r + n2 * k_sq_mean_l) / (k_mean_l * k_mean_r * N * N)
    if g_conf <= 0:
        return {**nan_out, "g_raw": float(g_raw), "g_conf": float(g_conf)}

    g_norm = g_raw / g_conf
    denom = k_mean_l * k_mean_r
    if n1 > 1 and n2 > 1 and denom > 0:
        g_conf_left = (n1 - 1) * k_sq_mean_r / (n1 * n1 * denom)
        g_conf_right = (n2 - 1) * k_sq_mean_l / (n2 * n2 * denom)
        mean_local_raw_left = sum_top / (n_rows * n_rows)
        mean_local_raw_right = sum_bot / (n_cols * n_cols)
        g_norm_left = mean_local_raw_left / g_conf_left if g_conf_left > 0 else np.nan
        g_norm_right = mean_local_raw_right / g_conf_right if g_conf_right > 0 else np.nan
    else:
        g_norm_left = g_norm_right = np.nan

    return {
        "g_raw": float(g_raw),
        "g_conf": float(g_conf),
        "g_norm": float(g_norm),
        "g_norm_left": float(g_norm_left),
        "g_norm_right": float(g_norm_right),
    }


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
    if n_left < 1 or n_right < 1:
        return {**base, "g_raw": np.nan, "g_conf": np.nan, "g_norm": np.nan,
                "g_norm_left": np.nan, "g_norm_right": np.nan}
    return {**base, **compute_johnson(M)}


def _compute_community_temporal(
    method: str,
    comm_label: str,
    left_nodes: list,
    right_nodes: list,
    comm_edges: pd.DataFrame,
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

    results = []
    if MAX_WORKERS > 1 and len(tasks) > 4:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(lambda t: _job(*t), tasks))
    else:
        results = [_job(*t) for t in tasks]

    return results


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
            print(f"  {method} / {label}: {len(left_nodes)} left, {len(right_nodes)} right, "
                  f"{len(comm_edges)} edges")
            rows = _compute_community_temporal(method, label, left_nodes, right_nodes, comm_edges)
            all_rows.extend(rows)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).sort_values(
        ["clustering_method", "community", "window_type", "year"]
    ).reset_index(drop=True)
    print(f"\nJohnson nestedness temporal: {len(df)} rows")
    return df
