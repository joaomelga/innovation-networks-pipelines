"""@bruin
name: experiment.spectral_radius_temporal
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Staniczenko et al. (2013) spectral radius as a nestedness metric, computed
  temporally per clustering method and community.

  Two snapshots per (community, year):
    - "cumulative": all edges with year <= Y
    - "rolling_5y": edges in [Y-4, Y]

  Reports weighted spectral radius (ρ(W)), distance-based radii (cosine
  projection per side), participation fractions, and top-K singular vector
  coordinates for each snapshot.

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
  - name: total_weight
    type: integer
  - name: rho_weighted
    type: float
  - name: rho_weighted_norm
    type: float
  - name: rho_distance
    type: float
  - name: rho_distance_norm
    type: float
  - name: rho_distance_late
    type: float
  - name: rho_distance_late_norm
    type: float
  - name: pr_frac_early_weighted
    type: float
  - name: pr_frac_late_weighted
    type: float
  - name: pr_frac_early_distance
    type: float
  - name: pr_frac_late_distance
    type: float
  - name: top_early_nodes_weighted
    type: string[]
  - name: top_early_values_weighted
    type: float[]
  - name: top_late_nodes_weighted
    type: string[]
  - name: top_late_values_weighted
    type: float[]
  - name: top_early_nodes_distance
    type: string[]
  - name: top_early_values_distance
    type: float[]
  - name: top_late_nodes_distance
    type: string[]
  - name: top_late_values_distance
    type: float[]

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
from lib.nestedness.spectral import compute_rho_pr_uv, topk_coords, rho_null, cosine_distance

DB_PATH = str(duckdb_path())
ROLLING_WINDOW = 5
MAX_WORKERS = min(8, (os.cpu_count() or 2))
TOP_K = int(os.environ.get("SPECTRAL_TOPK", 20))


def _build_matrix(rows: np.ndarray, cols: np.ndarray, shape: tuple) -> sp.csr_matrix:
    if rows.size == 0:
        return sp.csr_matrix(shape, dtype=np.int32)
    data = np.ones(rows.size, dtype=np.int32)
    return sp.coo_matrix((data, (rows, cols)), shape=shape).tocsr()


def _trim_and_compute(
    rows: np.ndarray, cols: np.ndarray, shape: tuple,
    left_nodes: list | None = None, right_nodes: list | None = None,
) -> dict:
    W = _build_matrix(rows, cols, shape)
    row_mask = np.asarray(W.sum(axis=1)).ravel() > 0
    col_mask = np.asarray(W.sum(axis=0)).ravel() > 0
    W = W[row_mask][:, col_mask]
    n_left, n_right = W.shape
    n_edges = int((W > 0).sum())
    total_weight = int(W.sum())
    base = {"n_left": n_left, "n_right": n_right, "n_edges": n_edges, "total_weight": total_weight}
    empty_topk = {
        "top_early_nodes_weighted": [], "top_early_values_weighted": [],
        "top_late_nodes_weighted": [], "top_late_values_weighted": [],
        "top_early_nodes_distance": [], "top_early_values_distance": [],
        "top_late_nodes_distance": [], "top_late_values_distance": [],
    }
    if n_left < 1 or n_right < 1 or n_edges == 0:
        return {
            **base,
            "rho_weighted": np.nan, "rho_weighted_norm": np.nan,
            "rho_distance": np.nan, "rho_distance_norm": np.nan,
            "rho_distance_late": np.nan, "rho_distance_late_norm": np.nan,
            "pr_frac_early_weighted": np.nan, "pr_frac_late_weighted": np.nan,
            "pr_frac_early_distance": np.nan, "pr_frac_late_distance": np.nan,
            **empty_topk,
        }

    sW_rows = np.asarray(W.sum(axis=1)).ravel().astype(np.float64)
    sW_cols = np.asarray(W.sum(axis=0)).ravel().astype(np.float64)

    D_early = cosine_distance(W, on_left=True)
    D_late = cosine_distance(W, on_left=False)

    rho_w, pr_early_w, pr_late_w, u_w, v_w = compute_rho_pr_uv(W)
    rho_d_early, pr_early_d, _, u_d_early, _ = compute_rho_pr_uv(D_early)
    rho_d_late, pr_late_d, _, u_d_late, _ = compute_rho_pr_uv(D_late)
    rho_w_null = rho_null(sW_rows, sW_cols)
    rho_d_norm = rho_d_early / (n_left - 1) if n_left > 1 and not np.isnan(rho_d_early) else np.nan
    rho_d_late_norm = (
        rho_d_late / (n_right - 1) if n_right > 1 and not np.isnan(rho_d_late) else np.nan
    )

    active_left = np.flatnonzero(row_mask)
    active_right = np.flatnonzero(col_mask)

    def _topk_pack(v, axis_active, nodes):
        idx_trimmed, vals = topk_coords(v, TOP_K)
        if not idx_trimmed:
            return [], []
        idx_orig = [int(axis_active[i]) for i in idx_trimmed]
        if nodes is not None:
            return [nodes[i] for i in idx_orig], vals
        return idx_orig, vals

    early_w_nodes, early_w_vals = _topk_pack(u_w, active_left, left_nodes)
    late_w_nodes, late_w_vals = _topk_pack(v_w, active_right, right_nodes)
    early_d_nodes, early_d_vals = _topk_pack(u_d_early, active_left, left_nodes)
    late_d_nodes, late_d_vals = _topk_pack(u_d_late, active_right, right_nodes)

    return {
        **base,
        "rho_weighted": rho_w,
        "rho_weighted_norm": rho_w / rho_w_null if rho_w_null > 0 else np.nan,
        "rho_distance": rho_d_early,
        "rho_distance_norm": rho_d_norm,
        "rho_distance_late": rho_d_late,
        "rho_distance_late_norm": rho_d_late_norm,
        "pr_frac_early_weighted": pr_early_w,
        "pr_frac_late_weighted": pr_late_w,
        "pr_frac_early_distance": pr_early_d,
        "pr_frac_late_distance": pr_late_d,
        "top_early_nodes_weighted": early_w_nodes,
        "top_early_values_weighted": early_w_vals,
        "top_late_nodes_weighted": late_w_nodes,
        "top_late_values_weighted": late_w_vals,
        "top_early_nodes_distance": early_d_nodes,
        "top_early_values_distance": early_d_vals,
        "top_late_nodes_distance": late_d_nodes,
        "top_late_values_distance": late_d_vals,
    }


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
        row = _trim_and_compute(sub_src, sub_tgt, shape, left_nodes, right_nodes)
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
    print(f"\nSpectral radius temporal: {len(df)} rows")
    return df
