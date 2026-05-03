"""@bruin
name: experiment.eci_temporal
image: python:3.13
connection: duckdb-default

depends:
  - graph.network
  - graph.edges

materialization:
  type: table
  strategy: create+replace

description: |
  Hidalgo & Hausmann (2009) Economic Complexity Index (ECI) adapted to bipartite
  VC syndication networks, computed temporally per clustering method and community.

  Edge weight = number of shared portfolio companies per (early, late) pair.

  Pipeline per (community, year, window_type):
    1. Build weighted biadjacency W.
    2. Compute Balassa RCA; threshold to binary M_RCA.
    3. Restrict to the largest connected component (LCC).
    4. ECI via SVD of D_e^{-1/2} M_RCA D_l^{-1/2}; second singular vector.
    5. Sign-flip so high ECI = selective with selective.
    6. Robust z-normalise (median/MAD) within LCC.

  Use "whole_network" method for whole-network temporal ECI.

columns:
  - name: clustering_method
    type: string
  - name: community
    type: string
  - name: year
    type: integer
  - name: window_type
    type: string
  - name: node
    type: string
  - name: set
    type: integer
  - name: eci_score
    type: float
  - name: eci_rank
    type: integer
  - name: eci_pctile
    type: float
  - name: rca_degree
    type: integer
  - name: strength
    type: integer
  - name: degree
    type: integer
  - name: n_left
    type: integer
  - name: n_right
    type: integer
  - name: lcc_n_left
    type: integer
  - name: lcc_n_right
    type: integer
  - name: n_edges
    type: integer
  - name: n_edges_rca
    type: integer
  - name: total_weight
    type: integer
  - name: spectral_gap
    type: float
  - name: tier_coupling
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
from lib.complexity.eci import rca_binary, compute_eci, ranks_and_pctiles

DB_PATH = str(duckdb_path())
ROLLING_WINDOW = 5
MAX_WORKERS = min(8, (os.cpu_count() or 2))
RCA_THRESHOLD = float(os.environ.get("ECI_RCA_THRESHOLD", 1.0))


def _build_weight_matrix(rows: np.ndarray, cols: np.ndarray, shape: tuple) -> sp.csr_matrix:
    if rows.size == 0:
        return sp.csr_matrix(shape, dtype=np.int64)
    data = np.ones(rows.size, dtype=np.int64)
    return sp.coo_matrix((data, (rows, cols)), shape=shape).tocsr()


def _snapshot_rows(
    rows: np.ndarray, cols: np.ndarray, shape: tuple,
    left_nodes: list, right_nodes: list,
    method: str, comm_label: str, year: int, window_type: str,
) -> list[dict]:
    W = _build_weight_matrix(rows, cols, shape)
    n_left, n_right = shape
    n_edges = int((W > 0).sum())
    total_weight = int(W.sum())
    meta = {
        "clustering_method": method,
        "community": comm_label,
        "year": year,
        "window_type": window_type,
        "n_left": n_left,
        "n_right": n_right,
        "n_edges": n_edges,
        "total_weight": total_weight,
    }

    M_rca = rca_binary(W, RCA_THRESHOLD)
    n_edges_rca = int(M_rca.nnz)
    eci_result = compute_eci(M_rca)
    eci_rows_arr = eci_result["eci_rows"]
    eci_cols_arr = eci_result["eci_cols"]

    diversity = np.asarray(M_rca.sum(axis=1)).ravel().astype(np.int64) if M_rca.nnz else np.zeros(n_left, np.int64)
    ubiquity = np.asarray(M_rca.sum(axis=0)).ravel().astype(np.int64) if M_rca.nnz else np.zeros(n_right, np.int64)
    s_rows = np.asarray(W.sum(axis=1)).ravel().astype(np.int64) if W.nnz else np.zeros(n_left, np.int64)
    s_cols = np.asarray(W.sum(axis=0)).ravel().astype(np.int64) if W.nnz else np.zeros(n_right, np.int64)
    M_bin = (W > 0).astype(np.int8)
    deg_rows = np.asarray(M_bin.sum(axis=1)).ravel().astype(np.int64) if W.nnz else np.zeros(n_left, np.int64)
    deg_cols = np.asarray(M_bin.sum(axis=0)).ravel().astype(np.int64) if W.nnz else np.zeros(n_right, np.int64)

    rk_rows, pct_rows = ranks_and_pctiles(eci_rows_arr)
    rk_cols, pct_cols = ranks_and_pctiles(eci_cols_arr)

    out = []
    for i, node in enumerate(left_nodes):
        out.append({
            **meta,
            "node": node, "set": 0,
            "eci_score": float(eci_rows_arr[i]) if np.isfinite(eci_rows_arr[i]) else np.nan,
            "eci_rank": int(rk_rows[i]) if rk_rows[i] > 0 else None,
            "eci_pctile": float(pct_rows[i]) if np.isfinite(pct_rows[i]) else np.nan,
            "rca_degree": int(diversity[i]),
            "strength": int(s_rows[i]),
            "degree": int(deg_rows[i]),
            "n_edges_rca": n_edges_rca,
            "spectral_gap": eci_result["spectral_gap"],
            "tier_coupling": eci_result["tier_coupling"],
            "lcc_n_left": eci_result["lcc_n_left"],
            "lcc_n_right": eci_result["lcc_n_right"],
        })
    for j, node in enumerate(right_nodes):
        out.append({
            **meta,
            "node": node, "set": 1,
            "eci_score": float(eci_cols_arr[j]) if np.isfinite(eci_cols_arr[j]) else np.nan,
            "eci_rank": int(rk_cols[j]) if rk_cols[j] > 0 else None,
            "eci_pctile": float(pct_cols[j]) if np.isfinite(pct_cols[j]) else np.nan,
            "rca_degree": int(ubiquity[j]),
            "strength": int(s_cols[j]),
            "degree": int(deg_cols[j]),
            "n_edges_rca": n_edges_rca,
            "spectral_gap": eci_result["spectral_gap"],
            "tier_coupling": eci_result["tier_coupling"],
            "lcc_n_left": eci_result["lcc_n_left"],
            "lcc_n_right": eci_result["lcc_n_right"],
        })
    return out


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

    def _job(idx: int, y: int, window_type: str) -> list[dict]:
        if window_type == "cumulative":
            sub_src, sub_tgt = src[:cum_end[idx]], tgt[:cum_end[idx]]
        else:
            start = np.searchsorted(yrs, y - ROLLING_WINDOW, side="right")
            sub_src, sub_tgt = src[start:cum_end[idx]], tgt[start:cum_end[idx]]
        return _snapshot_rows(sub_src, sub_tgt, shape, left_nodes, right_nodes,
                              method, comm_label, int(y), window_type)

    tasks = [(idx, int(y), wt)
             for idx, y in enumerate(years)
             for wt in ("cumulative", f"rolling_{ROLLING_WINDOW}y")]

    all_rows: list[dict] = []
    if MAX_WORKERS > 1 and len(tasks) > 4:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for chunk in ex.map(lambda t: _job(*t), tasks):
                all_rows.extend(chunk)
    else:
        for t in tasks:
            all_rows.extend(_job(*t))
    return all_rows


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
            print(f"  {method} / {label}: {len(left_nodes)}L + {len(right_nodes)}R")
            rows = _compute_community_temporal(method, label, left_nodes, right_nodes, comm_edges)
            all_rows.extend(rows)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).sort_values(
        ["clustering_method", "community", "window_type", "year", "set", "eci_rank"],
        na_position="last",
    ).reset_index(drop=True)
    print(f"\nECI temporal: {len(df)} rows")
    return df
