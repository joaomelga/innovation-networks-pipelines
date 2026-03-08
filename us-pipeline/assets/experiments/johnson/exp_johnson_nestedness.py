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
  Calculate Johnson et al. (2013) nestedness for the top 3 communities.
  Produces per-node local nestedness metrics (g_norm, g_raw) alongside degree.
  This is the main analytical output used for the nestedness vs degree scatter plots.

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
import numpy as np
import duckdb
import os

DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", "us_pipeline.duckdb")
TARGET_COMMUNITIES = [0, 1, 2]


# ── Johnson Nestedness Calculator (self-contained) ──
class JohnsonNestednessCalculator:
    """
    Johnson et al. (2013) nestedness for binary adjacency / biadjacency matrices.
    g_norm = g_raw / g_conf where g_conf is the analytical configuration-model expectation.
    """

    def __init__(self, mat):
        mat = np.asarray(mat)
        assert mat.ndim == 2
        assert np.all(np.logical_or(mat == 0, mat == 1)), "Matrix must be binary"
        assert not np.any(mat.sum(axis=1) == 0), "Matrix has rows with only zeros"
        assert not np.any(mat.sum(axis=0) == 0), "Matrix has columns with only zeros"

        self.original_mat = mat
        self.is_bipartite = mat.shape[0] != mat.shape[1]
        self.A = self._build_full_adjacency(mat)
        self.N = self.A.shape[0]
        self.k = self.A.sum(axis=1).astype(float)

    def _build_full_adjacency(self, mat):
        mat = np.asarray(mat, dtype=int)
        n_rows, n_cols = mat.shape
        if n_rows == n_cols:
            A = np.where(mat + mat.T > 0, 1, 0).astype(int)
        else:
            zero_rows = np.zeros((n_rows, n_rows), dtype=int)
            zero_cols = np.zeros((n_cols, n_cols), dtype=int)
            A = np.block([[zero_rows, mat], [mat.T, zero_cols]])
        return A

    def nestedness(self, return_local=False):
        A, N, k = self.A, self.N, self.k
        B = A.dot(A).astype(float)

        ki = k.reshape(N, 1)
        kj = k.reshape(1, N)
        denom = ki * kj

        with np.errstate(divide="ignore", invalid="ignore"):
            G = np.zeros_like(B, dtype=float)
            mask = denom > 0
            G[mask] = B[mask] / denom[mask]

        np.fill_diagonal(G, 0.0)

        num_pairs = N * (N - 1)
        g_raw = G.sum() / num_pairs if num_pairs > 0 else np.nan

        k_mean = k.mean()
        k_sq_mean = np.mean(k ** 2)

        if k_mean == 0 or N == 0:
            g_conf = np.nan
            g_norm = np.nan
        else:
            g_conf = (k_sq_mean / (k_mean ** 2)) / float(N)
            g_norm = g_raw / g_conf if g_conf > 0 else np.nan

        local_raw = None
        local_norm = None
        if return_local:
            if N > 1:
                local_raw = G.sum(axis=1) / (N - 1)
            else:
                local_raw = np.full(N, np.nan, dtype=float)
            local_norm = local_raw / g_conf if g_conf > 0 else np.full_like(local_raw, np.nan)

        return {
            "g_raw": g_raw,
            "g_conf": g_conf,
            "g_norm": g_norm,
            "local_raw": local_raw,
            "local_norm": local_norm,
        }


def _filter_zero_rows_cols(adjacency_matrix):
    row_mask = adjacency_matrix.sum(axis=1) > 0
    col_mask = adjacency_matrix.sum(axis=0) > 0
    return adjacency_matrix[row_mask][:, col_mask], row_mask, col_mask


def _compute_community_nestedness(nodes_df, edges_df, comm_id):
    """Compute Johnson nestedness for a single community."""
    community_nodes = nodes_df[nodes_df["community_id"] == comm_id].copy()
    community_node_ids = set(community_nodes["node"].values)

    community_edges = edges_df[
        (edges_df["Source"].isin(community_node_ids))
        & (edges_df["Target"].isin(community_node_ids))
    ]

    left_nodes = community_nodes[community_nodes["set"] == 0]["node"].values
    right_nodes = community_nodes[community_nodes["set"] == 1]["node"].values

    if len(left_nodes) == 0 or len(right_nodes) == 0:
        print(f"  Community {comm_id}: empty bipartite set — skipping")
        return None

    left_indices = {node: i for i, node in enumerate(left_nodes)}
    right_indices = {node: j for j, node in enumerate(right_nodes)}

    adj = np.zeros((len(left_nodes), len(right_nodes)), dtype=np.int8)
    for _, edge in community_edges.iterrows():
        s, t = edge["Source"], edge["Target"]
        if s in left_indices and t in right_indices:
            adj[left_indices[s], right_indices[t]] = 1
        elif t in left_indices and s in right_indices:
            adj[left_indices[t], right_indices[s]] = 1

    adj_filtered, row_mask, col_mask = _filter_zero_rows_cols(adj)
    if adj_filtered.sum() == 0 or adj_filtered.shape[0] == 0 or adj_filtered.shape[1] == 0:
        print(f"  Community {comm_id}: no edges after filtering — skipping")
        return None

    print(f"  Community {comm_id}: matrix {adj_filtered.shape[0]}×{adj_filtered.shape[1]}, "
          f"edges={int(adj_filtered.sum())}, density={adj_filtered.mean():.4f}")

    calc = JohnsonNestednessCalculator(adj_filtered)
    result = calc.nestedness(return_local=True)

    left_filtered = left_nodes[row_mask]
    right_filtered = right_nodes[col_mask]
    node_labels = list(left_filtered) + list(right_filtered)
    node_sets = [0] * len(left_filtered) + [1] * len(right_filtered)

    return {
        "node_labels": node_labels,
        "node_sets": node_sets,
        "local_g_norm": result["local_norm"],
        "local_g_raw": result["local_raw"],
        "g_norm": result["g_norm"],
        "g_raw": result["g_raw"],
        "g_conf": result["g_conf"],
    }


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
        result = _compute_community_nestedness(nodes_df, edges_df, comm_id)
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
