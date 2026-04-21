import numpy as np
import pandas as pd

from lib.nestedness.johnson import JohnsonNestednessCalculator


def filter_zero_rows_cols(adjacency_matrix):
    """Remove rows and columns that are all zeros from a matrix."""
    row_mask = adjacency_matrix.sum(axis=1) > 0
    col_mask = adjacency_matrix.sum(axis=0) > 0
    return adjacency_matrix[row_mask][:, col_mask], row_mask, col_mask


def compute_community_nestedness(nodes_df, edges_df, comm_id):
    """
    Compute Johnson nestedness for a single community.

    Returns dict with node_labels, node_sets, local_g_norm, local_g_raw,
    g_norm, g_raw, g_conf. Returns None if community is empty or has no edges.
    """
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

    adj_filtered, row_mask, col_mask = filter_zero_rows_cols(adj)
    if adj_filtered.sum() == 0 or adj_filtered.shape[0] == 0 or adj_filtered.shape[1] == 0:
        print(f"  Community {comm_id}: no edges after filtering — skipping")
        return None

    print(f"  Community {comm_id}: matrix {adj_filtered.shape[0]}x{adj_filtered.shape[1]}, "
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
