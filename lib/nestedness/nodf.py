"""NODF and wNODF nestedness metrics.

References:
  Almeida-Neto et al. (2008) A consistent metric for nestedness analysis.
  Almeida-Neto & Ulrich (2011) A straightforward computational approach for
    measuring nestedness using quantitative matrices.
"""

import numpy as np
import scipy.sparse as sp


def _axis_sum(overlap: sp.spmatrix, k: np.ndarray) -> float:
    """NODF paired-contribution sum on one axis (strict upper triangle)."""
    triu = sp.triu(overlap, k=1).tocoo()
    if triu.nnz == 0:
        return 0.0
    o = triu.data.astype(np.float64, copy=False)
    ki = k[triu.row]
    kj = k[triu.col]
    kmin = np.minimum(ki, kj)
    mask = (ki != kj) & (kmin > 0)
    if not mask.any():
        return 0.0
    return float(np.sum(o[mask] / kmin[mask]))


def compute_nodf(biadj: sp.csr_matrix) -> dict:
    """Binary NODF (Almeida-Neto et al. 2008) on a binary biadjacency.

    Returns dict with keys: nodf, nodf_rows, nodf_cols, fill.
    Values are in [0, 100]; NaN when the matrix is too small."""
    n_rows, n_cols = biadj.shape
    if n_rows < 2 or n_cols < 2:
        return {"nodf": np.nan, "nodf_rows": np.nan, "nodf_cols": np.nan, "fill": np.nan}

    k_rows = np.asarray(biadj.sum(axis=1)).ravel().astype(np.float64)
    k_cols = np.asarray(biadj.sum(axis=0)).ravel().astype(np.float64)

    M32 = biadj.astype(np.int32)
    sum_rows = _axis_sum(M32 @ M32.T, k_rows)
    sum_cols = _axis_sum(M32.T @ M32, k_cols)

    n_pairs_rows = n_rows * (n_rows - 1) / 2
    n_pairs_cols = n_cols * (n_cols - 1) / 2

    nodf_rows = sum_rows / n_pairs_rows * 100 if n_pairs_rows > 0 else np.nan
    nodf_cols = sum_cols / n_pairs_cols * 100 if n_pairs_cols > 0 else np.nan
    nodf = (sum_rows + sum_cols) / (n_pairs_rows + n_pairs_cols) * 100
    fill = float(biadj.sum()) / (n_rows * n_cols)

    return {
        "nodf": float(nodf),
        "nodf_rows": float(nodf_rows),
        "nodf_cols": float(nodf_cols),
        "fill": float(fill),
    }


def _wnodf_axis(W_sorted: sp.csr_matrix, sorted_sum: np.ndarray) -> tuple[float, int]:
    """Row-axis wNODF accumulator; call on W.T.tocsr() for the column axis.

    W_sorted must be sorted by row-sum descending.
    Returns (sum_of_pair_contributions, n_ordered_pairs)."""
    n_rows = W_sorted.shape[0]
    total = 0.0
    n_pairs = 0
    for j in range(1, n_rows):
        row_j = W_sorted[j].toarray().ravel()
        col_idx = np.flatnonzero(row_j > 0)
        m_j = int(col_idx.size)
        n_pairs += j
        if m_j == 0:
            continue
        prev = W_sorted[:j][:, col_idx].toarray()
        row_j_vals = row_j[col_idx]
        counts = (prev > row_j_vals[None, :]).sum(axis=1).astype(np.float64)
        equal_mask = sorted_sum[:j] == sorted_sum[j]
        contribs = np.where(equal_mask, 0.0, 100.0 * counts / m_j)
        total += float(contribs.sum())
    return total, n_pairs


def compute_wnodf(biadj: sp.csr_matrix) -> dict:
    """Weighted NODF (Almeida-Neto & Ulrich 2011) on a non-negative weighted biadjacency.

    Returns dict with keys: wnodf, wnodf_rows, wnodf_cols, fill."""
    nan_out = {"wnodf": np.nan, "wnodf_rows": np.nan, "wnodf_cols": np.nan, "fill": np.nan}
    n_rows, n_cols = biadj.shape
    if n_rows < 2 or n_cols < 2:
        return nan_out

    rowsum = np.asarray(biadj.sum(axis=1)).ravel().astype(np.float64)
    colsum = np.asarray(biadj.sum(axis=0)).ravel().astype(np.float64)
    if np.all(rowsum == 0) or np.all(colsum == 0):
        return nan_out

    row_order = np.argsort(-rowsum, kind="stable")
    col_order = np.argsort(-colsum, kind="stable")
    W = biadj[row_order][:, col_order].tocsr()
    rowsum_sorted = rowsum[row_order]
    colsum_sorted = colsum[col_order]

    total_rows, n_pair_rows = _wnodf_axis(W, rowsum_sorted)
    total_cols, n_pair_cols = _wnodf_axis(W.T.tocsr(), colsum_sorted)

    wnodf_rows = total_rows / n_pair_rows if n_pair_rows > 0 else np.nan
    wnodf_cols = total_cols / n_pair_cols if n_pair_cols > 0 else np.nan
    total_pairs = n_pair_rows + n_pair_cols
    wnodf = (total_rows + total_cols) / total_pairs if total_pairs > 0 else np.nan
    fill = float(biadj.nnz) / (n_rows * n_cols)

    return {
        "wnodf": float(wnodf),
        "wnodf_rows": float(wnodf_rows),
        "wnodf_cols": float(wnodf_cols),
        "fill": float(fill),
    }
