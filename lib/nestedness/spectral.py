"""Spectral-radius nestedness metrics.

Reference:
  Staniczenko et al. (2013) The ghost of nestedness in ecological networks.
  Nature Communications 4, 1391.
"""

import numpy as np
import scipy.sparse as sp
from scipy.sparse.linalg import svds


def _participation_fraction(v: np.ndarray) -> float:
    """Fraction of coordinates meaningfully participating in a unit vector v.

    PR(v) = (Σ v²)² / (Σ v⁴ · n) ∈ (1/n, 1].
    Uniform v → 1; spike at one coordinate → 1/n."""
    if v is None or v.size == 0:
        return float("nan")
    v2 = np.asarray(v, dtype=np.float64) ** 2
    num = v2.sum() ** 2
    denom = (v2 ** 2).sum()
    if denom <= 0:
        return float("nan")
    return float(num / denom / v.size)


def compute_rho_pr_uv(M: sp.csr_matrix) -> tuple:
    """Largest singular value of M plus leading singular vectors and PR fractions.

    Returns (rho, pr_fraction_rows, pr_fraction_cols, u, v) where u and v are
    the leading left/right singular vectors (or None for degenerate inputs)."""
    nan = float("nan")
    n_rows, n_cols = M.shape
    if n_rows == 0 or n_cols == 0 or M.nnz == 0:
        return nan, nan, nan, None, None
    if min(n_rows, n_cols) <= 2:
        U, s, Vt = np.linalg.svd(M.toarray().astype(np.float64), full_matrices=False)
        u = U[:, 0]
        v = Vt[0]
        rho = float(s[0])
    else:
        U, s, Vt = svds(M.astype(np.float64), k=1, which="LM", return_singular_vectors=True)
        u = U[:, -1]
        v = Vt[-1]
        rho = float(s[-1])
    return rho, _participation_fraction(u), _participation_fraction(v), u, v


def topk_coords(v: np.ndarray, k: int) -> tuple[list[int], list[float]]:
    """Top-k indices of |v| (sign-blind) and their absolute magnitudes, descending."""
    if v is None or v.size == 0:
        return [], []
    abs_v = np.abs(v.astype(np.float64))
    k = min(k, abs_v.size)
    if k <= 0:
        return [], []
    if k == abs_v.size:
        order = np.argsort(-abs_v)
    else:
        part = np.argpartition(-abs_v, k - 1)[:k]
        order = part[np.argsort(-abs_v[part])]
    return order.tolist(), abs_v[order].tolist()


def rho_null(row_sums: np.ndarray, col_sums: np.ndarray) -> float:
    """Leading-order configuration-null expectation for the spectral radius.

    ρ_null ≈ sqrt(Σ k²_rows · Σ k²_cols) / S,  S = Σ k_rows = Σ k_cols.
    Valid for binary (k = degree) and weighted (k = strength) inputs."""
    S = float(row_sums.sum())
    if S <= 0:
        return float("nan")
    return float(np.sqrt((row_sums ** 2).sum() * (col_sums ** 2).sum()) / S)


def cosine_distance(X: sp.csr_matrix, on_left: bool = True) -> sp.csr_matrix:
    """Cosine-distance projection of a bipartite matrix to one side.

    on_left=True  → Early × Early: S = X · Xᵀ  (n_left × n_left)
    on_left=False → Late  × Late:  S = Xᵀ · X  (n_right × n_right)

    Returns sparse distance matrix D = 1 − cos(i,j) ∈ [0, 1] with zero diagonal."""
    Xf = X.astype(np.float64)
    S = (Xf @ Xf.T) if on_left else (Xf.T @ Xf)
    S_dense = S.toarray() if sp.issparse(S) else np.asarray(S)
    diag = np.diag(S_dense).copy()
    safe = np.where(diag > 0, diag, 1.0)
    norm = np.sqrt(np.outer(safe, safe))
    cos = np.clip(S_dense / norm, 0.0, 1.0)
    D = 1.0 - cos
    np.fill_diagonal(D, 0.0)
    zero = np.flatnonzero(diag <= 0)
    if zero.size > 0:
        D[zero, :] = 0.0
        D[:, zero] = 0.0
    return sp.csr_matrix(D)
