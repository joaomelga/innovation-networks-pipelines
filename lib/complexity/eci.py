"""Economic Complexity Index (ECI) for bipartite networks.

Reference:
  Hidalgo & Hausmann (2009) The building blocks of economic complexity.
  PNAS 106(26), 10570-10575.

Adapted to bipartite VC syndication: early-stage ↔ late-stage investors.
Weight W(e, l) = number of shared portfolio companies per investor pair.
"""

import numpy as np
import scipy.sparse as sp
from scipy.sparse.csgraph import connected_components
from scipy.sparse.linalg import svds
from scipy.stats import rankdata


def rca_binary(W: sp.csr_matrix, threshold: float = 1.0) -> sp.csr_matrix:
    """Balassa Revealed Comparative Advantage, binarised at a threshold.

    RCA(e, l) = W(e, l) * S / (s_e * s_l).
    Returns a binary {0,1} matrix with 1 where RCA ≥ threshold."""
    if W.nnz == 0:
        return W.astype(np.int8)
    s_rows = np.asarray(W.sum(axis=1)).ravel().astype(np.float64)
    s_cols = np.asarray(W.sum(axis=0)).ravel().astype(np.float64)
    S = float(s_rows.sum())
    if S <= 0:
        return sp.csr_matrix(W.shape, dtype=np.int8)
    coo = W.tocoo()
    denom = s_rows[coo.row] * s_cols[coo.col]
    valid = denom > 0
    rca = np.zeros_like(coo.data, dtype=np.float64)
    rca[valid] = coo.data[valid] * S / denom[valid]
    keep = rca >= threshold
    if not keep.any():
        return sp.csr_matrix(W.shape, dtype=np.int8)
    return sp.coo_matrix(
        (np.ones(int(keep.sum()), dtype=np.int8), (coo.row[keep], coo.col[keep])),
        shape=W.shape,
    ).tocsr()


def largest_connected_component(M: sp.csr_matrix) -> tuple[np.ndarray, np.ndarray]:
    """Row and column boolean masks selecting the LCC of the bipartite graph M.

    Returns (row_mask, col_mask). Nodes outside the LCC are False."""
    n_rows, n_cols = M.shape
    if M.nnz == 0:
        return np.zeros(n_rows, dtype=bool), np.zeros(n_cols, dtype=bool)
    full = sp.bmat([[None, M], [M.T, None]], format="csr", dtype=np.int8)
    n_components, labels = connected_components(full, directed=False)
    if n_components == 1:
        row_mask = np.asarray(M.sum(axis=1)).ravel() > 0
        col_mask = np.asarray(M.sum(axis=0)).ravel() > 0
        return row_mask, col_mask
    sizes = np.bincount(labels)
    lcc = int(np.argmax(sizes))
    return labels[:n_rows] == lcc, labels[n_rows:] == lcc


def _z_robust(x: np.ndarray) -> np.ndarray:
    """Robust z-score using median + MAD (scale ≈ std under normal)."""
    if x.size == 0:
        return x
    med = np.median(x)
    mad = np.median(np.abs(x - med))
    if mad <= 0:
        std = np.std(x)
        if std <= 0:
            return np.zeros_like(x)
        return (x - np.mean(x)) / std
    return (x - med) / (1.4826 * mad)


def compute_eci(M_rca: sp.csr_matrix) -> dict:
    """ECI via SVD of the normalised biadjacency restricted to the LCC.

    Returns a dict with keys:
      eci_rows, eci_cols  — z-normalised ECI scores (NaN outside LCC)
      spectral_gap        — σ₁ − σ₂ of M_norm
      tier_coupling       — σ₂² ∈ [0, 1]
      lcc_n_left, lcc_n_right — LCC sizes

    Sign convention: flipped if Pearson(ECI_rows, log_diversity) < 0, so a
    high score consistently means "many above-expectation partners who are
    themselves selective"."""
    n_rows, n_cols = M_rca.shape
    nan_result = {
        "eci_rows": np.full(n_rows, np.nan),
        "eci_cols": np.full(n_cols, np.nan),
        "spectral_gap": float("nan"),
        "tier_coupling": float("nan"),
        "lcc_n_left": 0,
        "lcc_n_right": 0,
    }
    if M_rca.nnz == 0 or n_rows < 3 or n_cols < 3:
        return nan_result

    row_mask, col_mask = largest_connected_component(M_rca)
    if row_mask.sum() < 3 or col_mask.sum() < 3:
        return {**nan_result, "lcc_n_left": int(row_mask.sum()), "lcc_n_right": int(col_mask.sum())}

    M_lcc = M_rca[row_mask][:, col_mask].astype(np.float64)
    d_r = np.asarray(M_lcc.sum(axis=1)).ravel()
    d_c = np.asarray(M_lcc.sum(axis=0)).ravel()

    if (d_r <= 0).any() or (d_c <= 0).any():
        keep_r = d_r > 0
        keep_c = d_c > 0
        if keep_r.sum() < 3 or keep_c.sum() < 3:
            return {**nan_result, "lcc_n_left": int(row_mask.sum()), "lcc_n_right": int(col_mask.sum())}
        M_lcc = M_lcc[keep_r][:, keep_c]
        d_r = d_r[keep_r]
        d_c = d_c[keep_c]
        idx_r = np.flatnonzero(row_mask)
        idx_c = np.flatnonzero(col_mask)
        row_mask = np.zeros(n_rows, dtype=bool)
        col_mask = np.zeros(n_cols, dtype=bool)
        row_mask[idx_r[keep_r]] = True
        col_mask[idx_c[keep_c]] = True

    inv_sqrt_r = 1.0 / np.sqrt(d_r)
    inv_sqrt_c = 1.0 / np.sqrt(d_c)
    M_norm = sp.diags(inv_sqrt_r) @ M_lcc @ sp.diags(inv_sqrt_c)

    k_min = min(M_norm.shape)
    if k_min <= 2:
        U, s, Vt = np.linalg.svd(M_norm.toarray(), full_matrices=False)
    else:
        U, s, Vt = svds(M_norm, k=2, which="LM", return_singular_vectors=True)
        order = np.argsort(-s)
        s, U, Vt = s[order], U[:, order], Vt[order]

    if s.size < 2:
        return {**nan_result, "lcc_n_left": int(row_mask.sum()), "lcc_n_right": int(col_mask.sum())}

    spectral_gap = float(s[0]) - float(s[1])
    tier_coupling = float(s[1]) ** 2

    u_eci = np.asarray(U[:, 1]).ravel() * inv_sqrt_r
    v_eci = np.asarray(Vt[1]).ravel() * inv_sqrt_c

    log_div = np.log(d_r)
    if u_eci.size > 1 and np.std(u_eci) > 0 and np.std(log_div) > 0:
        if np.corrcoef(u_eci, log_div)[0, 1] < 0:
            u_eci = -u_eci
            v_eci = -v_eci

    eci_rows = np.full(n_rows, np.nan)
    eci_cols = np.full(n_cols, np.nan)
    eci_rows[row_mask] = _z_robust(u_eci)
    eci_cols[col_mask] = _z_robust(v_eci)

    return {
        "eci_rows": eci_rows,
        "eci_cols": eci_cols,
        "spectral_gap": spectral_gap,
        "tier_coupling": tier_coupling,
        "lcc_n_left": int(row_mask.sum()),
        "lcc_n_right": int(col_mask.sum()),
    }


def ranks_and_pctiles(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Dense rank (1 = highest) and percentile (1 = highest) over finite entries."""
    n = values.size
    ranks = np.full(n, -1, dtype=np.int64)
    pctiles = np.full(n, np.nan, dtype=np.float64)
    finite = np.isfinite(values)
    if finite.sum() == 0:
        return ranks, pctiles
    vals = values[finite]
    ranks[finite] = rankdata(-vals, method="min").astype(np.int64)
    n_finite = finite.sum()
    if n_finite > 1:
        pctiles[finite] = (rankdata(vals, method="average") - 1) / (n_finite - 1)
    else:
        pctiles[finite] = 1.0
    return ranks, pctiles
