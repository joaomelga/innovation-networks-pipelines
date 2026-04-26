import numpy as np


class JohnsonNestednessCalculator:
    """
    Johnson et al. (2013) nestedness for binary adjacency / biadjacency matrices.
    g_norm = g_raw / g_conf where g_conf is the analytical configuration-model expectation.
    """

    def __init__(self, mat, bipartite=None):
        mat = np.asarray(mat)
        assert mat.ndim == 2
        assert np.all(np.logical_or(mat == 0, mat == 1)), "Matrix must be binary"
        assert not np.any(mat.sum(axis=1) == 0), "Matrix has rows with only zeros"
        assert not np.any(mat.sum(axis=0) == 0), "Matrix has columns with only zeros"

        self.original_mat = mat
        # bipartite=None auto-detects by shape; pass bipartite=True for square biadjacency matrices
        self.is_bipartite = mat.shape[0] != mat.shape[1] if bipartite is None else bipartite
        self.A = self._build_full_adjacency(mat)
        self.N = self.A.shape[0]
        self.k = self.A.sum(axis=1).astype(float)

    def _build_full_adjacency(self, mat):
        mat = np.asarray(mat, dtype=int)
        n_rows, n_cols = mat.shape
        if self.is_bipartite:
            zero_rows = np.zeros((n_rows, n_rows), dtype=int)
            zero_cols = np.zeros((n_cols, n_cols), dtype=int)
            A = np.block([[zero_rows, mat], [mat.T, zero_cols]])
        else:
            A = np.where(mat + mat.T > 0, 1, 0).astype(int)
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
