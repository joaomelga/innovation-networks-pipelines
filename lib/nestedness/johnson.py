import numpy as np
import scipy.sparse as sp


class JohnsonNestednessCalculator:
    """
    Johnson et al. (2013) nestedness for non-negative biadjacency matrices.

    Accepts binary or weighted (integer/float) matrices. In the weighted case
    k_i is interpreted as strength s_i = Σ W[i,α], and the closed-form g_conf
    from the bipartite configuration model still applies (k → s).

    Exploits the bipartite block structure of A = [[0, M], [M.T, 0]]:
      A @ A = [[M @ M.T, 0], [0, M.T @ M]]
    so only the two diagonal blocks are computed. All heavy math runs on
    scipy.sparse matrices.

    g_conf uses the bipartite-specific null from Johnson et al. (2013),
    Appendix S1, Eq. 1:
      g_conf = (n1·<k²>₂ + n2·<k²>₁) / (<k>₁·<k>₂·N²)
    This reduces to <k²>/(<k>²·N) only when n1 = n2 (symmetric bipartite).
    """

    def __init__(self, mat):
        mat = np.asarray(mat)
        assert mat.ndim == 2, "mat must be 2-D"
        assert np.all(mat >= 0), "Matrix must be non-negative"
        assert not np.any(mat.sum(axis=1) == 0), "Matrix has rows with only zeros"
        assert not np.any(mat.sum(axis=0) == 0), "Matrix has columns with only zeros"

        n_rows, n_cols = mat.shape
        self.M = sp.csr_matrix(mat.astype(float))
        self.n_left = n_rows
        self.n_right = n_cols
        self.N = n_rows + n_cols

    def nestedness(self, return_local=False):
        M = self.M
        n1, n2, N = self.n_left, self.n_right, self.N

        k_left = np.asarray(M.sum(axis=1)).ravel().astype(float)
        k_right = np.asarray(M.sum(axis=0)).ravel().astype(float)

        inv_kl = sp.diags(1.0 / k_left)
        inv_kr = sp.diags(1.0 / k_right)

        # G_top[i,j] = |N(i) ∩ N(j)| / (k_i · k_j), i,j ∈ left set
        G_top = inv_kl @ (M @ M.T) @ inv_kl
        G_top = G_top - sp.diags(G_top.diagonal())

        # G_bot[i,j] = |N(i) ∩ N(j)| / (k_i · k_j), i,j ∈ right set
        G_bot = inv_kr @ (M.T @ M) @ inv_kr
        G_bot = G_bot - sp.diags(G_bot.diagonal())

        row_sum_top = np.asarray(G_top.sum(axis=1)).ravel()
        row_sum_bot = np.asarray(G_bot.sum(axis=1)).ravel()
        sum_G = row_sum_top.sum() + row_sum_bot.sum()

        num_pairs = N * (N - 1)
        g_raw = sum_G / num_pairs if num_pairs > 0 else np.nan

        # Bipartite configuration-model null (Johnson et al. 2013, Appendix S1, Eq. 1)
        k_mean_l = k_left.mean()
        k_mean_r = k_right.mean()
        k_sq_mean_l = float(np.mean(k_left ** 2))
        k_sq_mean_r = float(np.mean(k_right ** 2))

        if k_mean_l == 0 or k_mean_r == 0 or N == 0:
            g_conf = np.nan
            g_norm = np.nan
        else:
            g_conf = (n1 * k_sq_mean_r + n2 * k_sq_mean_l) / (k_mean_l * k_mean_r * N * N)
            g_norm = g_raw / g_conf if g_conf > 0 else np.nan

        local_raw = None
        local_norm = None
        if return_local:
            if n1 > 0 and n2 > 0:
                # Per-side normalization: divide by same-side count, not total N
                local_raw = np.concatenate([row_sum_top / n1, row_sum_bot / n2])
                denom = k_mean_l * k_mean_r
                if n1 > 1 and n2 > 1 and denom > 0:
                    # Per-side null expectations; baseline is 1.0 under the bipartite null
                    g_conf_left = (n1 - 1) * k_sq_mean_r / (n1 * n1 * denom)
                    g_conf_right = (n2 - 1) * k_sq_mean_l / (n2 * n2 * denom)
                    side_g_conf = np.concatenate([
                        np.full(n1, g_conf_left, dtype=float),
                        np.full(n2, g_conf_right, dtype=float),
                    ])
                    local_norm = np.where(side_g_conf > 0, local_raw / side_g_conf, np.nan)
                else:
                    local_norm = np.full_like(local_raw, np.nan)
            else:
                local_raw = np.full(N, np.nan, dtype=float)
                local_norm = np.full(N, np.nan, dtype=float)

        return {
            "g_raw": g_raw,
            "g_conf": g_conf,
            "g_norm": g_norm,
            "local_raw": local_raw,
            "local_norm": local_norm,
        }
