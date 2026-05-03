"""Economic Complexity Index (ECI) ranking — per community."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, region_selector, clustering_method_selector, SET_COLORS, SET_LABELS

st.set_page_config(page_title="ECI Ranking", layout="wide")

selected = region_selector()
if not selected:
    st.stop()
method = clustering_method_selector()

st.title("Economic Complexity Index (ECI) — investor ranking")
st.markdown(
    "Ranks early- and late-stage investors by **network position**, not raw size. "
    "ECI = second eigenvector of the bipartite reflection matrix of the "
    "**RCA-thresholded** co-investment graph (RCA ≥ 1 ⇒ a pair partners more than "
    "expected from each side's total activity). High ECI = many above-expectation "
    "partners, themselves selective. Computed on the largest connected component of "
    "`M_RCA` per snapshot — nodes outside the LCC get NaN ECI."
)

with st.expander("How to read ECI", expanded=False):
    st.markdown(
        """
**Pipeline (Hidalgo & Hausmann 2009):**
1. Weighted biadjacency `W` — count of shared portfolio companies per (early, late) pair.
2. Balassa **RCA**: `RCA[i,j] = (W[i,j]·S) / (sᵢ·sⱼ)` thresholded at ≥ 1.0.
3. Largest connected component of `M_RCA`, then SVD on `D_e^{-1/2} M_RCA D_l^{-1/2}`. The **second** singular vector is ECI.
4. Sign convention: flip if Spearman(ECI, log diversity) < 0. z-normalise with median/MAD.

**Diagnostics:**
- `tier_coupling = σ₂²` ∈ [0, 1] — strength of early–late tier alignment.
- `spectral_gap = 1 − σ₂` — resolution of the tier mode; small gap ⇒ ECI is noisy.
        """
    )


def _community_sort_key(name: str) -> int:
    try:
        return int(name.rsplit(" ", 1)[-1])
    except ValueError:
        return 0


@st.cache_data(ttl=600)
def _load(method: str) -> pd.DataFrame:
    df = query_df(
        f"SELECT * FROM experiment.eci_temporal WHERE clustering_method = '{method}'"
    )
    if not df.empty:
        df["set_label"] = df["set"].map(SET_LABELS)
    return df


df = _load(method)
if df.empty:
    st.info(
        "`experiment.eci_temporal` has no rows for this method. "
        "Run `exp_eci_temporal.py` first."
    )
    st.stop()

# ── Sidebar controls ──────────────────────────────────────────────────────────
communities = sorted(df["community"].unique(), key=_community_sort_key)
community = st.sidebar.selectbox("Community", communities, key="eci_comm")

window_types = sorted(df["window_type"].unique().tolist())
default_window = "cumulative" if "cumulative" in window_types else window_types[0]
window_choice = st.radio(
    "Window type", window_types, index=window_types.index(default_window),
    horizontal=True, key="eci_window",
)

view = df[(df["community"] == community) & (df["window_type"] == window_choice)].copy()
if view.empty:
    st.info("No data for this community/window combination.")
    st.stop()

min_year, max_year = int(view["year"].min()), int(view["year"].max())
year_range = st.slider("Year range", min_year, max_year, (min_year, max_year), key="eci_years")
view = view[view["year"].between(*year_range)].copy()

# Snapshot-level frame (one row per year, dedup network-level cols)
snap_cols = [c for c in ["year", "n_left", "n_right", "lcc_n_left", "lcc_n_right",
                          "n_edges", "n_edges_rca", "spectral_gap", "tier_coupling"]
             if c in view.columns]
snap = view[snap_cols].drop_duplicates(subset=["year"]).sort_values("year").reset_index(drop=True)

# =============================================================================
# Section 1 — Latest-year diagnostics
# =============================================================================
st.subheader(f"Latest-year snapshot — {community} ({window_choice})")
latest_snap = snap.iloc[-1] if not snap.empty else None

if latest_snap is not None and "tier_coupling" in latest_snap.index:
    tc = latest_snap["tier_coupling"]
    sg = latest_snap.get("spectral_gap", float("nan"))
    interp = (
        "Strong tier coupling" if not pd.isna(tc) and tc > 0.85
        else "Moderate" if not pd.isna(tc) and tc > 0.6
        else "Weak" if not pd.isna(tc)
        else "—"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("tier_coupling (σ₂²)", f"{tc:.3f}" if not pd.isna(tc) else "—",
              delta=interp, delta_color="off")
    c2.metric("spectral_gap (1−σ₂)", f"{sg:.3f}" if not pd.isna(sg) else "—")
    if "lcc_n_left" in latest_snap.index:
        c3.metric("LCC size (early × late)",
                  f"{int(latest_snap['lcc_n_left'])} × {int(latest_snap['lcc_n_right'])}")
    if "n_edges_rca" in latest_snap.index:
        c4.metric("RCA-thresholded edges", f"{int(latest_snap['n_edges_rca'])}")

# =============================================================================
# Section 2 — Tier coupling over time
# =============================================================================
if "tier_coupling" in snap.columns:
    st.subheader(f"Tier Coupling Over Time — {window_choice}")
    st.markdown(
        "`tier_coupling = σ₂²` ∈ [0, 1]. Higher = Tier-1 early funds preferentially "
        "syndicate with Tier-1 late funds. `spectral_gap = 1 − σ₂` governs how "
        "interpretable ECI rankings are in a given snapshot."
    )
    diag_fig = make_subplots(
        rows=1, cols=2, subplot_titles=("tier_coupling (σ₂²)", "spectral_gap (1−σ₂)"),
        horizontal_spacing=0.12,
    )
    diag_fig.add_trace(
        go.Scatter(x=snap["year"], y=snap["tier_coupling"], mode="lines+markers",
                   name="tier_coupling", line=dict(color="#8e44ad")),
        row=1, col=1,
    )
    if "spectral_gap" in snap.columns:
        diag_fig.add_trace(
            go.Scatter(x=snap["year"], y=snap["spectral_gap"], mode="lines+markers",
                       name="spectral_gap", line=dict(color="#27ae60")),
            row=1, col=2,
        )
    diag_fig.update_yaxes(range=[0, 1.05], row=1, col=1)
    if "spectral_gap" in snap.columns:
        diag_fig.update_yaxes(range=[0, 1.05], row=1, col=2)
    diag_fig.update_layout(height=380, hovermode="x unified", showlegend=False)
    st.plotly_chart(diag_fig, use_container_width=True, key="eci_diag")

# LCC size and RCA edges over time
size_titles = ["LCC nodes (early × late)", "RCA-thresholded edges"]
size_fig = make_subplots(rows=1, cols=2, subplot_titles=size_titles, horizontal_spacing=0.12)
if "lcc_n_left" in snap.columns:
    size_fig.add_trace(
        go.Scatter(x=snap["year"], y=snap["lcc_n_left"], mode="lines+markers",
                   name="Early (LCC)", line=dict(color="#e74c3c")), row=1, col=1,
    )
    size_fig.add_trace(
        go.Scatter(x=snap["year"], y=snap["lcc_n_right"], mode="lines+markers",
                   name="Late (LCC)", line=dict(color="#3498db")), row=1, col=1,
    )
if "n_edges_rca" in snap.columns:
    size_fig.add_trace(
        go.Scatter(x=snap["year"], y=snap["n_edges_rca"], mode="lines+markers",
                   name="RCA edges", showlegend=False, line=dict(color="#e67e22")),
        row=1, col=2,
    )
size_fig.update_layout(height=360, hovermode="x unified")
st.plotly_chart(size_fig, use_container_width=True, key="eci_size")

# =============================================================================
# Section 3 — Top-K ECI ranking table
# =============================================================================
st.subheader("Top investors by ECI rank")

c_year, c_topk = st.columns([2, 1])
with c_year:
    snapshot_year = st.slider("Snapshot year", min_year, max_year, max_year, key="eci_topk_year")
with c_topk:
    top_k = st.number_input("Top K", min_value=5, max_value=50, value=15, step=5, key="eci_topk_n")

eci_snap = view[(view["year"] == snapshot_year) & view["eci_score"].notna()].copy()
if eci_snap.empty:
    st.info(f"No ECI rows for year {snapshot_year} ({window_choice}). Try a different year.")
else:
    side_cols = st.columns(2)
    for col, side_val, side_name in [
        (side_cols[0], 0, "Early-stage (Set 0)"),
        (side_cols[1], 1, "Late-stage (Set 1)"),
    ]:
        with col:
            sub = (
                eci_snap[eci_snap["set"] == side_val]
                .sort_values("eci_rank")
                .head(int(top_k))
            )
            total_lcc = eci_snap[eci_snap["set"] == side_val].shape[0]
            st.markdown(f"**{side_name}** — top {len(sub)} of {total_lcc} LCC nodes")
            if sub.empty:
                st.caption("No ECI rows for this side.")
                continue
            disp = sub[["eci_rank", "node", "eci_score", "eci_pctile", "rca_degree", "strength", "degree"]].copy()
            disp.columns = ["rank", "node", "eci", "pctile", "rca_deg", "strength", "degree"]
            disp["eci"] = disp["eci"].round(2)
            disp["pctile"] = (disp["pctile"] * 100).round(1)
            st.dataframe(disp.reset_index(drop=True), use_container_width=True,
                         height=min(38 * (len(sub) + 1), 600))

# =============================================================================
# Section 4 — ECI vs RCA-degree scatter
# =============================================================================
st.subheader(f"ECI vs diversity / ubiquity — {window_choice}, year {snapshot_year}")
st.markdown(
    "X-axis is `rca_degree` (log) — **diversity** for early-stage funds, "
    "**ubiquity** for late-stage funds. ECI is designed to be *independent* of raw "
    "degree; a near-zero Spearman confirms this. Hover for individual node names."
)

sc_view = eci_snap.copy()
SIDE_LEGEND = {0: "Early-stage — diversity", 1: "Late-stage — ubiquity"}
SIDE_LEGEND_COLORS = {
    "Early-stage — diversity": SET_COLORS[0],
    "Late-stage — ubiquity": SET_COLORS[1],
}
sc_view["side_legend"] = sc_view["set"].map(SIDE_LEGEND)

if not sc_view.empty and "rca_degree" in sc_view.columns:
    spear_e = float("nan")
    spear_l = float("nan")
    if (sc_view["set"] == 0).sum() > 1:
        spear_e = sc_view[sc_view["set"] == 0][["eci_score", "rca_degree"]].corr(method="spearman").iloc[0, 1]
    if (sc_view["set"] == 1).sum() > 1:
        spear_l = sc_view[sc_view["set"] == 1][["eci_score", "rca_degree"]].corr(method="spearman").iloc[0, 1]

    sc_fig = px.scatter(
        sc_view,
        x="rca_degree", y="eci_pctile",
        color="side_legend", color_discrete_map=SIDE_LEGEND_COLORS,
        opacity=0.55,
        hover_data={"node": True, "eci_score": ":.2f", "degree": True, "strength": True},
        log_x=True,
        title=(
            f"{community} — ECI percentile vs RCA-degree<br>"
            f"<sub>Spearman: early/diversity={spear_e:+.2f}, late/ubiquity={spear_l:+.2f}</sub>"
        ),
        labels={
            "rca_degree": "diversity (early) / ubiquity (late), log",
            "eci_pctile": "ECI percentile",
            "side_legend": "Side",
        },
    )
    sc_fig.update_layout(height=440, legend=dict(title_text=""))
    st.plotly_chart(sc_fig, use_container_width=True, key="eci_scatter")

# =============================================================================
# Section 5 — Per-node ECI trajectory
# =============================================================================
st.subheader("Per-node ECI trajectory")
st.markdown(
    "Track how a fund's ECI percentile and rank move over time. "
    "Useful for identifying movers (funds whose ECI rank shifted substantially "
    "across windows)."
)

node_options = sorted(
    view[view["eci_score"].notna()]["node"].unique().tolist()
)
selected_nodes = st.multiselect(
    f"Nodes (typeahead, max 10) — {len(node_options)} LCC candidates",
    node_options, default=[], max_selections=10, key="eci_traj_nodes",
)

if selected_nodes:
    traj = view[view["node"].isin(selected_nodes)].sort_values(["node", "year"])
    traj_fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("ECI percentile (1 = highest)", "ECI rank (1 = highest)"),
        horizontal_spacing=0.12,
    )
    for n in selected_nodes:
        sub = traj[traj["node"] == n]
        traj_fig.add_trace(
            go.Scatter(x=sub["year"], y=sub["eci_pctile"], mode="lines+markers",
                       name=n, legendgroup=n),
            row=1, col=1,
        )
        traj_fig.add_trace(
            go.Scatter(x=sub["year"], y=sub["eci_rank"], mode="lines+markers",
                       name=n, legendgroup=n, showlegend=False),
            row=1, col=2,
        )
    traj_fig.update_yaxes(range=[0, 1.02], row=1, col=1)
    traj_fig.update_yaxes(autorange="reversed", row=1, col=2)
    traj_fig.update_layout(height=440, hovermode="x unified")
    st.plotly_chart(traj_fig, use_container_width=True, key="eci_traj")

# =============================================================================
# Section 6 — RCA matrix heatmap
# =============================================================================
st.subheader("RCA matrix — top-K × top-K submatrix")
st.markdown(
    "Binary `M_RCA` restricted to the top-K early × top-K late investors sorted by "
    "`rca_degree` descending. A blue cell means RCA(e, l) ≥ 1. With this ordering, "
    "the **nestedness staircase** is dense in the upper-left, tapering toward the "
    "lower-right."
)

ROLLING_WINDOW = 5


@st.cache_data(ttl=600)
def _load_edges(method: str, comm_id_str: str) -> pd.DataFrame:
    return query_df(
        f"SELECT \"Source\", \"Target\", year FROM graph.edges "
        f"WHERE clustering_method = '{method}' "
        f"AND community = (SELECT community_id FROM graph.network "
        f"WHERE clustering_method = '{method}' AND community_id >= 0 LIMIT 1)"
    )


@st.cache_data(ttl=600)
def _load_edges_comm(method: str, community_label: str) -> pd.DataFrame:
    comm_id = int(community_label.split(" ")[-1])
    return query_df(
        f"SELECT \"Source\", \"Target\", year FROM graph.edges "
        f"WHERE clustering_method = '{method}' AND community = {comm_id} AND year IS NOT NULL"
    )


@st.cache_data(ttl=600)
def _load_nodes_comm(method: str, community_label: str) -> pd.DataFrame:
    comm_id = int(community_label.split(" ")[-1])
    return query_df(
        f"SELECT node, set FROM graph.network "
        f"WHERE clustering_method = '{method}' AND community_id = {comm_id}"
    )


def _build_w_matrix(edges: pd.DataFrame, year: int, window_type: str,
                    left_nodes: list[str], right_nodes: list[str]) -> np.ndarray:
    if window_type == "cumulative":
        slice_ = edges[edges["year"] <= year]
    else:
        slice_ = edges[(edges["year"] >= year - ROLLING_WINDOW) & (edges["year"] <= year)]
    if slice_.empty:
        return np.zeros((len(left_nodes), len(right_nodes)), dtype=np.int64)
    counts = slice_.groupby(["Source", "Target"]).size()
    li = {n: i for i, n in enumerate(left_nodes)}
    ri = {n: j for j, n in enumerate(right_nodes)}
    W = np.zeros((len(left_nodes), len(right_nodes)), dtype=np.int64)
    for (s, t), v in counts.items():
        if s in li and t in ri:
            W[li[s], ri[t]] = v
    return W


def _rca_binary_dense(W: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    s_rows = W.sum(axis=1).astype(np.float64)
    s_cols = W.sum(axis=0).astype(np.float64)
    S = float(W.sum())
    if S <= 0:
        return np.zeros_like(W, dtype=np.int8), np.zeros_like(W, dtype=np.float64)
    denom = np.outer(s_rows, s_cols)
    rca = np.where(denom > 0, W * S / np.where(denom > 0, denom, 1.0), 0.0)
    return (rca >= 1.0).astype(np.int8), rca


c_mat1, c_mat2 = st.columns([1, 1])
with c_mat1:
    matrix_year = st.slider("Year", min_year, max_year, snapshot_year, key="eci_mat_year")
with c_mat2:
    matrix_k = st.number_input("K (top-K per side)", min_value=10, max_value=200,
                                value=40, step=10, key="eci_mat_k")
weighted_overlay = st.checkbox("Show RCA magnitude (continuous, clipped at 5)",
                                value=False, key="eci_mat_weighted")

mview = eci_snap[eci_snap["year"] == matrix_year] if "year" in eci_snap.columns else pd.DataFrame()
if mview.empty:
    st.info(f"No ECI rows for year {matrix_year} ({window_choice}).")
else:
    early_ranked = (
        mview[mview["set"] == 0].dropna(subset=["eci_rank"])
        .sort_values("rca_degree", ascending=False).head(int(matrix_k))
    )
    late_ranked = (
        mview[mview["set"] == 1].dropna(subset=["eci_rank"])
        .sort_values("rca_degree", ascending=False).head(int(matrix_k))
    )

    if early_ranked.empty or late_ranked.empty:
        st.info("Not enough ECI-ranked nodes for a heatmap in this snapshot.")
    else:
        try:
            all_edges = _load_edges_comm(method, community)
            nodes_df = _load_nodes_comm(method, community)
            left_full = nodes_df.loc[nodes_df["set"] == 0, "node"].tolist()
            right_full = nodes_df.loc[nodes_df["set"] == 1, "node"].tolist()

            W_full = _build_w_matrix(all_edges, int(matrix_year), window_choice, left_full, right_full)
            M_rca_full, rca_full = _rca_binary_dense(W_full)

            early_idx = [left_full.index(n) for n in early_ranked["node"] if n in left_full]
            late_idx = [right_full.index(n) for n in late_ranked["node"] if n in right_full]

            if early_idx and late_idx:
                sub_M = M_rca_full[np.ix_(early_idx, late_idx)]
                sub_rca = rca_full[np.ix_(early_idx, late_idx)]

                early_sub = early_ranked[early_ranked["node"].isin(left_full)]
                late_sub = late_ranked[late_ranked["node"].isin(right_full)]
                early_labels = [
                    f"{r['node']} — div {int(r['rca_degree'])} (ECI #{int(r['eci_rank'])})"
                    for _, r in early_sub.iterrows()
                ]
                late_labels = [
                    f"{r['node']} — ubq {int(r['rca_degree'])} (ECI #{int(r['eci_rank'])})"
                    for _, r in late_sub.iterrows()
                ]

                # Re-order rows and columns by RCA ≥ 1 occurrence count in the submatrix
                row_order = np.argsort(sub_M.sum(axis=1).astype(np.int64))[::-1]
                col_order = np.argsort(sub_M.sum(axis=0).astype(np.int64))[::-1]
                sub_M = sub_M[np.ix_(row_order, col_order)]
                sub_rca = sub_rca[np.ix_(row_order, col_order)]
                early_labels = [early_labels[i] for i in row_order]
                late_labels = [late_labels[i] for i in col_order]

                if weighted_overlay:
                    heat = np.clip(sub_rca, 0, 5)
                    colorbar_title = "RCA (clipped at 5)"
                    colorscale = "Blues"
                else:
                    heat = sub_M.astype(np.float64)
                    colorbar_title = "RCA ≥ 1"
                    colorscale = [[0, "#f8f9fa"], [1, "#1f4e79"]]

                density = float(sub_M.sum() / sub_M.size) if sub_M.size else float("nan")
                st.caption(
                    f"**{community} · {matrix_year} · {window_choice}** — "
                    f"{sub_M.shape[0]}×{sub_M.shape[1]} submatrix. "
                    f"Fill = {density:.1%} of cells have RCA ≥ 1."
                )

                heat_fig = go.Figure(data=go.Heatmap(
                    z=heat, x=late_labels, y=early_labels,
                    colorscale=colorscale,
                    colorbar=dict(title=colorbar_title),
                    hovertemplate=(
                        "Early: %{y}<br>Late: %{x}<br>"
                        + ("RCA: %{z:.2f}" if weighted_overlay else "RCA ≥ 1: %{z:.0f}")
                        + "<extra></extra>"
                    ),
                ))
                heat_fig.update_layout(
                    height=max(500, 14 * len(early_labels)),
                    xaxis=dict(side="top", tickangle=-60, tickfont=dict(size=9),
                               title="Late-stage — sorted by ubiquity desc"),
                    yaxis=dict(autorange="reversed", tickfont=dict(size=9),
                               title="Early-stage — sorted by diversity desc"),
                    margin=dict(l=280, r=40, t=180, b=40),
                )
                st.plotly_chart(heat_fig, use_container_width=True, key="eci_heat")

                half_r = max(1, sub_M.shape[0] // 2)
                half_c = max(1, sub_M.shape[1] // 2)
                ul = float(sub_M[:half_r, :half_c].mean())
                ur = float(sub_M[:half_r, half_c:].mean())
                ll = float(sub_M[half_r:, :half_c].mean())
                lr = float(sub_M[half_r:, half_c:].mean())
                st.markdown("**Quadrant density** (RCA-1 fill rate by diversity/ubiquity half):")
                q_cols = st.columns(4)
                q_cols[0].metric("UL — high × high", f"{ul:.1%}")
                q_cols[1].metric("UR — high div × low ubq", f"{ur:.1%}")
                q_cols[2].metric("LL — low div × high ubq", f"{ll:.1%}")
                q_cols[3].metric("LR — low × low", f"{lr:.1%}")
                st.caption(
                    "Nestedness staircase: **UL ≫ LR** with **LL > UR** is the Bascompte "
                    "inclusion pattern (generalists include specialists' partners)."
                )
        except Exception as exc:
            st.warning(f"Could not render RCA heatmap: {exc}")

# =============================================================================
# Section 7 — Raw data viewer
# =============================================================================
with st.expander("Raw data"):
    hide_nan = st.checkbox("Hide rows with NaN ECI (outside LCC)", value=True, key="eci_raw_nan")
    df_raw = view.copy()
    if hide_nan:
        df_raw = df_raw.dropna(subset=["eci_score"])
    disp_cols = ["year", "window_type", "node", "set", "set_label", "eci_rank", "eci_score",
                 "eci_pctile", "rca_degree", "strength", "degree", "lcc_n_left", "lcc_n_right",
                 "n_edges_rca", "tier_coupling", "spectral_gap"]
    disp_cols = [c for c in disp_cols if c in df_raw.columns]
    st.dataframe(
        df_raw[disp_cols].sort_values(["window_type", "year", "set", "eci_rank"]).reset_index(drop=True),
        use_container_width=True, height=420,
    )
