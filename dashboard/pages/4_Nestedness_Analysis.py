"""Interactive Johnson Nestedness Analysis dashboard."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, SET_COLORS, SET_LABELS, experiment_selector

st.set_page_config(page_title="Nestedness Analysis", layout="wide")

selected = experiment_selector()
if not selected:
    st.stop()

st.title("Johnson Nestedness Analysis")
st.markdown(
    "Interactive exploration of the Johnson et al. (2013) nestedness metrics "
    "computed on the bipartite VC syndication network communities."
)

# Load data
df = query_df("SELECT * FROM experiment.johnson_nestedness")
communities = sorted(df["community"].unique())

# Sidebar filters
st.sidebar.header("Filters")
selected_communities = st.sidebar.multiselect(
    "Communities", communities, default=communities
)
set_filter = st.sidebar.radio("Bipartite Set", ["All", "Late-stage (Set 0)", "Early-stage (Set 1)"])
degree_range = st.sidebar.slider(
    "Degree Range",
    int(df["degree"].min()),
    int(df["degree"].max()),
    (int(df["degree"].min()), int(df["degree"].max())),
)
log_y = st.sidebar.checkbox("Log scale (y-axis)", value=False)

# Apply filters
mask = (
    df["community"].isin(selected_communities)
    & df["degree"].between(degree_range[0], degree_range[1])
)
if set_filter == "Late-stage (Set 0)":
    mask &= df["set"] == 0
elif set_filter == "Early-stage (Set 1)":
    mask &= df["set"] == 1
filtered = df[mask].copy()
filtered["set_label"] = filtered["set"].map(SET_LABELS)

# =============================================================================
# 1. Global Nestedness Comparison (bar chart)
# =============================================================================
st.subheader("Global Nestedness (g_norm) Across Communities")

summary_rows = []
for comm in communities:
    group = df[df["community"] == comm]
    summary_rows.append(
        {
            "Community": comm,
            "Nodes": len(group),
            "Set 0 (late)": int((group["set"] == 0).sum()),
            "Set 1 (early)": int((group["set"] == 1).sum()),
            "g_norm": group["g_norm"].iloc[0],
            "g_raw": group["g_raw"].iloc[0],
            "g_conf": group["g_conf"].iloc[0],
            "Mean Degree": round(group["degree"].mean(), 1),
            "Mean Local g_norm": round(group["local_g_norm"].mean(), 4),
        }
    )
summary_df = pd.DataFrame(summary_rows)

col1, col2 = st.columns([1, 1])

with col1:
    fig_bar = px.bar(
        summary_df,
        x="Community",
        y="g_norm",
        color="g_norm",
        color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
        hover_data=["g_raw", "g_conf", "Nodes"],
        text=summary_df["g_norm"].apply(lambda x: f"{x:.4f}"),
    )
    fig_bar.add_hline(
        y=1.0,
        line_dash="dash",
        line_color="red",
        annotation_text="Random expectation (g=1)",
        annotation_position="top left",
    )
    fig_bar.update_layout(
        yaxis_title="g_norm (Normalized Nestedness)",
        showlegend=False,
        height=400,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.markdown("**Summary Table**")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

# =============================================================================
# 2. Scatter: Degree vs Local Nestedness
# =============================================================================
st.subheader("Degree vs Local Nestedness")

view_mode = st.radio(
    "View mode", ["Faceted (all communities)", "Single community"], horizontal=True
)

if view_mode == "Faceted (all communities)":
    fig_scatter = px.scatter(
        filtered,
        x="degree",
        y="local_g_norm",
        color="set_label",
        facet_col="community",
        color_discrete_map={
            SET_LABELS[0]: SET_COLORS[0],
            SET_LABELS[1]: SET_COLORS[1],
        },
        hover_data=["node", "degree", "local_g_norm"],
        opacity=0.5,
        render_mode="webgl",
    )
    fig_scatter.update_layout(height=500)
    if log_y:
        fig_scatter.update_yaxes(type="log")
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    single_comm = st.selectbox("Select community", selected_communities)
    comm_data = filtered[filtered["community"] == single_comm]
    fig_scatter = px.scatter(
        comm_data,
        x="degree",
        y="local_g_norm",
        color="set_label",
        color_discrete_map={
            SET_LABELS[0]: SET_COLORS[0],
            SET_LABELS[1]: SET_COLORS[1],
        },
        hover_data=["node", "degree", "local_g_norm"],
        opacity=0.5,
        render_mode="webgl",
        trendline="ols",
        title=f"{single_comm} - Degree vs Local Nestedness",
    )
    fig_scatter.update_layout(height=500)
    if log_y:
        fig_scatter.update_yaxes(type="log")
    st.plotly_chart(fig_scatter, use_container_width=True)

# =============================================================================
# 3. Histogram: Local g_norm Distribution
# =============================================================================
st.subheader("Local g_norm Distribution by Set")

fig_hist = px.histogram(
    filtered,
    x="local_g_norm",
    color="set_label",
    facet_col="community",
    color_discrete_map={
        SET_LABELS[0]: SET_COLORS[0],
        SET_LABELS[1]: SET_COLORS[1],
    },
    barmode="overlay",
    opacity=0.6,
    nbins=40,
)
fig_hist.update_layout(height=400)
st.plotly_chart(fig_hist, use_container_width=True)

# =============================================================================
# 4. Asymmetry Analysis
# =============================================================================
st.subheader("Asymmetric Nestedness: Late-stage vs Early-stage")
st.markdown(
    "Mean local g_norm by bipartite set per community. "
    "The paper finds that **Community 2 (Silicon Valley)** exhibits significantly higher "
    "nestedness for late-stage investors than early-stage ones."
)

asym_rows = []
for comm in selected_communities:
    group = df[df["community"] == comm]
    for set_id, label in SET_LABELS.items():
        set_data = group[group["set"] == set_id]["local_g_norm"]
        asym_rows.append(
            {
                "Community": comm,
                "Set": label,
                "Mean Local g_norm": set_data.mean(),
                "Std": set_data.std(),
                "Count": len(set_data),
            }
        )
asym_df = pd.DataFrame(asym_rows)

fig_asym = px.bar(
    asym_df,
    x="Community",
    y="Mean Local g_norm",
    color="Set",
    barmode="group",
    error_y="Std",
    color_discrete_map={
        SET_LABELS[0]: SET_COLORS[0],
        SET_LABELS[1]: SET_COLORS[1],
    },
    hover_data=["Count"],
)
fig_asym.update_layout(height=400)
st.plotly_chart(fig_asym, use_container_width=True)

# =============================================================================
# 5. Correlation Statistics
# =============================================================================
# st.subheader("Correlation: Degree vs Local Nestedness")

# corr_rows = []
# for comm in selected_communities:
#     group = df[df["community"] == comm].dropna(subset=["degree", "local_g_norm"])
#     if len(group) > 2:
#         r_p, p_p = pearsonr(group["degree"], group["local_g_norm"])
#         r_s, p_s = spearmanr(group["degree"], group["local_g_norm"])
#         corr_rows.append(
#             {
#                 "Community": comm,
#                 "Set": "Overall",
#                 "Pearson r": round(r_p, 4),
#                 "Pearson p": f"{p_p:.2e}",
#                 "Spearman rho": round(r_s, 4),
#                 "Spearman p": f"{p_s:.2e}",
#                 "N": len(group),
#             }
#         )
#     for set_id, label in SET_LABELS.items():
#         set_data = group[group["set"] == set_id]
#         if len(set_data) > 2:
#             r_p, p_p = pearsonr(set_data["degree"], set_data["local_g_norm"])
#             r_s, p_s = spearmanr(set_data["degree"], set_data["local_g_norm"])
#             corr_rows.append(
#                 {
#                     "Community": comm,
#                     "Set": label,
#                     "Pearson r": round(r_p, 4),
#                     "Pearson p": f"{p_p:.2e}",
#                     "Spearman rho": round(r_s, 4),
#                     "Spearman p": f"{p_s:.2e}",
#                     "N": len(set_data),
#                 }
#             )

# if corr_rows:
#     corr_df = pd.DataFrame(corr_rows)
#     st.dataframe(corr_df, use_container_width=True, hide_index=True)

# =============================================================================
# 6. Node-Level Data Table
# =============================================================================
st.subheader("Node-Level Data")

search = st.text_input("Search investor name", "")
table_data = filtered.copy()
if search:
    table_data = table_data[table_data["node"].str.contains(search, case=False, na=False)]

st.dataframe(
    table_data[["node", "community", "set", "degree", "local_g_norm", "local_g_raw", "g_norm"]]
    .sort_values("degree", ascending=False)
    .reset_index(drop=True),
    use_container_width=True,
    height=400,
)
