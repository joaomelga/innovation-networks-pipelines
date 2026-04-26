"""Interactive Johnson Nestedness Analysis dashboard."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr

import plotly.colors as pc
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
def _community_sort_key(name):
    parts = name.rsplit(" ", 1)
    try:
        return int(parts[-1])
    except ValueError:
        return float("inf")

communities = sorted(df["community"].unique(), key=_community_sort_key)
community_sizes = df.groupby("community").size().to_dict()

_GEO_SQL = """
WITH investor_geo AS (
    SELECT Source AS node, ANY_VALUE(investor_country_left) AS country, ANY_VALUE(investor_region_left) AS region
    FROM graph.edges
    GROUP BY Source
    UNION ALL
    SELECT Target AS node, ANY_VALUE(investor_country_right) AS country, ANY_VALUE(investor_region_right) AS region
    FROM graph.edges
    GROUP BY Target
),
deduped AS (
    SELECT node, ANY_VALUE(country) AS country, ANY_VALUE(region) AS region
    FROM investor_geo
    GROUP BY node
)
SELECT
    n.community,
    n.set,
    d.country,
    d.region,
    COUNT(DISTINCT n.node) AS node_count
FROM experiment.johnson_nestedness n
JOIN deduped d ON n.node = d.node
WHERE d.country IS NOT NULL
GROUP BY n.community, n.set, d.country, d.region
ORDER BY n.community, node_count DESC
"""
geo_df = query_df(_GEO_SQL)

# Sidebar filters
st.sidebar.header("Filters")
_all_sizes = [community_sizes.get(c, 0) for c in communities]
_size_min, _size_max = min(_all_sizes), max(_all_sizes)
size_range = st.sidebar.slider(
    "Community Size (nodes)",
    min_value=_size_min,
    max_value=_size_max,
    value=(_size_min, _size_max),
)
size_filtered_communities = [
    c for c in communities if size_range[0] <= community_sizes.get(c, 0) <= size_range[1]
]
selected_communities = st.sidebar.multiselect(
    "Communities", size_filtered_communities, default=size_filtered_communities
)
set_filter = st.sidebar.radio("Bipartite Set", ["All", "Late-stage (Set 0)", "Early-stage (Set 1)"])
degree_range = st.sidebar.slider(
    "Degree Range",
    int(df["degree"].min()),
    int(df["degree"].max()),
    (int(df["degree"].min()), int(df["degree"].max())),
)
top_n = st.sidebar.slider("Top N countries / Regions", 5, 30, 10)

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
for comm in selected_communities:
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

_COMMUNITY_SCALE = ["#f39c12", "#2ecc71", "#2ec7cc"]
_g_min = summary_df["g_norm"].min()
_g_max = summary_df["g_norm"].max()
_g_denom = (_g_max - _g_min) if _g_max != _g_min else 1.0
community_colors = {
    row["Community"]: pc.sample_colorscale(
        _COMMUNITY_SCALE, (row["g_norm"] - _g_min) / _g_denom
    )[0]
    for _, row in summary_df.iterrows()
}

col1, col2 = st.columns([1, 1])

with col1:
    fig_bar = px.bar(
        summary_df,
        x="Community",
        y="g_norm",
        color="g_norm",
        color_continuous_scale=_COMMUNITY_SCALE,
        hover_data=["g_raw", "g_conf", "Nodes"],
        text=summary_df["g_norm"].apply(lambda x: f"{x:.4f}"),
        category_orders={"Community": list(summary_df["Community"])},
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

_, _scatter_ctrl_right = st.columns([2, 1])

with _scatter_ctrl_right:
    _log_cols = st.columns(2)
    log_x = _log_cols[0].checkbox("Log x-axis", value=False)
    log_y = _log_cols[1].checkbox("Log y-axis", value=False)


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
if log_x:
    fig_scatter.update_xaxes(type="log")
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
# 5. Geographic Distribution
# =============================================================================
st.subheader("Geographic Distribution of Community Nodes")

if not geo_df.empty:
    # Apply filters
    geo_mask = geo_df["community"].isin(selected_communities)
    if set_filter == "Late-stage (Set 0)":
        geo_mask &= geo_df["set"] == 0
    elif set_filter == "Early-stage (Set 1)":
        geo_mask &= geo_df["set"] == 1
    geo_filtered = geo_df[geo_mask]

    geo_col1, geo_col2 = st.columns([1, 1])

    with geo_col1:
        st.markdown("**Top Countries**")
        country_agg = (
            geo_filtered.groupby(["community", "country"])["node_count"]
            .sum()
            .reset_index()
        )
        top_countries = (
            country_agg.groupby("country")["node_count"]
            .sum()
            .nlargest(top_n)
            .index
        )
        country_plot = country_agg[country_agg["country"].isin(top_countries)].copy()
        country_plot["country"] = pd.Categorical(
            country_plot["country"],
            categories=top_countries[::-1],
            ordered=True,
        )
        country_plot = country_plot.sort_values("country")
        fig_country = px.bar(
            country_plot,
            x="node_count",
            y="country",
            color="community",
            color_discrete_map=community_colors,
            orientation="h",
            barmode="group",
            labels={"node_count": "Node Count", "country": "Country"},
        )
        fig_country.update_layout(height=max(300, top_n * 28), yaxis_title="")
        st.plotly_chart(fig_country, use_container_width=True)

    with geo_col2:
        st.markdown("**Regions**")
        region_agg = (
            geo_filtered.groupby(["community", "region"])["node_count"]
            .sum()
            .reset_index()
        )
        top_regions = (
            region_agg.groupby("region")["node_count"]
            .sum()
            .sort_values(ascending=False)
            .nlargest(top_n)
            .index
        )
        region_agg["region"] = pd.Categorical(
            region_agg["region"],
            categories=top_regions[::-1],
            ordered=True,
        )
        region_agg = region_agg.sort_values("region")
        fig_region = px.bar(
            region_agg,
            x="node_count",
            y="region",
            color="community",
            color_discrete_map=community_colors,
            orientation="h",
            barmode="group",
            labels={"node_count": "Node Count", "region": "Region"},
        )
        fig_region.update_layout(height=max(300, len(top_regions) * 28), yaxis_title="")
        st.plotly_chart(fig_region, use_container_width=True)
else:
    st.info("Geographic data not available for this experiment.")

# =============================================================================
# 6. Correlation Statistics
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
# 7. Node-Level Data Table
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
