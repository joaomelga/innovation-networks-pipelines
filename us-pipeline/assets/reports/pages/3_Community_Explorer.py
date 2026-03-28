"""Community Explorer - deep-dive into detected communities."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import networkx as nx

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, SET_COLORS, SET_LABELS

st.set_page_config(page_title="Community Explorer", layout="wide")
st.title("Community Explorer")
st.markdown(
    "Explore the community structure detected by greedy modularity optimization "
    "on the bipartite VC syndication network."
)

# --- Load data ---
network_df = query_df("SELECT * FROM graph.network")
edges_df = query_df(
    "SELECT Source, Target, community, community_left, community_right, org_uuid FROM graph.edges"
)

# Community stats
comm_stats = (
    network_df.groupby("community_id")
    .agg(
        nodes=("node", "count"),
        set_0=("set", lambda s: (s == 0).sum()),
        set_1=("set", lambda s: (s == 1).sum()),
        community_size=("community_size", "first"),
    )
    .reset_index()
    .sort_values("nodes", ascending=False)
)
comm_stats["set_ratio"] = (comm_stats["set_1"] / comm_stats["set_0"]).round(2)

# Edge counts per community
edge_counts = edges_df[edges_df["community"] >= 0].groupby("community").size().reset_index(name="edges")
comm_stats = comm_stats.merge(edge_counts, left_on="community_id", right_on="community", how="left")
comm_stats["edges"] = comm_stats["edges"].fillna(0).astype(int)
comm_stats = comm_stats.drop(columns=["community"], errors="ignore")

# =============================================================================
# 1. Community Size Distribution
# =============================================================================
st.subheader("Community Size Distribution")

top_n = st.slider("Show top N communities", 5, len(comm_stats), 20)
plot_data = comm_stats.head(top_n).copy()
plot_data["label"] = "Community " + plot_data["community_id"].astype(str)
plot_data["highlight"] = plot_data["community_id"].isin([0, 1, 2]).map(
    {True: "Top 3 (nestedness computed)", False: "Other"}
)

fig_size = px.bar(
    plot_data,
    x="label",
    y="nodes",
    color="highlight",
    color_discrete_map={"Top 3 (nestedness computed)": "#2ecc71", "Other": "#bdc3c7"},
    hover_data=["set_0", "set_1", "edges", "set_ratio"],
    log_y=True,
)
fig_size.update_layout(
    xaxis_title="Community",
    yaxis_title="Number of Nodes (log scale)",
    height=400,
    xaxis_tickangle=-45,
)
st.plotly_chart(fig_size, use_container_width=True)

# =============================================================================
# 2. Community Composition (stacked bar)
# =============================================================================
st.subheader("Community Composition: Late-stage vs Early-stage")

comp_data = comm_stats.head(top_n).copy()
comp_melted = pd.melt(
    comp_data,
    id_vars=["community_id"],
    value_vars=["set_0", "set_1"],
    var_name="set",
    value_name="count",
)
comp_melted["set"] = comp_melted["set"].map({"set_0": SET_LABELS[0], "set_1": SET_LABELS[1]})
comp_melted["community_label"] = "Community " + comp_melted["community_id"].astype(str)

fig_comp = px.bar(
    comp_melted,
    x="community_label",
    y="count",
    color="set",
    color_discrete_map={SET_LABELS[0]: SET_COLORS[0], SET_LABELS[1]: SET_COLORS[1]},
    barmode="stack",
)
fig_comp.update_layout(
    xaxis_title="Community",
    yaxis_title="Node Count",
    height=400,
    xaxis_tickangle=-45,
)
st.plotly_chart(fig_comp, use_container_width=True)

# =============================================================================
# 3. Set Balance Scatter
# =============================================================================
st.subheader("Set Balance Across Communities")
st.markdown("Each point is a community. Y-axis shows early/late ratio; size = edge count.")

fig_balance = px.scatter(
    comm_stats[comm_stats["nodes"] > 2],
    x="nodes",
    y="set_ratio",
    size="edges",
    hover_data=["community_id", "set_0", "set_1"],
    log_x=True,
    color=comm_stats[comm_stats["nodes"] > 2]["community_id"].isin([0, 1, 2]).map(
        {True: "Top 3", False: "Other"}
    ),
    color_discrete_map={"Top 3": "#2ecc71", "Other": "#95a5a6"},
)
fig_balance.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="Balanced")
fig_balance.update_layout(
    xaxis_title="Community Size (nodes, log)",
    yaxis_title="Early/Late Ratio (set_1 / set_0)",
    height=400,
)
st.plotly_chart(fig_balance, use_container_width=True)

# =============================================================================
# 4. Community Detail Panel
# =============================================================================
st.subheader("Community Detail")

available_comms = sorted(comm_stats["community_id"].tolist())
selected_comm = st.selectbox("Select Community", available_comms, index=0)

comm_nodes = network_df[network_df["community_id"] == selected_comm]
comm_edges = edges_df[
    (edges_df["community_left"] == selected_comm)
    | (edges_df["community_right"] == selected_comm)
]
intra_edges = comm_edges[comm_edges["community"] == selected_comm]
cross_edges = comm_edges[comm_edges["community"] != selected_comm]

# Metrics row
mc1, mc2, mc3, mc4 = st.columns(4)
mc1.metric("Nodes", len(comm_nodes))
mc2.metric("Late-stage (Set 0)", int((comm_nodes["set"] == 0).sum()))
mc3.metric("Early-stage (Set 1)", int((comm_nodes["set"] == 1).sum()))
mc4.metric("Intra-community Edges", len(intra_edges))

st.metric("Cross-community Edges", len(cross_edges))

col_left, col_right = st.columns([1, 1])

# --- Degree Distribution ---
with col_left:
    st.markdown("**Degree Distribution**")
    # Compute degree from edges
    degree_source = intra_edges.groupby("Source").size().reset_index(name="degree")
    degree_target = intra_edges.groupby("Target").size().reset_index(name="degree")
    degree_source.columns = ["node", "degree"]
    degree_target.columns = ["node", "degree"]
    degree_all = pd.concat([degree_source, degree_target]).groupby("node")["degree"].sum().reset_index()
    degree_all = degree_all.merge(
        comm_nodes[["node", "set"]], on="node", how="inner"
    )
    degree_all["set_label"] = degree_all["set"].map(SET_LABELS)

    if not degree_all.empty:
        fig_deg = px.histogram(
            degree_all,
            x="degree",
            color="set_label",
            color_discrete_map={SET_LABELS[0]: SET_COLORS[0], SET_LABELS[1]: SET_COLORS[1]},
            barmode="overlay",
            opacity=0.6,
            nbins=30,
        )
        fig_deg.update_layout(height=350, xaxis_title="Degree", yaxis_title="Count")
        st.plotly_chart(fig_deg, use_container_width=True)

# --- Top Nodes Table ---
with col_right:
    st.markdown("**Top Nodes by Degree**")
    # Try to join nestedness data if available
    nestedness_df = query_df("SELECT node, local_g_norm FROM experiment.johnson_nestedness")
    top_nodes = degree_all.sort_values("degree", ascending=False).head(20).copy()
    top_nodes = top_nodes.merge(nestedness_df, on="node", how="left")
    top_nodes["set_label"] = top_nodes["set"].map(SET_LABELS)
    st.dataframe(
        top_nodes[["node", "set_label", "degree", "local_g_norm"]].reset_index(drop=True),
        use_container_width=True,
        height=350,
    )

# --- Network Visualization ---
st.markdown("**Network Visualization**")
st.markdown("_Showing top-degree nodes with their edges. Colored by set._")

max_nodes = st.slider("Max nodes to display", 20, 500, 100, step=10)

if not degree_all.empty:
    # Subsample to top-degree nodes
    top_degree_nodes = set(degree_all.nlargest(max_nodes, "degree")["node"].tolist())

    # Build subgraph
    sub_edges = intra_edges[
        intra_edges["Source"].isin(top_degree_nodes)
        & intra_edges["Target"].isin(top_degree_nodes)
    ]

    if len(sub_edges) > 0:
        G = nx.Graph()
        for _, row in sub_edges.iterrows():
            G.add_edge(row["Source"], row["Target"])

        # Add node attributes
        node_set_map = dict(zip(comm_nodes["node"], comm_nodes["set"]))
        node_degree_map = dict(zip(degree_all["node"], degree_all["degree"]))

        # Compute layout
        pos = nx.spring_layout(G, seed=42, k=1.5 / np.sqrt(len(G.nodes())), iterations=50)

        # Build edge traces
        edge_x, edge_y = [], []
        for u, v in G.edges():
            x0, y0 = pos[u]
            x1, y1 = pos[v]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            mode="lines",
            line=dict(width=0.3, color="#cccccc"),
            hoverinfo="none",
        )

        # Build node traces (one per set)
        fig_net = go.Figure(data=[edge_trace])

        for set_id in [0, 1]:
            nodes_in_set = [n for n in G.nodes() if node_set_map.get(n) == set_id]
            if not nodes_in_set:
                continue
            x_vals = [pos[n][0] for n in nodes_in_set]
            y_vals = [pos[n][1] for n in nodes_in_set]
            sizes = [max(5, min(30, node_degree_map.get(n, 1))) for n in nodes_in_set]
            hover_texts = [f"{n}<br>Degree: {node_degree_map.get(n, 0)}" for n in nodes_in_set]

            fig_net.add_trace(
                go.Scatter(
                    x=x_vals, y=y_vals,
                    mode="markers",
                    marker=dict(size=sizes, color=SET_COLORS[set_id], line=dict(width=0.5, color="white")),
                    text=hover_texts,
                    hoverinfo="text",
                    name=SET_LABELS[set_id],
                )
            )

        fig_net.update_layout(
            showlegend=True,
            height=600,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            title=f"Community {selected_comm} - Top {min(max_nodes, len(top_degree_nodes))} nodes by degree",
        )
        st.plotly_chart(fig_net, use_container_width=True)
    else:
        st.info("No edges between the selected top-degree nodes.")
else:
    st.info("No degree data available for this community.")
