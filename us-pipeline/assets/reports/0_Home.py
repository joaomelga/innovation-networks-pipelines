"""Home page — Pipeline Overview for US VC Syndication Networks."""

import streamlit as st
from db import query_df, format_number

st.set_page_config(
    page_title="US VC Syndication Networks",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

st.title("US VC Syndication Networks Pipeline")
st.markdown(
    "Interactive dashboards for the **Staged Bipartite Venture Networks** analysis — "
    "exploring nestedness and community structure in US venture capital syndication networks."
)

# Pipeline DAG
st.subheader("Pipeline Architecture")
st.graphviz_chart(
    """
    digraph pipeline {
        rankdir=LR;
        node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10];
        edge [color="#888888"];

        subgraph cluster_raw {
            label="Raw"; style=filled; color="#e8f4fd";
            raw_companies [label="companies\\n(820K)", fillcolor="#d4e6f1"];
            raw_investors [label="investors\\n(39K)", fillcolor="#d4e6f1"];
            raw_investments [label="investments\\n(424K)", fillcolor="#d4e6f1"];
            raw_funding_rounds [label="funding_rounds\\n(268K)", fillcolor="#d4e6f1"];
        }

        subgraph cluster_staging {
            label="Staging"; style=filled; color="#fef9e7";
            stg_companies [label="companies_clean\\n(597K)", fillcolor="#fdebd0"];
            stg_inv_clean [label="investments_clean\\n(390K)", fillcolor="#fdebd0"];
            stg_inv_funded [label="investments_funded\\n(148K)", fillcolor="#fdebd0"];
        }

        subgraph cluster_core {
            label="Core"; style=filled; color="#eafaf1";
            core_vc [label="vc_investments\\n(105K)", fillcolor="#d5f5e3"];
            core_pairs [label="investment_pairs\\n(156K)", fillcolor="#d5f5e3"];
        }

        subgraph cluster_graph {
            label="Graph"; style=filled; color="#f4ecf7";
            graph_network [label="network\\n(15K nodes)", fillcolor="#e8daef"];
            graph_edges [label="edges\\n(156K)", fillcolor="#e8daef"];
        }

        subgraph cluster_experiment {
            label="Experiment"; style=filled; color="#fdedec";
            exp_nestedness [label="johnson_nestedness\\n(12.5K)", fillcolor="#fadbd8"];
        }

        raw_companies -> stg_companies;
        raw_investments -> stg_inv_clean;
        stg_companies -> stg_inv_funded;
        stg_inv_clean -> stg_inv_funded;
        stg_inv_funded -> core_vc;
        core_vc -> core_pairs;
        core_pairs -> graph_network;
        core_pairs -> graph_edges;
        graph_network -> graph_edges;
        graph_network -> exp_nestedness;
        graph_edges -> exp_nestedness;
    }
    """,
    use_container_width=True,
)

# Key Metrics
st.subheader("Key Metrics")

counts = {}
for table in [
    "raw.companies",
    "raw.investors",
    "raw.investments",
    "raw.funding_rounds",
    "staging.companies_clean",
    "staging.investments_clean",
    "staging.investments_funded",
    "core.vc_investments",
    "core.investment_pairs",
    "graph.network",
    "graph.edges",
]:
    row = query_df(f"SELECT COUNT(*) AS cnt FROM {table}")
    counts[table] = int(row["cnt"].iloc[0])

n_communities = int(
    query_df("SELECT COUNT(DISTINCT community_id) AS n FROM graph.network")["n"].iloc[0]
)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Raw Companies", format_number(counts["raw.companies"]))
col2.metric(
    "Clean Companies",
    format_number(counts["staging.companies_clean"]),
    delta=f"{counts['staging.companies_clean'] / counts['raw.companies'] * 100:.0f}% retained",
)
col3.metric(
    "Funded Investments",
    format_number(counts["staging.investments_funded"]),
    delta=f"{counts['staging.investments_funded'] / counts['raw.investments'] * 100:.0f}% of raw",
)
col4.metric(
    "VC Investments",
    format_number(counts["core.vc_investments"]),
    delta=f"{counts['core.vc_investments'] / counts['staging.investments_funded'] * 100:.0f}% of funded",
)

col5, col6, col7, col8 = st.columns(4)
col5.metric("Investment Pairs", format_number(counts["core.investment_pairs"]))
col6.metric("Graph Nodes", format_number(counts["graph.network"]))
col7.metric("Graph Edges", format_number(counts["graph.edges"]))
col8.metric("Communities Detected", n_communities)

# Nestedness Headline
st.subheader("Johnson Nestedness — Top Communities")

nestedness_df = query_df(
    """
    SELECT community,
           COUNT(*) AS nodes,
           MIN(g_norm) AS g_norm,
           MIN(g_raw) AS g_raw,
           MIN(g_conf) AS g_conf
    FROM experiment.johnson_nestedness
    GROUP BY community
    ORDER BY community
    """
)

cols = st.columns(len(nestedness_df))
for i, row in nestedness_df.iterrows():
    with cols[i]:
        g = row["g_norm"]
        label = "Highly Nested" if g > 2 else "Random" if abs(g - 1) < 0.05 else ""
        st.metric(
            label=row["community"],
            value=f"{g:.4f}",
            delta=label,
            delta_color="normal" if g > 2 else "off",
        )
        st.caption(f"g_raw={row['g_raw']:.4f} | g_conf={row['g_conf']:.4f} | {int(row['nodes'])} nodes")

st.markdown("---")
st.markdown(
    "_Navigate using the sidebar to explore pipeline stages, data, communities, and nestedness analysis._"
)
