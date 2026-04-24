"""Home page - Pipeline Overview for VC Syndication Networks."""

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db import (
    SET_0_COLOR,
    SET_1_COLOR,
    discover_experiments,
    experiment_selector,
    format_number,
    query_df,
    query_df_by_experiment,
)

st.set_page_config(
    page_title="VC Syndication Networks",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

st.title("VC Syndication Networks Pipeline")
st.markdown(
    "Interactive dashboards for the **Staged Bipartite Venture Networks** analysis - "
    "exploring nestedness and community structure across experiments."
)

with st.expander("Getting started — click to expand", expanded=not st.session_state.get("_home_visited")):
    st.session_state["_home_visited"] = True
    st.markdown(
        """
**How to navigate**
- Use the **sidebar** (left) to move between dashboard pages:
  - **Home**: pipeline overview and key metrics (you are here)
  - **Data Explorer**: browse raw and processed tables
  - **Community Explorer**: inspect investor communities detected in the network
  - **Nestedness Analysis**: Johnson nestedness scores and visualisations

**Selecting an experiment**
Each experiment corresponds to a specific pipeline run with its own DuckDB database.
Use the **experiment selector** below to choose which run's results to display across all pages.

"""
    )

# Experiment selector
selected = experiment_selector()
if not selected:
    st.stop()

st.info(f"Viewing results from experiment: **{selected}**")

# Experiments Overview
_METHOD_INFO: dict[str, dict] = {
    "modularity": {
        "icon": "🔷",
        "name": "Greedy Modularity Maximization",
        "description": (
            "Partitions the co-investment network by greedily optimising modularity Q "
            "(Clauset, Newman & Moore, 2004). Produces communities that maximise "
            "within-group edge density relative to a random baseline. "
            "Agnostic to nestedness — used here as the structural baseline."
        ),
    },
    "nestlon": {
        "icon": "🔶",
        "name": "NESTLON",
        "description": (
            "Nestedness-aware community detection (Grimm & Tessone, 2017). "
            "Identifies nested components by iteratively extracting the most nested "
            "sub-graph, yielding communities where investor specialisation follows "
            "a strict inclusion hierarchy. Directly aligned with the Johnson nestedness hypothesis."
        ),
    },
}

st.subheader("Experiments Overview")
_all_exps = discover_experiments()
_exp_cols = st.columns(max(len(_all_exps), 1))
for _col, (_exp_name, _db_path) in zip(_exp_cols, _all_exps.items()):
    # Parse clustering_method from config.yml (local mode only; not available in HF mode)
    _method = "unknown"
    if not str(_db_path).startswith("hf://"):
        _config_path = Path(_db_path).parent / "config.yml"
        if _config_path.exists():
            for _line in _config_path.read_text().splitlines():
                if _line.startswith("clustering_method"):
                    _method = _line.split(":", 1)[-1].strip()
                    break
    _info = _METHOD_INFO.get(_method, {"icon": "⚙️", "name": _method, "description": "No description available."})
    _is_active = _exp_name == selected
    with _col:
        st.markdown(
            f"{'**→ ' if _is_active else ''}{_info['icon']} **{_exp_name}**{'** (active)' if _is_active else ''}"
        )
        st.markdown(f"*Method: {_info['name']}*")
        st.caption(_info["description"])

# Pipeline DAG
st.subheader("Pipeline Architecture")
st.graphviz_chart(
    """
    digraph pipeline {
        rankdir=LR;
        node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10];
        edge [color="#888888"];

        subgraph cluster_raw {
            label="Stage 1 · Raw Data"; style=filled; color="#e8f4fd";
            raw_companies     [label="Company Profiles",   fillcolor="#d4e6f1"];
            raw_investors     [label="Investor Profiles",  fillcolor="#d4e6f1"];
            raw_investments   [label="Investment Records", fillcolor="#d4e6f1"];
            raw_funding_rounds [label="Funding Rounds",   fillcolor="#d4e6f1"];
        }

        subgraph cluster_staging {
            label="Stage 2 · Cleaned & Filtered"; style=filled; color="#fef9e7";
            stg_companies  [label="Verified Companies",    fillcolor="#fdebd0"];
            stg_inv_clean  [label="Valid Investments",     fillcolor="#fdebd0"];
            stg_inv_funded [label="Funded Deals\\n(≥$150K)", fillcolor="#fdebd0"];
        }

        subgraph cluster_core {
            label="Stage 3 · VC Focus"; style=filled; color="#eafaf1";
            core_vc    [label="VC Investments",   fillcolor="#d5f5e3"];
            core_pairs [label="Co-investor Pairs", fillcolor="#d5f5e3"];
        }

        subgraph cluster_graph {
            label="Stage 4 · Network"; style=filled; color="#f4ecf7";
            graph_network [label="Network Nodes", fillcolor="#e8daef"];
            graph_edges   [label="Network Edges", fillcolor="#e8daef"];
        }

        subgraph cluster_experiment {
            label="Stage 5 · Analysis"; style=filled; color="#fdedec";
            exp_nestedness [label="Nestedness\\nScores", fillcolor="#fadbd8"];
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
    counts[table] = 0 if row.empty else int(row["cnt"].iloc[0])

n_communities = 0
comm_row = query_df("SELECT COUNT(DISTINCT community_id) AS n FROM graph.network")
if not comm_row.empty:
    n_communities = int(comm_row["n"].iloc[0])

col1, col2, col3, col4 = st.columns(4)
col1.metric("🏢 Companies", format_number(counts["raw.companies"]))
if counts["raw.companies"] > 0:
    col2.metric(
        "🏢 Verified Companies",
        format_number(counts["staging.companies_clean"]),
        delta=f"{counts['staging.companies_clean'] / counts['raw.companies'] * 100:.0f}% retained",
    )
else:
    col2.metric("🏢 Verified Companies", "0")
if counts["raw.investments"] > 0:
    col3.metric(
        "💰 Funded Deals",
        format_number(counts["staging.investments_funded"]),
        delta=f"{counts['staging.investments_funded'] / counts['raw.investments'] * 100:.0f}% of raw",
    )
else:
    col3.metric("💰 Funded Deals", "0")
if counts["staging.investments_funded"] > 0:
    col4.metric(
        "🤝 VC Investments",
        format_number(counts["core.vc_investments"]),
        delta=f"{counts['core.vc_investments'] / counts['staging.investments_funded'] * 100:.0f}% of funded",
    )
else:
    col4.metric("🤝 VC Investments", "0")

col5, col6, col7, col8 = st.columns(4)
col5.metric("🔗 Co-investor Pairs", format_number(counts["core.investment_pairs"]))
col6.metric("🌐 Network Nodes", format_number(counts["graph.network"]))
col7.metric("↔ Edges", format_number(counts["graph.edges"]))
col8.metric("🏘 Communities", n_communities)

# Nestedness Headline
st.subheader("Johnson Nestedness - Top Communities")

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

if not nestedness_df.empty:
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
else:
    st.info("No nestedness data yet. Run the experiment pipeline first.")

# Experiment Comparison
st.subheader("Experiment Comparison")

all_experiments = discover_experiments()

if len(all_experiments) < 2:
    st.info(
        "Only one experiment found. Run additional pipeline configurations "
        "to enable cross-experiment comparison."
    )
else:
    _COUNTS_SQL = """
        SELECT
            (SELECT COUNT(*) FROM graph.network)                     AS nodes,
            (SELECT COUNT(*) FROM graph.edges)                       AS edges,
            (SELECT COUNT(DISTINCT community_id) FROM graph.network) AS communities
    """
    _NEST_SQL = """
        SELECT community, MIN(g_norm) AS g_norm
        FROM experiment.johnson_nestedness
        GROUP BY community
        ORDER BY community
    """

    comparison_rows = []
    for exp_name in all_experiments:
        counts_df = query_df_by_experiment(_COUNTS_SQL, exp_name)
        nest_df = query_df_by_experiment(_NEST_SQL, exp_name)

        row: dict = {"Experiment": exp_name}

        if not counts_df.empty:
            row["Nodes"] = int(counts_df["nodes"].iloc[0])
            row["Edges"] = int(counts_df["edges"].iloc[0])
            row["Communities"] = int(counts_df["communities"].iloc[0])
        else:
            row["Nodes"] = row["Edges"] = row["Communities"] = None

        if not nest_df.empty:
            total_g, count_g = 0.0, 0
            for _, nrow in nest_df.iterrows():
                col_name = f"g_norm ({nrow['community']})"
                row[col_name] = round(float(nrow["g_norm"]), 4)
                total_g += float(nrow["g_norm"])
                count_g += 1
            row["Avg g_norm"] = round(total_g / count_g, 4) if count_g else None
        else:
            row["Avg g_norm"] = None

        comparison_rows.append(row)

    comparison_df = pd.DataFrame(comparison_rows)

    def _highlight_selected(r: pd.Series) -> list[str]:
        color = "background-color: #fff3cd" if r["Experiment"] == selected else ""
        return [color] * len(r)

    st.dataframe(
        comparison_df.style.apply(_highlight_selected, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    g_norm_cols = [c for c in comparison_df.columns if c.startswith("g_norm (")]
    if g_norm_cols:
        fig = go.Figure()
        x_labels = [c.replace("g_norm (", "").rstrip(")") for c in g_norm_cols]
        for i, exp_row in comparison_df.iterrows():
            exp_name = exp_row["Experiment"]
            is_selected = exp_name == selected
            fig.add_trace(
                go.Bar(
                    name=exp_name + (" ★" if is_selected else ""),
                    x=x_labels,
                    y=[exp_row.get(c) for c in g_norm_cols],
                    marker_color=SET_0_COLOR if i % 2 == 0 else SET_1_COLOR,
                    opacity=1.0 if is_selected else 0.6,
                    marker_line_width=3 if is_selected else 0,
                    marker_line_color="#333333" if is_selected else None,
                )
            )
        fig.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="#888888",
            annotation_text="Random (g=1)",
            annotation_position="top left",
        )
        fig.update_layout(
            barmode="group",
            xaxis_title="Community",
            yaxis_title="g_norm (Normalized Nestedness)",
            height=420,
            legend_title="Experiment",
            title="Nestedness g_norm per Community — All Experiments",
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.markdown(
    "_Navigate using the sidebar to explore pipeline stages, data, communities, and nestedness analysis._"
)
