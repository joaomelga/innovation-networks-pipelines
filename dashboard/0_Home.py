"""Home page - Pipeline Overview for VC Syndication Networks."""

import streamlit as st

from db import discover_regions

st.set_page_config(
    page_title="VC Syndication Networks",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)

st.title("VC Syndication Networks Pipeline")
st.markdown(
    "Interactive dashboards for the **Staged Bipartite Venture Networks** analysis — "
    "exploring nestedness and community structure across regions and clustering methods."
)

with st.expander("Getting started — click to expand", expanded=not st.session_state.get("_home_visited")):
    st.session_state["_home_visited"] = True
    st.markdown(
        """
**How to navigate**
- Use the **sidebar** (left) to move between dashboard pages:
  - **Home**: pipeline overview and run instructions (you are here)
  - **Pipeline Funnel**: record counts through each pipeline stage
  - **Data Explorer**: browse investments, sectors, geography, and funding
  - **Community Explorer**: inspect investor communities — select **Region** and **Clustering Method** in the sidebar
  - **Nestedness Analysis**: Johnson nestedness scores — select **Region** and **Clustering Method** in the sidebar

**Selecting a region and method**
Use the **Region** selector in the sidebar on any data page to switch between pipeline outputs.
On *Community Explorer* and *Nestedness Analysis*, a **Clustering Method** selector is also available.
"""
    )

# =============================================================================
# How to Run
# =============================================================================
st.subheader("How to Run the Pipeline")
st.markdown(
    "Set `REGION` to the target geography and run Bruin with the matching `--environment`. "
    "A single pipeline run computes community structure for **all clustering methods** at once."
)

st.markdown("**Pipeline commands**")
st.code(
    """\
# United States
REGION=us bruin run --environment us assets/

# France
REGION=fr bruin run --environment fr assets/

# Europe
REGION=eu bruin run --environment eu assets/""",
    language="bash",
)

st.markdown("**Available environment variables**")
st.dataframe(
    {
        "Variable": ["REGION", "BRUIN_RAW_DIR", "BRUIN_DUCKDB_PATH", "BRUIN_FIGURES_DIR"],
        "Default": ["us", "data/{REGION}/", "outputs/{REGION}/pipeline.duckdb", "outputs/{REGION}/figures/"],
        "Description": [
            "Target region — must match one of: us, fr, eu",
            "Override path to raw CSV data directory",
            "Override path to the output DuckDB file",
            "Override path to the figures output directory",
        ],
    },
    use_container_width=True,
    hide_index=True,
)

# =============================================================================
# Available Regions
# =============================================================================
st.subheader("Available Pipeline Outputs")
regions = discover_regions()
if regions:
    st.success(f"Found **{len(regions)}** region(s) with pipeline output: {', '.join(f'`{r}`' for r in regions)}")
    st.markdown("Use the **Region** selector in the sidebar on any data page to explore results.")
else:
    st.info("No pipeline outputs found yet. Run the pipeline for at least one region to get started.")

# =============================================================================
# Pipeline Architecture
# =============================================================================
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
            label="Stage 4 · Network\\n(all methods)"; style=filled; color="#f4ecf7";
            graph_network [label="Network Nodes\\n(nestlon + modularity)", fillcolor="#e8daef"];
            graph_edges   [label="Network Edges\\n(nestlon + modularity)", fillcolor="#e8daef"];
        }

        subgraph cluster_experiment {
            label="Stage 5 · Analysis\\n(all methods)"; style=filled; color="#fdedec";
            exp_nestedness [label="Nestedness\\nScores\\n(nestlon + modularity)", fillcolor="#fadbd8"];
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

st.markdown("---")
st.markdown(
    "_Navigate using the sidebar to explore pipeline stages, data, communities, and nestedness analysis._"
)
