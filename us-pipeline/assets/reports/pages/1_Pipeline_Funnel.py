"""Pipeline Funnel - data characterization at each pipeline stage."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, format_number

st.set_page_config(page_title="Pipeline Funnel 1", layout="wide")
st.title("Pipeline Funnel")
st.markdown(
    "Understand what happens at each pipeline stage - how many records are retained "
    "and what gets filtered out at each step."
)

# =============================================================================
# 1. Overall Funnel
# =============================================================================
st.subheader("Record Counts Through Pipeline Stages")

view = st.radio("View", ["Companies", "Investments", "Both"], horizontal=True)

# Companies funnel
company_stages = {
    "Companies in raw.companies": query_df("SELECT COUNT(*) AS n FROM raw.companies")["n"].iloc[0],
    "Companies in staging.companies_clean": query_df("SELECT COUNT(*) AS n FROM staging.companies_clean")["n"].iloc[0],
    "Companies in staging.investments_funded": query_df(
        "SELECT COUNT(DISTINCT org_uuid) AS n FROM staging.investments_funded"
    )["n"].iloc[0],
    "Companies in core.vc_investments": query_df(
        "SELECT COUNT(DISTINCT org_uuid) AS n FROM core.vc_investments"
    )["n"].iloc[0],
    "Companies in core.investment_pairs": query_df(
        "SELECT COUNT(DISTINCT org_uuid) AS n FROM core.investment_pairs"
    )["n"].iloc[0],
}

# Investments funnel
investment_stages = {
    "raw.investments": query_df("SELECT COUNT(*) AS n FROM raw.investments")["n"].iloc[0],
    "staging.investments_clean": query_df("SELECT COUNT(*) AS n FROM staging.investments_clean")["n"].iloc[0],
    "staging.investments_funded": query_df("SELECT COUNT(*) AS n FROM staging.investments_funded")["n"].iloc[0],
    "core.vc_investments": query_df("SELECT COUNT(*) AS n FROM core.vc_investments")["n"].iloc[0],
    "core.investment_pairs": query_df("SELECT COUNT(*) AS n FROM core.investment_pairs")["n"].iloc[0],
}


def make_funnel(stages: dict, title: str):
    df = pd.DataFrame(
        [{"Stage": k, "Count": int(v)} for k, v in stages.items()]
    )
    first = df["Count"].iloc[0]
    df["% of Initial"] = (df["Count"] / first * 100).round(1)
    df["text"] = df.apply(lambda r: f"{format_number(r['Count'])} ({r['% of Initial']:.0f}%)", axis=1)

    fig = px.bar(
        df,
        y="Stage",
        x="Count",
        orientation="h",
        text="text",
        color="% of Initial",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        title=title,
        yaxis=dict(autorange="reversed"),
        height=350,
        showlegend=False,
    )
    fig.update_traces(textposition="outside")
    return fig


if view in ("Companies", "Both"):
    st.plotly_chart(make_funnel(company_stages, "Companies Funnel"), use_container_width=True)
if view in ("Investments", "Both"):
    st.plotly_chart(make_funnel(investment_stages, "Investments Funnel"), use_container_width=True)

# =============================================================================
# 2. Company Filtering Breakdown
# =============================================================================
st.subheader("Company Filtering Breakdown")
st.markdown("Why do companies get excluded from `raw.companies` to `staging.companies_clean`?")

filter_df = query_df(
    """
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN uuid IS NULL OR name IS NULL OR founded_year IS NULL THEN 1 ELSE 0 END) AS missing_fields,
        SUM(CASE WHEN founded_year > 2017 THEN 1 ELSE 0 END) AS founded_after_2017,
        SUM(CASE WHEN status IN ('closed', 'acquired', 'ipo') THEN 1 ELSE 0 END) AS exited,
        SUM(CASE
            WHEN uuid IS NOT NULL AND name IS NOT NULL AND founded_year IS NOT NULL
                 AND founded_year <= 2017
                 AND (status IS NULL OR status NOT IN ('closed', 'acquired', 'ipo'))
            THEN 1 ELSE 0 END
        ) AS passed
    FROM raw.companies
    """
)

breakdown = pd.DataFrame(
    [
        {"Reason": "Missing fields (uuid/name/founded_year)", "Count": int(filter_df["missing_fields"].iloc[0])},
        {"Reason": "Founded after 2017", "Count": int(filter_df["founded_after_2017"].iloc[0])},
        {"Reason": "Exited (closed/acquired/IPO)", "Count": int(filter_df["exited"].iloc[0])},
        {"Reason": "Passed all filters", "Count": int(filter_df["passed"].iloc[0])},
    ]
)

col1, col2 = st.columns([1, 1])
with col1:
    fig_pie = px.pie(
        breakdown,
        values="Count",
        names="Reason",
        color="Reason",
        color_discrete_map={
            "Passed all filters": "#2ecc71",
            "Founded after 2017": "#f39c12",
            "Exited (closed/acquired/IPO)": "#e74c3c",
            "Missing fields (uuid/name/founded_year)": "#95a5a6",
        },
    )
    fig_pie.update_layout(height=400)
    st.plotly_chart(fig_pie, use_container_width=True)
with col2:
    st.dataframe(breakdown, use_container_width=True, hide_index=True)
    st.caption(
        "Note: A company can match multiple exclusion reasons. "
        "The counts above show how many match each criterion individually."
    )

# =============================================================================
# 3. Investment Type Distribution Before vs After VC Filter
# =============================================================================
st.subheader("Investment Types: Before vs After VC Filter")

type_before = query_df(
    """
    SELECT investment_type, COUNT(*) AS count_funded
    FROM staging.investments_funded
    GROUP BY investment_type
    ORDER BY count_funded DESC
    """
)
type_after = query_df(
    """
    SELECT investment_type, COUNT(*) AS count_vc
    FROM core.vc_investments
    GROUP BY investment_type
    ORDER BY count_vc DESC
    """
)

type_compare = type_before.merge(type_after, on="investment_type", how="outer").fillna(0)
type_compare = type_compare.sort_values("count_funded", ascending=False)

type_melted = pd.melt(
    type_compare,
    id_vars=["investment_type"],
    value_vars=["count_funded", "count_vc"],
    var_name="Stage",
    value_name="Count",
)
type_melted["Stage"] = type_melted["Stage"].map(
    {"count_funded": "investments_funded", "count_vc": "vc_investments"}
)

fig_types = px.bar(
    type_melted,
    x="investment_type",
    y="Count",
    color="Stage",
    barmode="group",
    color_discrete_map={"investments_funded": "#3498db", "vc_investments": "#2ecc71"},
)
fig_types.update_layout(
    xaxis_title="Investment Type",
    yaxis_title="Count",
    height=400,
    xaxis_tickangle=-45,
)
st.plotly_chart(fig_types, use_container_width=True)

# =============================================================================
# 4. Temporal Coverage
# =============================================================================
st.subheader("Temporal Coverage: Investments per Year")

temporal_raw = query_df(
    """
    SELECT announced_year AS year, COUNT(*) AS count
    FROM raw.investments
    WHERE announced_year IS NOT NULL AND announced_year >= 1990
    GROUP BY announced_year ORDER BY announced_year
    """
)
temporal_funded = query_df(
    """
    SELECT announced_year AS year, COUNT(*) AS count
    FROM staging.investments_funded
    WHERE announced_year IS NOT NULL AND announced_year >= 1990
    GROUP BY announced_year ORDER BY announced_year
    """
)
temporal_vc = query_df(
    """
    SELECT announced_year AS year, COUNT(*) AS count
    FROM core.vc_investments
    WHERE announced_year IS NOT NULL AND announced_year >= 1990
    GROUP BY announced_year ORDER BY announced_year
    """
)

temporal_raw["Stage"] = "raw.investments"
temporal_funded["Stage"] = "investments_funded"
temporal_vc["Stage"] = "vc_investments"

temporal_all = pd.concat([temporal_raw, temporal_funded, temporal_vc])

fig_temporal = px.line(
    temporal_all,
    x="year",
    y="count",
    color="Stage",
    color_discrete_map={
        "raw.investments": "#95a5a6",
        "investments_funded": "#3498db",
        "vc_investments": "#2ecc71",
    },
    markers=True,
)
fig_temporal.update_layout(
    xaxis_title="Year",
    yaxis_title="Investment Count",
    height=400,
)
st.plotly_chart(fig_temporal, use_container_width=True)

# =============================================================================
# 5. Pairs Generation Summary
# =============================================================================
st.subheader("Investment Pairs Summary")

pairs_stats = query_df(
    """
    SELECT
        COUNT(*) AS total_pairs,
        COUNT(DISTINCT org_uuid) AS companies_with_pairs,
        COUNT(DISTINCT investor_name_left) AS unique_late_investors,
        COUNT(DISTINCT investor_name_right) AS unique_early_investors
    FROM core.investment_pairs
    """
)

pc1, pc2, pc3, pc4 = st.columns(4)
pc1.metric("Total Pairs", format_number(int(pairs_stats["total_pairs"].iloc[0])))
pc2.metric("Companies with Pairs", format_number(int(pairs_stats["companies_with_pairs"].iloc[0])))
pc3.metric("Unique Late-stage Investors", format_number(int(pairs_stats["unique_late_investors"].iloc[0])))
pc4.metric("Unique Early-stage Investors", format_number(int(pairs_stats["unique_early_investors"].iloc[0])))

# Pairs per company distribution
pairs_per_co = query_df(
    """
    SELECT org_uuid, COUNT(*) AS pairs
    FROM core.investment_pairs
    GROUP BY org_uuid
    """
)

fig_pairs_dist = px.histogram(
    pairs_per_co,
    x="pairs",
    nbins=50,
    log_y=True,
    color_discrete_sequence=["#3498db"],
)
fig_pairs_dist.update_layout(
    xaxis_title="Number of Investment Pairs per Company",
    yaxis_title="Count (log scale)",
    height=350,
)
st.plotly_chart(fig_pairs_dist, use_container_width=True)
