"""Data Explorer - general exploration of the VC investment dataset."""

import streamlit as st
import plotly.express as px
import pandas as pd

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, format_number, region_selector

st.set_page_config(page_title="Data Explorer", layout="wide")

selected = region_selector()
if not selected:
    st.stop()

st.title("Data Explorer")
st.markdown("Explore the VC investment dataset - geography, sectors, funding, and temporal patterns.")

# =============================================================================
# Sidebar Filters
# =============================================================================
st.sidebar.header("Filters")

# Year range
year_range_data = query_df(
    "SELECT MIN(announced_year)::INT AS min_y, MAX(announced_year)::INT AS max_y "
    "FROM core.vc_investments WHERE announced_year IS NOT NULL"
)
min_year = int(year_range_data["min_y"].iloc[0])
max_year = int(year_range_data["max_y"].iloc[0])
year_range = st.sidebar.slider("Year Range", min_year, max_year, (min_year, max_year))

# Country filter
countries = query_df(
    "SELECT DISTINCT company_country FROM core.vc_investments "
    "WHERE company_country IS NOT NULL ORDER BY company_country"
)["company_country"].tolist()
selected_countries = st.sidebar.multiselect("Company Countries", countries, default=[])

# Investment type filter
inv_types = query_df(
    "SELECT DISTINCT investment_type FROM core.vc_investments ORDER BY investment_type"
)["investment_type"].tolist()
selected_types = st.sidebar.multiselect("Investment Types", inv_types, default=[])
top_n_regions = st.sidebar.slider("Top N Investor Regions", 5, 30, 10)

# Build WHERE clause
where_parts = ["announced_year BETWEEN {min_y} AND {max_y}"]
params = {"min_y": year_range[0], "max_y": year_range[1]}

if selected_countries:
    country_list = ", ".join(f"'{c}'" for c in selected_countries)
    where_parts.append(f"company_country IN ({country_list})")
if selected_types:
    type_list = ", ".join(f"'{t}'" for t in selected_types)
    where_parts.append(f"investment_type IN ({type_list})")

where_clause = " AND ".join(where_parts).format(**params)

# =============================================================================
# 1. Geographic Distribution
# =============================================================================
st.subheader("Geographic Distribution")

geo_tab = st.radio("View by", ["Company Country", "Investor Country"], horizontal=True)

if geo_tab == "Company Country":
    geo_df = query_df(
        f"""
        SELECT company_country AS country, COUNT(*) AS investments
        FROM core.vc_investments
        WHERE {where_clause} AND company_country IS NOT NULL
        GROUP BY company_country
        ORDER BY investments DESC
        """
    )
else:
    geo_df = query_df(
        f"""
        SELECT investor_country AS country, COUNT(*) AS investments
        FROM core.vc_investments
        WHERE {where_clause} AND investor_country IS NOT NULL
        GROUP BY investor_country
        ORDER BY investments DESC
        """
    )

col_map, col_table = st.columns([2, 1])

with col_map:
    fig_map = px.choropleth(
        geo_df,
        locations="country",
        locationmode="country names",
        color="investments",
        color_continuous_scale="Blues",
        hover_data=["investments"],
    )
    fig_map.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig_map, use_container_width=True)

with col_table:
    st.dataframe(geo_df.head(20), use_container_width=True, hide_index=True, height=400)

# Investor Regions bar chart
investor_regions = query_df(
    f"""
    SELECT investor_region AS region, COUNT(*) AS investments
    FROM core.vc_investments
    WHERE {where_clause} AND investor_region IS NOT NULL
    GROUP BY investor_region
    ORDER BY investments DESC
    LIMIT {top_n_regions}
    """
)

if not investor_regions.empty:
    fig_regions = px.bar(
        investor_regions,
        y="region",
        x="investments",
        orientation="h",
        color="investments",
        color_continuous_scale="Blues",
        labels={"investments": "Investment Count", "region": "Investor Region"},
    )
    fig_regions.update_layout(
        yaxis=dict(autorange="reversed"),
        height=max(300, len(investor_regions) * 28),
        xaxis_title="Investment Count",
        yaxis_title="",
        showlegend=False,
        title="Investor Regions",
    )
    st.plotly_chart(fig_regions, use_container_width=True)

# =============================================================================
# 2. Top Investors
# =============================================================================
st.subheader("Top Investors")

top_investors = query_df(
    f"""
    SELECT
        investor_name,
        COUNT(*) AS investments,
        COUNT(DISTINCT org_uuid) AS companies,
        COUNT(DISTINCT investment_type) AS stage_types,
        SUM(total_funding_usd) AS total_funding
    FROM core.vc_investments
    WHERE {where_clause}
    GROUP BY investor_name
    ORDER BY investments DESC
    LIMIT 50
    """
)
top_investors["total_funding"] = top_investors["total_funding"].apply(
    lambda x: f"${x / 1e9:.2f}B" if x >= 1e9 else f"${x / 1e6:.1f}M" if x >= 1e6 else f"${x:,.0f}"
)

st.dataframe(top_investors, use_container_width=True, hide_index=True, height=400)

# =============================================================================
# 3. Sector / Category Analysis
# =============================================================================
st.subheader("Top Sectors / Categories")

categories = query_df(
    f"""
    SELECT category, COUNT(*) AS investments
    FROM core.vc_investments
    WHERE {where_clause} AND category IS NOT NULL
    GROUP BY category
    ORDER BY investments DESC
    LIMIT 25
    """
)

fig_cat = px.bar(
    categories,
    y="category",
    x="investments",
    orientation="h",
    color="investments",
    color_continuous_scale="Greens",
)
fig_cat.update_layout(
    yaxis=dict(autorange="reversed"),
    height=500,
    xaxis_title="Investment Count",
    yaxis_title="Category",
    showlegend=False,
)
st.plotly_chart(fig_cat, use_container_width=True)

# =============================================================================
# 4. Funding Distribution
# =============================================================================
st.subheader("Funding Distribution")

funding_df = query_df(
    f"""
    SELECT total_funding_usd, investment_type
    FROM core.vc_investments
    WHERE {where_clause} AND total_funding_usd > 0
    """
)

import numpy as np

funding_df["log_funding"] = np.log10(funding_df["total_funding_usd"])

fig_funding = px.histogram(
    funding_df,
    x="log_funding",
    color="investment_type",
    nbins=60,
    log_y=True,
    barmode="overlay",
    opacity=0.6,
)
fig_funding.update_layout(
    xaxis_title="Total Funding USD (log10)",
    yaxis_title="Count (log scale)",
    height=400,
)
st.plotly_chart(fig_funding, use_container_width=True)

# =============================================================================
# 5. Temporal Trends
# =============================================================================
st.subheader("Investment Trends Over Time")

# Group investment types into early/late
temporal = query_df(
    f"""
    SELECT
        announced_year::INT AS year,
        CASE
            WHEN investment_type IN ('angel', 'pre_seed', 'seed', 'series_a') THEN 'Early-stage'
            ELSE 'Late-stage'
        END AS stage_group,
        COUNT(*) AS investments
    FROM core.vc_investments
    WHERE {where_clause} AND announced_year IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1
    """
)

fig_temporal = px.line(
    temporal,
    x="year",
    y="investments",
    color="stage_group",
    color_discrete_map={"Early-stage": "#e74c3c", "Late-stage": "#3498db"},
    markers=True,
)
fig_temporal.update_layout(
    xaxis_title="Year",
    yaxis_title="Investment Count",
    height=400,
)
st.plotly_chart(fig_temporal, use_container_width=True)

# =============================================================================
# 6. Co-Investment Statistics
# =============================================================================
st.subheader("Co-Investment (Syndication) Statistics")

pairs_by_year = query_df(
    """
    SELECT year::INT AS year, COUNT(*) AS pairs
    FROM core.investment_pairs
    GROUP BY year
    ORDER BY year
    """
)

fig_pairs_yr = px.bar(
    pairs_by_year,
    x="year",
    y="pairs",
    color_discrete_sequence=["#9b59b6"],
)
fig_pairs_yr.update_layout(
    xaxis_title="Year",
    yaxis_title="Number of Syndication Pairs",
    height=350,
)
st.plotly_chart(fig_pairs_yr, use_container_width=True)

# Top companies by syndication pairs
top_syndicated = query_df(
    """
    SELECT
        ip.org_uuid,
        vc.company_name,
        COUNT(*) AS pairs
    FROM core.investment_pairs ip
    JOIN (SELECT DISTINCT org_uuid, company_name FROM core.vc_investments) vc
        ON ip.org_uuid = vc.org_uuid
    GROUP BY ip.org_uuid, vc.company_name
    ORDER BY pairs DESC
    LIMIT 100
    """
)

st.markdown("**Top 100 Companies by Syndication Pairs**")
st.dataframe(top_syndicated, use_container_width=True, hide_index=True)
