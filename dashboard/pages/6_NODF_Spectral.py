"""NODF and spectral radius temporal evolution — per community."""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, region_selector, clustering_method_selector

st.set_page_config(page_title="NODF & Spectral Temporal", layout="wide")

selected = region_selector()
if not selected:
    st.stop()
method = clustering_method_selector()

st.title("NODF & Spectral Radius — Temporal Evolution")
st.markdown(
    "Three nestedness metrics over time per community: **binary NODF** "
    "(Almeida-Neto 2008), **weighted NODF** (Almeida-Neto & Ulrich 2011), "
    "and **spectral radius** (Staniczenko 2013, the cleaner weighted alternative)."
)

SIDE_COLORS = {"Early-stage (rows)": "#e74c3c", "Late-stage (cols)": "#3498db"}


def _community_sort_key(name: str) -> int:
    try:
        return int(name.rsplit(" ", 1)[-1])
    except ValueError:
        return 0


@st.cache_data(ttl=600)
def _load(method: str, table: str) -> pd.DataFrame:
    return query_df(f"SELECT * FROM {table} WHERE clustering_method = '{method}'")


def _controls(df: pd.DataFrame, key_prefix: str) -> tuple[str, str, tuple[int, int]]:
    communities = sorted(df["community"].unique(), key=_community_sort_key)
    community = st.selectbox("Community", communities, key=f"{key_prefix}_comm")
    window_types = sorted(df["window_type"].unique().tolist())
    default = "cumulative" if "cumulative" in window_types else window_types[0]
    window = st.radio("Window type", window_types, index=window_types.index(default),
                      horizontal=True, key=f"{key_prefix}_window")
    view = df[(df["community"] == community) & (df["window_type"] == window)]
    if view.empty:
        return community, window, (0, 0)
    min_y, max_y = int(view["year"].min()), int(view["year"].max())
    year_range = st.slider("Year range", min_y, max_y, (min_y, max_y), key=f"{key_prefix}_years")
    return community, window, year_range


# =============================================================================
# NODF tab
# =============================================================================
def render_nodf_tab() -> None:
    df = _load(method, "experiment.nodf_temporal")
    if df.empty:
        st.info("`experiment.nodf_temporal` has no rows. Run the experiment first.")
        return

    community, window_choice, year_range = _controls(df, "nodf")
    view = df[(df["community"] == community) & (df["window_type"] == window_choice)
              & (df["year"].between(*year_range))].copy()
    if view.empty:
        st.info("No data for this selection.")
        return

    latest = view.sort_values("year").iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("NODF", f"{latest['nodf']:.2f}")
    c2.metric("NODF rows", f"{latest['nodf_rows']:.2f}")
    c3.metric("NODF cols", f"{latest['nodf_cols']:.2f}")
    c4.metric("Fill", f"{latest['fill']:.4f}")

    st.subheader(f"NODF Over Time — {window_choice}")
    fig = px.line(view, x="year", y="nodf", markers=True,
                  hover_data={"nodf_rows": ":.2f", "nodf_cols": ":.2f", "n_edges": True, "fill": ":.4f"},
                  labels={"nodf": "NODF (0–100)", "year": "Year"})
    fig.update_layout(height=420, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True, key="nodf_main")

    st.subheader("Per-Side Decomposition")
    melted = view.melt(id_vars=["year"], value_vars=["nodf_rows", "nodf_cols"],
                       var_name="side", value_name="value")
    melted["side"] = melted["side"].map(
        {"nodf_rows": "Early-stage (rows)", "nodf_cols": "Late-stage (cols)"}
    )
    fig_side = px.line(melted, x="year", y="value", color="side",
                       color_discrete_map=SIDE_COLORS, markers=True,
                       labels={"value": "NODF per side", "year": "Year", "side": "Side"})
    fig_side.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(fig_side, use_container_width=True, key="nodf_side")

    st.subheader("Raw Data")
    st.dataframe(view.sort_values("year").reset_index(drop=True), use_container_width=True, height=280)


# =============================================================================
# WNODF tab
# =============================================================================
def render_wnodf_tab() -> None:
    df = _load(method, "experiment.wnodf_temporal")
    if df.empty:
        st.info("`experiment.wnodf_temporal` has no rows. Run the experiment first.")
        return

    community, window_choice, year_range = _controls(df, "wnodf")
    view = df[(df["community"] == community) & (df["window_type"] == window_choice)
              & (df["year"].between(*year_range))].copy()
    if view.empty:
        st.info("No data for this selection.")
        return

    latest = view.sort_values("year").iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("WNODF", f"{latest['wnodf']:.2f}")
    c2.metric("WNODF rows", f"{latest['wnodf_rows']:.2f}")
    c3.metric("WNODF cols", f"{latest['wnodf_cols']:.2f}")
    c4.metric("Total weight", f"{int(latest['total_weight'])}")

    st.subheader(f"WNODF Over Time — {window_choice}")
    fig = px.line(view, x="year", y="wnodf", markers=True,
                  hover_data={"wnodf_rows": ":.2f", "wnodf_cols": ":.2f", "n_edges": True, "total_weight": True},
                  labels={"wnodf": "WNODF (0–100)", "year": "Year"})
    fig.update_layout(height=420, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True, key="wnodf_main")

    st.subheader("Per-Side Decomposition")
    melted = view.melt(id_vars=["year"], value_vars=["wnodf_rows", "wnodf_cols"],
                       var_name="side", value_name="value")
    melted["side"] = melted["side"].map(
        {"wnodf_rows": "Early-stage (rows)", "wnodf_cols": "Late-stage (cols)"}
    )
    fig_side = px.line(melted, x="year", y="value", color="side",
                       color_discrete_map=SIDE_COLORS, markers=True,
                       labels={"value": "WNODF per side", "year": "Year", "side": "Side"})
    fig_side.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(fig_side, use_container_width=True, key="wnodf_side")

    st.subheader("Raw Data")
    st.dataframe(view.sort_values("year").reset_index(drop=True), use_container_width=True, height=280)


# =============================================================================
# Spectral tab
# =============================================================================
def render_spectral_tab() -> None:
    df = _load(method, "experiment.spectral_radius_temporal")
    if df.empty:
        st.info("`experiment.spectral_radius_temporal` has no rows. Run the experiment first.")
        return

    community, window_choice, year_range = _controls(df, "spec")
    view = df[(df["community"] == community) & (df["window_type"] == window_choice)
              & (df["year"].between(*year_range))].copy()
    if view.empty:
        st.info("No data for this selection.")
        return

    latest = view.sort_values("year").iloc[-1]
    rw = latest["rho_weighted_norm"]
    interp = (
        "Highly nested" if rw > 1.5
        else "Nested" if rw > 1.05
        else "Anti-nested" if rw < 0.95
        else "Random"
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ρ_weighted_norm", f"{rw:.4f}", delta=interp,
              delta_color="normal" if rw > 1.05 else ("inverse" if rw < 0.95 else "off"))
    c2.metric("ρ_distance_norm", f"{latest.get('rho_distance_norm', float('nan')):.4f}"
              if pd.notna(latest.get("rho_distance_norm")) else "—")
    c3.metric("ρ_distance_late_norm", f"{latest.get('rho_distance_late_norm', float('nan')):.4f}"
              if pd.notna(latest.get("rho_distance_late_norm")) else "—")
    c4.metric("Year / Edges", f"{int(latest['year'])} / {int(latest['n_edges'])}")

    st.subheader(f"Normalised spectral radius over time — {window_choice}")
    value_vars = ["rho_weighted_norm"]
    label_map = {"rho_weighted_norm": "Weighted (s = strength)"}
    if "rho_distance_norm" in view.columns:
        value_vars.append("rho_distance_norm")
        label_map["rho_distance_norm"] = "Distance Early × Early"
    if "rho_distance_late_norm" in view.columns:
        value_vars.append("rho_distance_late_norm")
        label_map["rho_distance_late_norm"] = "Distance Late × Late"

    melted = view.melt(id_vars=["year"], value_vars=value_vars, var_name="variant", value_name="rho_norm")
    melted["variant"] = melted["variant"].map(label_map)
    fig = px.line(melted, x="year", y="rho_norm", color="variant", markers=True,
                  labels={"rho_norm": "ρ_norm", "year": "Year", "variant": "Variant"})
    fig.add_hline(y=1.0, line_dash="dash", line_color="grey",
                  annotation_text="Random (ρ_norm=1)", annotation_position="top left")
    fig.update_layout(height=450, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True, key="spec_main")

    # Participation fractions
    pr_cols = [c for c in view.columns if c.startswith("pr_frac_")]
    if pr_cols:
        st.subheader("Per-side participation fractions")
        pr_map = {
            "pr_frac_early_weighted": "Early weighted",
            "pr_frac_late_weighted": "Late weighted",
            "pr_frac_early_distance": "Early distance",
            "pr_frac_late_distance": "Late distance",
        }
        pr_melted = view.melt(id_vars=["year"], value_vars=[c for c in pr_cols if c in pr_map],
                              var_name="series", value_name="pr_frac")
        pr_melted["series"] = pr_melted["series"].map(pr_map)
        fig_pr = px.line(pr_melted, x="year", y="pr_frac", color="series", markers=True,
                         labels={"pr_frac": "Participation fraction", "year": "Year", "series": "Series"})
        fig_pr.update_yaxes(range=[0, 1])
        fig_pr.update_layout(height=380, hovermode="x unified")
        st.plotly_chart(fig_pr, use_container_width=True, key="spec_pr")

    st.subheader("Raw Data")
    disp_cols = ["year", "window_type", "n_left", "n_right", "n_edges", "total_weight",
                 "rho_weighted_norm", "rho_distance_norm", "rho_distance_late_norm"]
    st.dataframe(view[[c for c in disp_cols if c in view.columns]].sort_values("year"),
                 use_container_width=True, height=280)


tab_nodf, tab_wnodf, tab_spectral = st.tabs(["NODF (binary)", "WNODF (weighted)", "Spectral radius"])

with tab_nodf:
    render_nodf_tab()
with tab_wnodf:
    render_wnodf_tab()
with tab_spectral:
    render_spectral_tab()
