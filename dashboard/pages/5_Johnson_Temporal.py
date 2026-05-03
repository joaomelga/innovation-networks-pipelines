"""Johnson Nestedness temporal evolution — per community."""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import query_df, region_selector, clustering_method_selector, SET_COLORS

st.set_page_config(page_title="Johnson Temporal", layout="wide")

selected = region_selector()
if not selected:
    st.stop()
method = clustering_method_selector()

st.title("Johnson Nestedness — Temporal Evolution")
st.markdown(
    "Johnson et al. (2013) **g_norm** computed year-by-year on a community's "
    "bipartite subgraph. **Cumulative** uses all edges up to year Y; "
    "**rolling 5y** uses edges in [Y-4, Y]. "
    "g_norm > 1 = more nested than the analytical configuration-model expectation; "
    "≈ 1 = random; < 1 = anti-nested."
)

SIDE_COLORS = {"Early-stage": "#e74c3c", "Late-stage": "#3498db"}


def _community_sort_key(name: str) -> int:
    try:
        return int(name.rsplit(" ", 1)[-1])
    except ValueError:
        return 0


@st.cache_data(ttl=600)
def _load_temporal(method: str, table: str) -> pd.DataFrame:
    return query_df(
        f"SELECT * FROM {table} WHERE clustering_method = '{method}'"
    )


def _render_tab(table: str, key_prefix: str, variant_label: str, has_weight: bool) -> None:
    df = _load_temporal(method, table)
    if df.empty:
        st.info(
            f"`{table}` has no rows for method `{method}`. "
            "Run the corresponding experiment first."
        )
        return

    communities = sorted(df["community"].unique(), key=_community_sort_key)
    community = st.selectbox(
        "Community",
        communities,
        key=f"{key_prefix}_comm",
    )

    window_types = sorted(df["window_type"].unique().tolist())
    default_window = "cumulative" if "cumulative" in window_types else window_types[0]
    window_choice = st.radio(
        "Window type",
        window_types,
        index=window_types.index(default_window),
        horizontal=True,
        key=f"{key_prefix}_window",
    )

    view = df[(df["community"] == community) & (df["window_type"] == window_choice)].copy()
    if view.empty:
        st.info("No data for this combination.")
        return

    min_year, max_year = int(view["year"].min()), int(view["year"].max())
    year_range = st.slider(
        "Year range", min_year, max_year, (min_year, max_year), key=f"{key_prefix}_years"
    )
    view = view[view["year"].between(*year_range)].copy()

    # Latest-year metrics
    latest = view.sort_values("year").iloc[-1]
    g = latest["g_norm"]
    interp = (
        "Highly nested" if g > 1.5
        else "Nested" if g > 1.05
        else "Anti-nested" if g < 0.95
        else "Random"
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("g_norm", f"{g:.4f}", delta=interp,
                delta_color="normal" if g > 1.05 else ("inverse" if g < 0.95 else "off"))
    col2.metric("g_raw", f"{latest['g_raw']:.6f}")
    col3.metric("g_conf", f"{latest['g_conf']:.6f}")
    col4.metric("Year / Edges", f"{int(latest['year'])} / {int(latest['n_edges'])}")

    # g_norm over time
    st.subheader(f"g_norm Over Time — {window_choice} ({variant_label})")
    hover = {"g_raw": ":.5f", "g_conf": ":.5f", "n_edges": True}
    if has_weight and "total_weight" in view.columns:
        hover["total_weight"] = True
    fig = px.line(
        view, x="year", y="g_norm",
        markers=True, hover_data=hover,
        labels={"g_norm": "g_norm", "year": "Year"},
    )
    fig.add_hline(y=1.0, line_dash="dash", line_color="grey",
                  annotation_text="Random (g=1)", annotation_position="top left")
    fig.update_layout(height=420, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_gnorm")

    # Decomposition g_raw vs g_conf
    st.subheader("Decomposition: g_raw vs g_conf")
    dec_fig = make_subplots(rows=1, cols=2, subplot_titles=("g_raw (observed)", "g_conf (null)"),
                            horizontal_spacing=0.12)
    dec_fig.add_trace(go.Scatter(x=view["year"], y=view["g_raw"], mode="lines+markers",
                                 name="g_raw", line=dict(color="#2ecc71")), row=1, col=1)
    dec_fig.add_trace(go.Scatter(x=view["year"], y=view["g_conf"], mode="lines+markers",
                                 name="g_conf", line=dict(color="#e67e22")), row=1, col=2)
    dec_fig.update_layout(height=380, hovermode="x unified")
    dec_fig.update_yaxes(title_text="g_raw", row=1, col=1)
    dec_fig.update_yaxes(title_text="g_conf", row=1, col=2)
    st.plotly_chart(dec_fig, use_container_width=True, key=f"{key_prefix}_dec")

    # Per-side
    if "g_norm_left" in view.columns and "g_norm_right" in view.columns:
        st.subheader("Per-Side Decomposition")
        st.markdown(
            "**g_norm_left** = nestedness of the early (left) side; "
            "**g_norm_right** = late (right) side. Each is centered at 1 under the null."
        )
        melted = view.melt(id_vars=["year"], value_vars=["g_norm_left", "g_norm_right"],
                           var_name="side", value_name="value")
        melted["side"] = melted["side"].map(
            {"g_norm_left": "Early-stage", "g_norm_right": "Late-stage"}
        )
        fig_side = px.line(
            melted, x="year", y="value", color="side",
            color_discrete_map=SIDE_COLORS, markers=True,
            labels={"value": "Mean local g_norm", "year": "Year", "side": "Side"},
        )
        fig_side.add_hline(y=1.0, line_dash="dash", line_color="grey",
                           annotation_text="Random (g=1)", annotation_position="top left")
        fig_side.update_layout(height=380, hovermode="x unified")
        st.plotly_chart(fig_side, use_container_width=True, key=f"{key_prefix}_side")

    # Network size
    st.subheader("Network Size Over Time")
    ncols = 3 if has_weight and "total_weight" in view.columns else 2
    titles = ["Nodes (left × right)", "Edges"] + (["Total weight"] if ncols == 3 else [])
    size_fig = make_subplots(rows=1, cols=ncols, subplot_titles=titles, horizontal_spacing=0.09)
    size_fig.add_trace(go.Scatter(x=view["year"], y=view["n_left"], mode="lines+markers",
                                   name="Left (early)", line=dict(color="#e74c3c")), row=1, col=1)
    size_fig.add_trace(go.Scatter(x=view["year"], y=view["n_right"], mode="lines+markers",
                                   name="Right (late)", line=dict(color="#3498db")), row=1, col=1)
    size_fig.add_trace(go.Scatter(x=view["year"], y=view["n_edges"], mode="lines+markers",
                                   name="Edges", showlegend=False, line=dict(color="#8e44ad")), row=1, col=2)
    if ncols == 3:
        size_fig.add_trace(go.Scatter(x=view["year"], y=view["total_weight"], mode="lines+markers",
                                       name="Weight", showlegend=False, line=dict(color="#f39c12")), row=1, col=3)
    size_fig.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(size_fig, use_container_width=True, key=f"{key_prefix}_size")

    st.subheader("Raw Data")
    st.dataframe(view.sort_values("year").reset_index(drop=True), use_container_width=True, height=300)


tab_binary, tab_weighted = st.tabs(["Binary (degree)", "Weighted (strength)"])

with tab_binary:
    st.markdown(
        "_Original Johnson formula on the binary biadjacency — edge present/absent "
        "regardless of how many companies the two investors shared._"
    )
    _render_tab(
        table="experiment.johnson_nestedness_temporal",
        key_prefix="bin",
        variant_label="binary",
        has_weight=False,
    )

with tab_weighted:
    st.markdown(
        "_Weighted Johnson: edge weight = # of distinct portfolio companies per "
        "(early, late) investor pair. k → s (strength) generalisation._"
    )
    _render_tab(
        table="experiment.johnson_nestedness_weighted_temporal",
        key_prefix="wtd",
        variant_label="weighted",
        has_weight=True,
    )
