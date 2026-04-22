import pandas as pd
import networkx as nx


def build_bipartite_graph(
    pairs_df: pd.DataFrame,
    left_col: str = "investor_name_left",
    right_col: str = "investor_name_right",
) -> nx.Graph:
    """
    Build a bipartite NetworkX graph from an edge-pair DataFrame.

    Sets node attribute bipartite=0 for left_col nodes (late-stage),
    bipartite=1 for right_col nodes (early-stage).
    """
    G = nx.Graph()

    left_nodes = set(pairs_df[left_col].dropna().unique())
    right_nodes = set(pairs_df[right_col].dropna().unique())

    G.add_nodes_from(left_nodes, bipartite=0)   # set 0 = late-stage
    G.add_nodes_from(right_nodes, bipartite=1)  # set 1 = early-stage

    for _, row in pairs_df.iterrows():
        G.add_edge(row[left_col], row[right_col])

    set_0 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 0}
    set_1 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 1}

    print(f"Graph: {G.number_of_nodes()} nodes ({len(set_0)} late-stage, {len(set_1)} early-stage), {G.number_of_edges()} edges")

    return G


def get_bipartite_sets(G: nx.Graph) -> tuple[set, set]:
    """Return (set_0, set_1) from bipartite node attributes."""
    set_0 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 0}
    set_1 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 1}
    return set_0, set_1
