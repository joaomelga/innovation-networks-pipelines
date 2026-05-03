import networkx as nx
from lib.graph.registry import register


@register
class WholeNetworkClustering:
    """Trivial clustering: returns the entire graph as a single community.

    Use this method to run temporal/global experiments on the whole bipartite
    network without any community partitioning. In graph.edges this produces
    community = 0 for every edge, so temporal experiments iterate over that
    single community and operate on the full graph — identical to the
    mathematician's improved experiments that had no community concept."""

    name = "whole_network"

    def detect_communities(self, G: nx.Graph) -> list[set[str]]:
        return [set(G.nodes())]
