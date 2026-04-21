import networkx as nx
from networkx.algorithms.community import greedy_modularity_communities

from lib.graph.registry import register


@register
class ModularityClustering:
    """Greedy modularity maximization community detection."""

    name = "modularity"

    def detect_communities(self, G: nx.Graph) -> list[set[str]]:
        communities = list(greedy_modularity_communities(G))
        communities = sorted(communities, key=len, reverse=True)

        print(f"Detected {len(communities)} communities")
        for i, comm in enumerate(communities[:10]):
            print(f"  Community {i}: {len(comm)} nodes")

        return communities
