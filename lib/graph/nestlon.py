import networkx as nx
import numpy as np

from lib.graph.registry import register


@register
class NestlonClustering:
    """
    NESTLON (Nestedness detection based on Local Neighbourhood).
    Grimm & Tessone (2017).

    Detects nested components in bipartite graphs by checking whether
    low-degree nodes' neighborhoods are subsets of high-degree nodes'
    neighborhoods. Returns nested components as communities.
    """

    name = "nestlon"

    def __init__(self, min_component_size: int = 3, containment_threshold: float = 1.0):
        """
        Args:
            min_component_size: Minimum number of nodes for a nested component.
            containment_threshold: Fraction of neighborhood that must be contained
                for a node to be considered nested (1.0 = strict subset,
                <1.0 = approximate containment).
        """
        self.min_component_size = min_component_size
        self.containment_threshold = containment_threshold

    def detect_communities(self, G: nx.Graph) -> list[set[str]]:
        """
        Detect nested components in a bipartite graph.

        Returns communities as a list of node sets, sorted by size descending.
        Nodes not in any nested component are grouped into a residual community.
        """
        set_0 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 0}
        set_1 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 1}

        communities = []
        remaining_0 = set(set_0)
        remaining_1 = set(set_1)

        iteration = 0
        while remaining_0 and remaining_1:
            remaining = remaining_0 | remaining_1
            H = G.subgraph(remaining).copy()

            # Remove isolated nodes from subgraph
            isolates = set(nx.isolates(H))
            H.remove_nodes_from(isolates)
            remaining_0 -= isolates
            remaining_1 -= isolates

            if not remaining_0 or not remaining_1:
                break

            nested_component = self._find_nested_component(
                H, remaining_0 & set(H.nodes()), remaining_1 & set(H.nodes())
            )

            if len(nested_component) < self.min_component_size:
                break

            communities.append(nested_component)
            remaining_0 -= nested_component
            remaining_1 -= nested_component
            iteration += 1

        # Add remaining nodes as residual community
        residual = remaining_0 | remaining_1
        if residual:
            communities.append(residual)

        communities = sorted(communities, key=len, reverse=True)

        print(f"NESTLON: detected {len(communities)} components")
        for i, comm in enumerate(communities[:10]):
            print(f"  Component {i}: {len(comm)} nodes")

        return communities

    def _find_nested_component(
        self, G: nx.Graph, set_0: set, set_1: set
    ) -> set[str]:
        """
        Find the maximal nested component in a bipartite graph.

        For each bipartite set, identify nodes whose neighborhoods form
        a containment chain (low-degree neighborhoods are subsets of
        high-degree neighborhoods).
        """
        nested_0 = self._extract_nested_chain(G, set_0)
        nested_1 = self._extract_nested_chain(G, set_1)

        # A nested component includes nested nodes from both sides
        # plus their cross-set neighbors that participate in the nesting
        component = set()

        # Add nested nodes from set_0 and their neighbors in set_1
        for node in nested_0:
            component.add(node)
            neighbors_in_1 = {n for n in G.neighbors(node) if n in set_1}
            component.update(neighbors_in_1)

        # Add nested nodes from set_1 and their neighbors in set_0
        for node in nested_1:
            component.add(node)
            neighbors_in_0 = {n for n in G.neighbors(node) if n in set_0}
            component.update(neighbors_in_0)

        return component & set(G.nodes())

    def _extract_nested_chain(self, G: nx.Graph, node_set: set) -> set[str]:
        """
        From a set of nodes in the same bipartite partition, extract those
        forming a nested chain (neighborhood containment hierarchy).

        Nodes are sorted by degree descending. A node is included if its
        neighborhood is contained in some already-included node's neighborhood.
        """
        if not node_set:
            return set()

        sorted_nodes = sorted(node_set, key=lambda n: G.degree(n), reverse=True)

        # Highest-degree node always starts the chain
        nested = {sorted_nodes[0]}
        reference_neighborhoods = [frozenset(G.neighbors(sorted_nodes[0]))]

        for node in sorted_nodes[1:]:
            node_neighbors = set(G.neighbors(node))
            if not node_neighbors:
                continue

            if self.containment_threshold >= 1.0:
                # Strict containment: N(u) ⊆ N(v)
                is_nested = any(
                    node_neighbors.issubset(ref_nb)
                    for ref_nb in reference_neighborhoods
                )
            else:
                # Approximate containment: |N(u) ∩ N(v)| / |N(u)| >= threshold
                is_nested = any(
                    len(node_neighbors & ref_nb) / len(node_neighbors) >= self.containment_threshold
                    for ref_nb in reference_neighborhoods
                )

            if is_nested:
                nested.add(node)
                reference_neighborhoods.append(frozenset(node_neighbors))

        return nested


def compute_pairwise_nestedness(G: nx.Graph, node_set: set) -> np.ndarray:
    """
    Compute pairwise nestedness matrix for nodes in the same bipartite set.

    n[i,j] = |N(i) ∩ N(j)| / |N(i)| — the fraction of i's neighbors
    that are also j's neighbors. If n[i,j] = 1, then i is nested in j.

    Returns a square matrix indexed by sorted node list.
    """
    nodes = sorted(node_set)
    n = len(nodes)
    neighborhoods = {node: set(G.neighbors(node)) for node in nodes}

    matrix = np.zeros((n, n))
    for i, u in enumerate(nodes):
        nu = neighborhoods[u]
        if not nu:
            continue
        for j, v in enumerate(nodes):
            if i == j:
                continue
            nv = neighborhoods[v]
            matrix[i, j] = len(nu & nv) / len(nu)

    return matrix
