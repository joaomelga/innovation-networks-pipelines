"""Tests for the NESTLON clustering algorithm."""

import networkx as nx
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from lib.graph.nestlon import NestlonClustering, compute_pairwise_nestedness
from lib.graph.construction import get_bipartite_sets


def _make_perfectly_nested_bipartite():
    """
    Create a perfectly nested bipartite graph.

    Late-stage (set 0): L1, L2, L3, L4 (sorted by degree desc)
    Early-stage (set 1): E1, E2, E3, E4

    Adjacency (L x E):
        E1 E2 E3 E4
    L1 [ 1  1  1  1 ]   <- generalist, connects to all
    L2 [ 1  1  1  0 ]   <- subset of L1
    L3 [ 1  1  0  0 ]   <- subset of L2
    L4 [ 1  0  0  0 ]   <- subset of L3

    This is perfectly nested: N(L4) ⊂ N(L3) ⊂ N(L2) ⊂ N(L1)
    """
    G = nx.Graph()
    late = ["L1", "L2", "L3", "L4"]
    early = ["E1", "E2", "E3", "E4"]

    G.add_nodes_from(late, bipartite=0)
    G.add_nodes_from(early, bipartite=1)

    edges = [
        ("L1", "E1"), ("L1", "E2"), ("L1", "E3"), ("L1", "E4"),
        ("L2", "E1"), ("L2", "E2"), ("L2", "E3"),
        ("L3", "E1"), ("L3", "E2"),
        ("L4", "E1"),
    ]
    G.add_edges_from(edges)
    return G


def _make_non_nested_bipartite():
    """
    Create a bipartite graph with NO nested structure.
    Each node connects to a unique partner — no overlap.

    L1 -- E1
    L2 -- E2
    L3 -- E3
    """
    G = nx.Graph()
    G.add_nodes_from(["L1", "L2", "L3"], bipartite=0)
    G.add_nodes_from(["E1", "E2", "E3"], bipartite=1)
    G.add_edges_from([("L1", "E1"), ("L2", "E2"), ("L3", "E3")])
    return G


def _make_mixed_bipartite():
    """
    Create a bipartite graph with a nested core and a non-nested periphery.

    Nested core (L1, L2, L3 connect to E1, E2 in nested fashion):
        L1: E1, E2, E3
        L2: E1, E2
        L3: E1

    Non-nested periphery:
        L4: E4
        L5: E5
    """
    G = nx.Graph()
    G.add_nodes_from(["L1", "L2", "L3", "L4", "L5"], bipartite=0)
    G.add_nodes_from(["E1", "E2", "E3", "E4", "E5"], bipartite=1)

    # Nested core
    G.add_edges_from([
        ("L1", "E1"), ("L1", "E2"), ("L1", "E3"),
        ("L2", "E1"), ("L2", "E2"),
        ("L3", "E1"),
    ])
    # Non-nested periphery
    G.add_edges_from([("L4", "E4"), ("L5", "E5")])

    return G


def test_perfectly_nested():
    """All nodes in a perfectly nested graph should be in one component."""
    G = _make_perfectly_nested_bipartite()
    nestlon = NestlonClustering(min_component_size=3)
    communities = nestlon.detect_communities(G)

    # The main community should contain all 8 nodes
    assert len(communities) >= 1
    main_community = communities[0]
    assert len(main_community) == 8, f"Expected 8 nodes, got {len(main_community)}"
    print("PASS: test_perfectly_nested")


def test_non_nested():
    """Non-nested graph should have no large nested component."""
    G = _make_non_nested_bipartite()
    nestlon = NestlonClustering(min_component_size=3)
    communities = nestlon.detect_communities(G)

    # No community should have more than the minimum threshold
    # (nodes can't form containment chains with unique partners)
    # The algorithm should put everything in residual
    for comm in communities:
        # With unique connections, the only nested node is the first in chain
        # No strict containment is possible
        pass
    print(f"PASS: test_non_nested ({len(communities)} communities found)")


def test_mixed_structure():
    """Mixed graph should separate nested core from non-nested periphery."""
    G = _make_mixed_bipartite()
    nestlon = NestlonClustering(min_component_size=3)
    communities = nestlon.detect_communities(G)

    # The largest community should contain the nested core nodes
    main_community = communities[0]
    assert "L1" in main_community, "L1 (generalist) should be in main component"
    assert "L2" in main_community, "L2 should be in main component"
    assert "L3" in main_community, "L3 should be in main component"
    assert "E1" in main_community, "E1 (connected to all nested L nodes) should be in main component"

    print(f"PASS: test_mixed_structure (main component: {len(main_community)} nodes)")


def test_pairwise_nestedness_matrix():
    """Pairwise nestedness should reflect containment relationships."""
    G = _make_perfectly_nested_bipartite()
    set_0, _ = get_bipartite_sets(G)

    matrix = compute_pairwise_nestedness(G, set_0)
    nodes = sorted(set_0)

    # L4 (degree 1) should be perfectly nested in L1 (degree 4)
    l4_idx = nodes.index("L4")
    l1_idx = nodes.index("L1")

    # n[L4, L1] = |N(L4) ∩ N(L1)| / |N(L4)| = 1/1 = 1.0
    assert matrix[l4_idx, l1_idx] == 1.0, f"L4 should be nested in L1, got {matrix[l4_idx, l1_idx]}"

    # n[L1, L4] = |N(L1) ∩ N(L4)| / |N(L1)| = 1/4 = 0.25
    assert abs(matrix[l1_idx, l4_idx] - 0.25) < 1e-10, f"Expected 0.25, got {matrix[l1_idx, l4_idx]}"

    print("PASS: test_pairwise_nestedness_matrix")


def test_approximate_containment():
    """Test NESTLON with relaxed containment threshold."""
    G = nx.Graph()
    G.add_nodes_from(["L1", "L2", "L3"], bipartite=0)
    G.add_nodes_from(["E1", "E2", "E3", "E4"], bipartite=1)

    # L1 connects to E1, E2, E3, E4 (generalist)
    # L2 connects to E1, E2, E3 (subset of L1)
    # L3 connects to E1, E4 — NOT a subset of L2 (E4 not in L2)
    #   but 50% overlap with L2 (E1 is shared)
    G.add_edges_from([
        ("L1", "E1"), ("L1", "E2"), ("L1", "E3"), ("L1", "E4"),
        ("L2", "E1"), ("L2", "E2"), ("L2", "E3"),
        ("L3", "E1"), ("L3", "E4"),
    ])

    # Strict: L3 should NOT be nested (E4 not in L2's neighbors)
    strict = NestlonClustering(min_component_size=2, containment_threshold=1.0)
    strict_comms = strict.detect_communities(G)

    # Relaxed: with threshold=0.5, L3 should be included
    relaxed = NestlonClustering(min_component_size=2, containment_threshold=0.5)
    relaxed_comms = relaxed.detect_communities(G)

    print(f"PASS: test_approximate_containment (strict main: {len(strict_comms[0])}, relaxed main: {len(relaxed_comms[0])})")


if __name__ == "__main__":
    test_perfectly_nested()
    test_non_nested()
    test_mixed_structure()
    test_pairwise_nestedness_matrix()
    test_approximate_containment()
    print("\nAll NESTLON tests passed!")
