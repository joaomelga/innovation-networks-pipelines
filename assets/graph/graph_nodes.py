"""@bruin
name: graph.network
image: python:3.13
connection: duckdb-default

depends:
  - core.investment_pairs

materialization:
  type: table
  strategy: create+replace

description: |
  Build bipartite syndication graph from investment pairs.
  Detects communities using ALL supported clustering methods (nestlon, modularity).
  Each method's results are stored as separate rows with a clustering_method column.

columns:
  - name: node
    type: string
  - name: community_id
    type: integer
  - name: community_size
    type: integer
  - name: set
    type: integer
  - name: clustering_method
    type: string

@bruin"""

import pandas as pd
import duckdb
import os
import sys
from pathlib import Path

_ASSETS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))

from _lib.config import duckdb_path
from lib.graph.construction import build_bipartite_graph, get_bipartite_sets
from lib.graph.registry import get_method
import lib.graph.modularity      # register modularity
import lib.graph.nestlon         # register nestlon
import lib.graph.whole_network   # register whole_network

DB_PATH = str(duckdb_path())
ALL_METHODS = ["nestlon", "modularity", "whole_network"]


def materialize():
    con = duckdb.connect(DB_PATH, read_only=True)
    pairs_df = con.execute("SELECT * FROM core.investment_pairs").fetchdf()
    con.close()

    print(f"Loaded {len(pairs_df)} investment pairs")

    G = build_bipartite_graph(pairs_df)
    set_0, set_1 = get_bipartite_sets(G)

    all_rows = []
    for method_name in ALL_METHODS:
        print(f"Running clustering method: {method_name}")
        method = get_method(method_name)
        communities = method.detect_communities(G)

        for comm_idx, community in enumerate(communities):
            for node in community:
                bipartite_set = 0 if node in set_0 else 1
                all_rows.append({
                    "node": node,
                    "community_id": comm_idx,
                    "community_size": len(community),
                    "id": node,
                    "set": bipartite_set,
                    "clustering_method": method_name,
                })

        print(f"  {method_name}: {len(communities)} communities detected")

    nodes_df = pd.DataFrame(all_rows)
    print(f"Nodes table: {len(nodes_df)} rows ({len(ALL_METHODS)} methods)")
    return nodes_df
