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
  Detect communities using the clustering method specified by CLUSTERING_METHOD env var.
  Produce the canonical 'nodes' table with community assignments and bipartite set membership.

columns:
  - name: node
    type: string
  - name: community_id
    type: integer
  - name: community_size
    type: integer
  - name: set
    type: integer

@bruin"""

import pandas as pd
import duckdb
import os
import sys
from pathlib import Path

_ASSETS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ASSETS_DIR))
sys.path.insert(0, str(_ASSETS_DIR.parent))

from _lib.config import clustering_method, duckdb_path
from lib.graph.construction import build_bipartite_graph, get_bipartite_sets
from lib.graph.registry import get_method
import lib.graph.modularity  # register modularity
import lib.graph.nestlon     # register nestlon

DB_PATH = str(duckdb_path())


def materialize():
    # 1. Load investment pairs from DuckDB
    con = duckdb.connect(DB_PATH, read_only=True)
    pairs_df = con.execute("SELECT * FROM core.investment_pairs").fetchdf()
    con.close()

    print(f"Loaded {len(pairs_df)} investment pairs")

    # 2. Build bipartite graph
    G = build_bipartite_graph(pairs_df)
    set_0, set_1 = get_bipartite_sets(G)

    # 3. Dispatch to configured clustering method
    method_name = clustering_method()
    print(f"Using clustering method: {method_name}")
    method = get_method(method_name)
    communities = method.detect_communities(G)

    # 4. Build nodes DataFrame
    rows = []
    for comm_idx, community in enumerate(communities):
        for node in community:
            bipartite_set = 0 if node in set_0 else 1
            rows.append({
                "node": node,
                "community_id": comm_idx,
                "community_size": len(community),
                "id": node,
                "set": bipartite_set,
            })

    nodes_df = pd.DataFrame(rows)
    print(f"Nodes table: {len(nodes_df)} rows")

    return nodes_df
