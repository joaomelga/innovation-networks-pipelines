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
  Detect communities using greedy modularity.
  Produce the canonical 'nodes' table with community assignments and bipartite set membership.
  This asset materializes the NODES table; the edges table is built separately.

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
import numpy as np
import duckdb
import os
import networkx as nx
from networkx.algorithms.community import greedy_modularity_communities

DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", "us_pipeline.duckdb")

def materialize():
    # 1. Load investment pairs from DuckDB
    con = duckdb.connect(DB_PATH, read_only=True)
    pairs_df = con.execute("SELECT * FROM core.investment_pairs").fetchdf()
    con.close()

    print(f"Loaded {len(pairs_df)} investment pairs")

    # 2. Build bipartite graph
    G = nx.Graph()

    left_nodes = set(pairs_df["investor_name_left"].dropna().unique())
    right_nodes = set(pairs_df["investor_name_right"].dropna().unique())

    G.add_nodes_from(left_nodes, bipartite=0)   # set 0 = late-stage
    G.add_nodes_from(right_nodes, bipartite=1)  # set 1 = early-stage

    for _, row in pairs_df.iterrows():
        G.add_edge(row["investor_name_left"], row["investor_name_right"])

    set_0 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 0}
    set_1 = {n for n, d in G.nodes(data=True) if d.get("bipartite") == 1}

    print(f"Graph: {G.number_of_nodes()} nodes ({len(set_0)} late-stage, {len(set_1)} early-stage), {G.number_of_edges()} edges")

    # 3. Community detection
    communities = list(greedy_modularity_communities(G))
    communities = sorted(communities, key=lambda x: len(x), reverse=True)

    print(f"Detected {len(communities)} communities")
    for i, comm in enumerate(communities[:10]):
        print(f"  Community {i}: {len(comm)} nodes")

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
