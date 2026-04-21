"""@bruin
name: graph.network
image: python:3.13
connection: duckdb-us-modularity

depends:
  - core.investment_pairs

materialization:
  type: table
  strategy: create+replace

description: |
  Build bipartite syndication graph from investment pairs.
  Detect communities using the clustering method specified in config.yml.
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
import yaml
from pathlib import Path

# Add repo root to path so lib/ is importable
EXPERIMENT_DIR = Path(__file__).resolve().parent.parent.parent
REPO_ROOT = EXPERIMENT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from lib.graph.construction import build_bipartite_graph, get_bipartite_sets
from lib.graph.registry import get_method
import lib.graph.modularity  # register modularity
import lib.graph.nestlon     # register nestlon

CONFIG = yaml.safe_load(open(EXPERIMENT_DIR / "config.yml"))
DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", str(EXPERIMENT_DIR / "us_modularity.duckdb"))


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
    method_name = CONFIG["clustering_method"]
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
