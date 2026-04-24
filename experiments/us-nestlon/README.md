# us-nestlon

US Crunchbase data + NESTLON community detection (Grimm & Tessone, 2017).

Same raw → core pipeline as `us-modularity`, with `graph.network` using NESTLON instead of greedy modularity. Johnson nestedness analysis is then applied to the detected communities.

## Algorithm

NESTLON finds nested components by neighborhood containment: starting from the highest-degree node, it greedily adds nodes whose neighbor sets are subsets (within `containment_threshold`) of already-included nodes. This iterates on the residual graph to produce multiple components, with the final residual collected into one catch-all community.

Configured in `config.yml`:
```yaml
clustering_method: nestlon
```

The implementation lives in `../../lib/graph/nestlon.py`.

## Run the pipeline

```bash
# From repo root — install dependencies first (only needed once)
cd experiments/us-nestlon
uv sync
cd ../..

# Validate
bruin validate experiments/us-nestlon

# Run full pipeline (--workers 1 avoids DuckDB file-locking on Windows)
bruin run experiments/us-nestlon --workers 1
```

Pipeline layers run in order: `raw` → `staging` → `core` → `graph` → `experiments` → `reports`.

To resume from a specific asset:
```bash
bruin run experiments/us-nestlon --downstream experiments/us-nestlon/assets/graph/graph_nodes.py
```

To run a single asset:
```bash
bruin run experiments/us-nestlon/assets/experiments/johnson/exp_johnson_nestedness.py
```

Outputs are written to `outputs/figures/johnson_nestedness/`:
- `johnson_summary.csv` — global g_norm per community
- `johnson_nestedness_vs_degree.png` — local g_norm vs degree scatter, faceted by community
- `johnson_gnorm_comparison.png` — g_norm bar chart across communities

## Run the tests

Tests for the NESTLON algorithm are in `lib/tests/test_nestlon.py`. Run from the repo root:

```bash
# With pytest (recommended)
cd lib
uv run pytest tests/test_nestlon.py -v

# Or directly
uv run python tests/test_nestlon.py
```

The tests cover:
| Test | What it checks |
|---|---|
| `test_perfectly_nested` | All nodes in a fully nested graph land in one component |
| `test_non_nested` | Graph with no containment structure produces no large component |
| `test_mixed_structure` | Nested core is separated from non-nested periphery |
| `test_pairwise_nestedness_matrix` | Containment ratios are computed correctly |
| `test_approximate_containment` | Relaxed `containment_threshold` includes near-nested nodes |

## Compare with us-modularity

Both experiments share the same data and all layers up to `core`. The only difference is in `graph.network`:

| | us-modularity | us-nestlon |
|---|---|---|
| Method | Greedy modularity (Newman) | NESTLON (Grimm & Tessone) |
| Community criterion | Maximizes modularity Q | Neighborhood containment |
| Expected structure | Assortative groups | Nested components |

To compare communities, load both experiments in the Streamlit dashboard and use the Community Explorer page.
