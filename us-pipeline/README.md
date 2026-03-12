# US VC Syndication Networks Pipeline

Bruin pipeline that replicates the data cleaning, graph construction, and Johnson nestedness analysis originally developed in the [innovation-networks-exploration](https://github.com/joaomelga/innovation-networks-exploration) project.

## Pipeline Architecture

```
raw/                # Load CSV.GZ files from memoire/data/raw/us into DuckDB
  ├─ raw_companies.py
  ├─ raw_investors.py
  ├─ raw_investments.py
  └─ raw_funding_rounds.py

staging/            # Clean & filter data (paper methodology)
  ├─ stg_companies_clean.sql        (exclude post-2017, exits, missing info)
  ├─ stg_investments_clean.sql      (remove missing/invalid records)
  └─ stg_investments_funded.sql     ($150K threshold, exclude accelerator-only)

core/               # Build analytical datasets
  ├─ core_vc_investments.sql        (extract VC investments, create node names)
  └─ core_investment_pairs.sql      (create bipartite edge pairs)

graph/              # Network construction
  ├─ graph_nodes.py                 (nodes and respective communities)
  └─ graph_edges.sql                (edges table with community assignments)

experiments/
  └─ johnson/
      └─ exp_johnson_nestedness.py  (Johnson 2013 nestedness per community)

reports/
  └─ report_johnson_nestedness.py   (scatter plots: local nestedness vs degree)
```

## DAG (Directed Acyclic Graph)

![DAG](https://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/joaomelga/innovation-networks-pipelines/docs/initial-docs/us-pipeline/diagrams/DAG.puml&fmt=svg)

## How to Run

```bash
# From the bruin/ directory:

# Validate the pipeline
bruin validate us-pipeline

# Run the full pipeline
bruin run us-pipeline

# Run a specific asset (with its upstream dependencies)
bruin run us-pipeline/assets/reports/report_johnson_nestedness.py

# Run only the experiment
bruin run us-pipeline/assets/experiments/johnson/exp_johnson_nestedness.py
```

## Outputs

- **DuckDB tables**: All intermediate and final tables stored in `us-pipeline/us_pipeline.duckdb`
  - `raw.*` — raw loaded data
  - `staging.*` — cleaned data
  - `core.*` — VC investments and investment pairs
  - `graph.network` — nodes with community assignments (equivalent to `nodes.csv`)
  - `graph.edges` — edges with community info (equivalent to `edges.csv`)
  - `experiment.johnson_nestedness` — local nestedness metrics per node
- **Figures**: `us-pipeline/outputs/figures/johnson_nestedness/`
  - `johnson_nestedness_vs_degree.png` — scatter plot
  - `johnson_gnorm_comparison.png` — bar chart comparing communities
  - `johnson_summary.csv` — global metrics per community

## Data Source

Raw data is read from `memoire/data/raw/us/` (CSV.GZ files):
- `companies.csv.gz`
- `investors.csv.gz`
- `investments.csv.gz`
- `funding_rounds.csv.gz`

## Key Concepts

- **Set 0** (left / late-stage): Investors participating in series_b+ rounds
- **Set 1** (right / early-stage): Investors participating in angel/seed/series_a rounds
- **Johnson nestedness g_norm**: `g_raw / g_conf` — measures how much the network deviates from random nestedness
  - `g_norm > 1` → more nested than expected (specialist-generalist hierarchy)
  - `g_norm < 1` → less nested (anti-nested)
  - `g_norm ≈ 1` → random structure