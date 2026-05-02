# Innovation Networks Pipelines

A multi-region, multi-method research platform for analyzing structural properties of innovation networks — investor syndication, enterprise co-investment, and beyond. Built around reproducible [Bruin](https://github.com/bruin-data/bruin) pipelines and interactive Streamlit dashboards.

## Contents

- [Innovation Networks Pipelines](#innovation-networks-pipelines)
  - [Contents](#contents)
  - [Dashboard](#dashboard)
  - [Motivation](#motivation)
  - [What's Inside](#whats-inside)
  - [Project Structure](#project-structure)
  - [Prerequisites](#prerequisites)
    - [uv (Python package manager)](#uv-python-package-manager)
    - [Bruin CLI](#bruin-cli)
  - [Quick Start](#quick-start)
    - [1. Install dependencies](#1-install-dependencies)
    - [2. Run the pipeline for a region](#2-run-the-pipeline-for-a-region)
    - [3. Launch the Streamlit dashboard](#3-launch-the-streamlit-dashboard)
  - [Environment Variables](#environment-variables)
  - [Clustering Methods](#clustering-methods)
  - [References](#references)
  - [Author](#author)

## Dashboard

The interactive dashboard lets you explore any region's pipeline output — switch between pipeline layers, inspect community structure, and visualize nestedness results in real time.

> **Live app:** [innovation-networks-pipelines.streamlit.app](https://innovation-networks-pipelines.streamlit.app/)

Community Explorer and Nestedness Analysis support a **Clustering Method** selector to compare nestlon and modularity results within the same run.

## Motivation

This repository bridges **academic research** and **production-grade data engineering**. It reimplements the data processing and network analysis workflows originally developed as exploratory Jupyter notebooks in the [innovation-networks-exploration](https://github.com/joaomelga/innovation-networks-exploration) project into structured, reproducible pipelines — making experiments comparable, extensible, and shareable.

## What's Inside

- **Bruin pipelines** for ingesting, cleaning, and transforming Crunchbase venture capital data
- **Graph construction** of bipartite investor syndication networks with all clustering methods run in a single pass
- **Nestedness analysis** using the Johnson et al. (2013) methodology
- **NESTLON algorithm** (Grimm & Tessone, 2017) for nestedness-aware community detection
- **DuckDB** as local analytical warehouse (one per region, all methods stored together)
- **Interactive Streamlit dashboards** for exploring pipeline data, communities, and nestedness results

## Project Structure

```
innovation-networks-pipelines/
├── .bruin.yml               # Bruin environments: us, fr, eu
├── pipeline.yml             # Single unified pipeline definition
├── pyproject.toml           # Python dependencies
├── assets/                  # Unified pipeline assets (raw → reports)
│   ├── _lib/
│   │   └── config.py        # REGION env var resolution, path helpers
│   ├── raw/                 # Ingest raw CSVs
│   ├── staging/             # Clean and filter
│   ├── core/                # VC focus, investment pairs
│   ├── graph/               # Community detection (all methods in one pass)
│   ├── experiments/
│   │   └── johnson/         # Johnson nestedness (all methods in one pass)
│   └── reports/             # Figures and summary CSVs
├── lib/                     # Shared Python library
│   ├── graph/               # Graph construction, clustering (modularity, NESTLON)
│   ├── nestedness/          # Nestedness calculators (Johnson JDM-NODF)
│   └── utils/               # DuckDB helpers, bipartite matrix utilities
├── data/                    # Raw data (CSV.GZ)
│   ├── us/                  # US Crunchbase data
│   └── fr/                  # France Crunchbase data
├── outputs/                 # Generated DuckDBs and figures (git-ignored)
│   ├── us/pipeline.duckdb
│   └── fr/pipeline.duckdb
└── dashboard/               # Streamlit multi-region dashboard
    ├── 0_Home.py
    └── pages/
```

A single pipeline run for a given region computes community structure for **all clustering methods** at once (`nestlon` and `modularity`). Results are stored in the same DuckDB with a `clustering_method` column.

## Prerequisites

### uv (Python package manager)

This project uses [uv](https://docs.astral.sh/uv/) to manage Python dependencies. Install it by following the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/).

### Bruin CLI

Install the [Bruin CLI](https://github.com/bruin-data/bruin) to run the data pipelines.

## Quick Start

### 1. Install dependencies

```bash
uv sync
```

### 2. Run the pipeline for a region

@todo: add possibility to run only one clustering method

```bash
# United States
REGION=us bruin run . --environment us assets/ --workers 1

# France
REGION=fr bruin run . --environment fr assets/ --workers 1

# Europe
REGION=eu bruin run . --environment eu assets/ --workers 1
```

The pipeline runs in layer order: `raw` → `staging` → `core` → `graph` → `experiments` → `reports`. Both `nestlon` and `modularity` community detection methods are run in a single pass and stored in `outputs/{REGION}/pipeline.duckdb`.

To run a single asset or start from a specific point:

```bash
# Run from graph onward (re-run community detection and analysis)
REGION=us bruin run . --environment us --downstream assets/graph/graph_nodes.py assets/

# Run a single asset
REGION=us bruin run . --environment us assets/experiments/johnson/exp_johnson_nestedness.py
```

### 3. Launch the Streamlit dashboard

```bash
cd dashboard
uv run streamlit run 0_Home.py
```

The dashboard auto-discovers any region with a populated `outputs/{region}/pipeline.duckdb` file. Use the **Region** selector in the sidebar to switch between regions.

| Page | Region selector | Method selector | Description |
|---|---|---|---|
| Home | — | — | Pipeline overview and run instructions |
| Pipeline Funnel | ✓ | — | Record counts at each layer, filter breakdowns |
| Data Explorer | ✓ | — | Geography, sectors, funding distributions, temporal trends |
| Community Explorer | ✓ | ✓ | Community structure, bipartite composition, network visualization |
| Nestedness Analysis | ✓ | ✓ | Johnson g_norm charts, degree vs nestedness scatter, asymmetry analysis |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REGION` | `us` | Target region — must match a `.bruin.yml` environment (`us`, `fr`, `eu`) |
| `BRUIN_RAW_DIR` | `data/{REGION}/` | Override path to raw CSV data directory |
| `BRUIN_DUCKDB_PATH` | `outputs/{REGION}/pipeline.duckdb` | Override path to the output DuckDB file |
| `BRUIN_FIGURES_DIR` | `outputs/{REGION}/figures/` | Override path to the figures output directory |

## Clustering Methods

All methods run automatically in every pipeline execution. Results are differentiated by the `clustering_method` column in `graph.network`, `graph.edges`, and `experiment.johnson_nestedness`.

| Method | `clustering_method` value | Description |
|---|---|---|
| Greedy modularity | `modularity` | Optimizes modularity Q (Newman) |
| NESTLON | `nestlon` | Detects nested components via local neighborhood containment (Grimm & Tessone, 2017) |

## References

- Melga, J., Leroy, B., & Dalle, J.-M. (2026). *Staged Bipartite Venture Networks: Preliminary evidence of asymmetric nestedness in the Silicon Valley*. Extended abstract.
- Johnson, S., Domínguez-García, V., & Muñoz, M. A. (2013). Factors determining nestedness in complex networks. *PLoS ONE*, 8(9).
- Grimm, A., & Tessone, C. J. (2017). Analysing the sensitivity of nestedness detection methods. *Applied Network Science*, 2, 21.

## Author

**João Melga** — [GitHub](https://github.com/joaomelga)
