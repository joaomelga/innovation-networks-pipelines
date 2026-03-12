# Innovation Networks Pipelines

A data engineering project that builds reproducible pipelines for analyzing venture capital syndication networks, applying the methodology from my research on nested investor syndication structures (Melga, 2025).

## Motivation

This repository explores the intersection of **academic research** and **modern data engineering practices**. It reimplements the data processing and network analysis workflows from my master's thesis — originally developed as exploratory Jupyter notebooks — into structured, production-style data pipelines using [Bruin](https://github.com/bruin-data/bruin).

The skills and tools applied here were largely acquired through the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp), where I gained hands-on experience with pipeline orchestration, data warehousing, and analytics engineering (see my [zoomcamp homework repo](https://github.com/joaomelga/data-eng-zoomcamp-hw-2026) for details).

## What's Inside

- **Bruin pipelines** for ingesting, cleaning, and transforming Crunchbase venture capital data
- **Graph construction** of bipartite investor syndication networks with community detection
- **Nestedness analysis** using the Johnson et al. (2013) methodology
- **DuckDB** as local analytical warehouse
- **Automated reports** with scatter plots and correlation analysis

## Project Structure

```
├── .bruin.yml           # Bruin project configuration (DuckDB connection)
├── us-pipeline/         # Main pipeline for US venture capital data
│   ├── pipeline.yml
│   ├── pyproject.toml   # Python dependencies (managed by uv)
│   ├── data/            # Raw data (CSV/CSV.GZ, not tracked in git)
│   └── assets/          # Pipeline assets organized by layer
│       ├── raw/         # Data ingestion (CSV → DuckDB)
│       ├── staging/     # Data cleaning & filtering
│       ├── core/        # Analytical datasets
│       ├── graph/       # Network construction & community detection
│       ├── experiments/ # Nestedness calculations
│       └── reports/     # Visualizations & correlation analysis
└── references/          # Supporting papers
```

See the [us-pipeline README](us-pipeline/README.md) for detailed pipeline architecture, DAG, and run instructions.

## Quick Start

```bash
# Validate the pipeline
bruin validate us-pipeline

# Run (use --workers 1 on Windows to avoid DuckDB file lock issues)
bruin run us-pipeline --workers 1
```

## References

- Melga, J. (2025). *Nested Investor Syndication Networks*. Master's thesis.
- Johnson, S., Domínguez-García, V., & Muñoz, M. A. (2013). Factors determining nestedness in complex networks. *PLoS ONE*, 8(9).

## Author

**João Melga** — [GitHub](https://github.com/joaomelga)