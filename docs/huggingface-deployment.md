# Deploying the Dashboard via HuggingFace

The dashboard supports two modes controlled by a single environment variable:

| Mode | Condition | Data source |
|------|-----------|-------------|
| **Local** | `HF_DATASET_REPO` not set | `.duckdb` files under `outputs/` |
| **HF** | `HF_DATASET_REPO` is set | Parquet files on HuggingFace Datasets |

HF dataset repo: [`joaomelga/innovation-networks-pipelines`](https://huggingface.co/datasets/joaomelga/innovation-networks-pipelines)

---

## 1. Export an experiment to HuggingFace

Run this after each pipeline execution to keep the HF dataset up to date.

> Install hugging face CLI following instruction from [huggingface.co/docs](https://huggingface.co/docs/huggingface_hub/guides/cli#standalone-installer-recommended)

```bash
# Authenticate with HuggingFace first (one-time)
hf auth login

cd ./scripts
uv sync

# Export an experiment (auto-discovers the .duckdb file)
uv run python export_to_hf.py --experiment us

# Or with an explicit path
uv run python export_to_hf.py \
  --experiment us \
  --db-path ../outputs/us/us_modularity.duckdb
```

This exports every table in `HF_TABLES` (defined in `dashboard/db.py`) as a Parquet file and uploads them to `{experiment}/` in the HF dataset repo. Tables that don't exist in a given experiment are silently skipped.

**Dependencies:** `pip install huggingface-hub duckdb`

---

## 2. Test HF mode locally

```bash
HF_DATASET_REPO=joaomelga/innovation-networks-pipelines uv run streamlit run dashboard/0_Home.py
```

The dashboard will discover outputs from HF and query Parquet files over HTTP — no local `.duckdb` file needed.

---

## 3. Deploy on Streamlit Community Cloud

1. Push this repo to GitHub (public or private with Streamlit access).
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Set the main file to `dashboard/0_Home.py`.
4. Under **Advanced settings → Secrets**, add:
   ```toml
   HF_DATASET_REPO = "joaomelga/innovation-networks-pipelines"
   ```
5. Deploy.

---

## How it works

When `HF_DATASET_REPO` is set, `dashboard/db.py`:

1. Lists experiment directories from the HF dataset repo instead of scanning the local filesystem.
2. Creates an **in-memory DuckDB** connection per experiment.
3. Loads the `httpfs` extension and creates SQL views like:
   ```sql
   CREATE VIEW graph.network AS
     SELECT * FROM read_parquet('https://huggingface.co/datasets/joaomelga/innovation-networks-pipelines/resolve/main/us/graph_network.parquet')
   ```

All dashboard pages use unchanged `schema.table` SQL — the views make the switch transparent.

DuckDB's predicate pushdown means only the columns and row ranges needed for a given query are fetched, keeping network usage low even for large tables.

---

## Parquet file layout on HF

```
joaomelga/innovation-networks-pipelines (dataset repo)
└── {experiment}/
    ├── raw_companies.parquet
    ├── raw_investors.parquet
    ├── raw_investments.parquet
    ├── raw_funding_rounds.parquet
    ├── staging_companies_clean.parquet
    ├── staging_investments_clean.parquet
    ├── staging_investments_funded.parquet
    ├── core_vc_investments.parquet
    ├── core_investment_pairs.parquet
    ├── graph_network.parquet
    ├── graph_edges.parquet
    └── experiment_johnson_nestedness.parquet
```
