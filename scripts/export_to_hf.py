"""Export DuckDB experiment tables to Parquet and upload to HuggingFace Datasets.

Usage:
    python scripts/export_to_hf.py --experiment us-modularity
    python scripts/export_to_hf.py --experiment us-modularity --repo joaomelga/innovation-networks-pipelines
    python scripts/export_to_hf.py --experiment us-modularity --db-path experiments/us-modularity/us_modularity.duckdb
"""

import argparse
import tempfile
from pathlib import Path

import duckdb
from huggingface_hub import HfApi

from sys import path as sys_path

sys_path.insert(0, str(Path(__file__).resolve().parent.parent / "dashboard"))
from db import HF_TABLES

DEFAULT_REPO = "joaomelga/innovation-networks-pipelines"
REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_db_path(experiment: str, db_path_override: str | None) -> str:
    if db_path_override:
        return db_path_override
    exp_dir = REPO_ROOT / "outputs" / experiment
    candidates = list(exp_dir.glob("*.duckdb"))
    if not candidates:
        raise FileNotFoundError(f"No .duckdb file found in {exp_dir}/")
    return str(candidates[0])


def main():
    parser = argparse.ArgumentParser(description="Export DuckDB tables to HuggingFace Datasets")
    parser.add_argument("--experiment", required=True, help="Experiment directory name (e.g. us-modularity)")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="HuggingFace dataset repo ID")
    parser.add_argument("--db-path", help="Override .duckdb file path (auto-discovered if omitted)")
    args = parser.parse_args()

    db_path = resolve_db_path(args.experiment, args.db_path)
    print(f"Connecting to: {db_path}")
    conn = duckdb.connect(db_path, read_only=True)
    api = HfApi()

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp) / args.experiment
        out_dir.mkdir()

        for schema, table in HF_TABLES:
            out_file = out_dir / f"{schema}_{table}.parquet"
            try:
                conn.execute(f"COPY {schema}.{table} TO '{out_file}' (FORMAT PARQUET)")
                size_mb = out_file.stat().st_size / 1_048_576
                print(f"  exported {schema}.{table} → {out_file.name} ({size_mb:.1f} MB)")
            except duckdb.CatalogException:
                print(f"  skipped  {schema}.{table} (not in this experiment)")

        print(f"\nUploading {args.experiment}/ to {args.repo} ...")
        api.upload_folder(
            repo_id=args.repo,
            repo_type="dataset",
            folder_path=str(out_dir),
            path_in_repo=args.experiment,
        )
        print(f"Done. Files available at https://huggingface.co/datasets/{args.repo}/tree/main/{args.experiment}")


if __name__ == "__main__":
    main()
