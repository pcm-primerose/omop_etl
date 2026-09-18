import argparse
import os
from pathlib import Path

from omop_etl.cli.main import main as cli_main
from omop_etl.env_config import (
    ATHENA_DIR,
    DATA_ROOT,
    DEFAULT_DATASET,
    LOG_LEVEL,
    MAPPING_DIR,
    SYNTHETIC_DATASETS,
    resolve_dataset,
)

DEFAULT_LOCAL_DSN = "postgresql://omop:omop@localhost:5433/omop"


def main() -> int:
    """
    Local dev convenience wrapper around `etl load` (src/omop_etl/cli/main.py):
    resolves --dataset/.env defaults into the real CLI args and delegates the
    actual pipeline run to it.
    """
    parser = argparse.ArgumentParser(description="OMOP ETL pipeline (local dev wrapper around `etl load`)")
    parser.add_argument(
        "--dataset",
        default=None,
        help=f"Dataset name ({', '.join(SYNTHETIC_DATASETS)}) or explicit path. Defaults to 'impress_150'.",
    )
    parser.add_argument("--trial", default="IMPRESS")
    parser.add_argument("--target-biomarker", default=None)
    parser.add_argument(
        "--athena-dir",
        type=Path,
        default=ATHENA_DIR,
        help=f"Dir containing this release's Athena CSVs (CONCEPT.csv, VOCABULARY.csv, ...). Defaults to {ATHENA_DIR}.",
    )
    parser.add_argument(
        "--mapping-dir",
        type=Path,
        default=MAPPING_DIR,
        help=f"Dir containing static.csv/structural.csv/semantic.csv. Defaults to {MAPPING_DIR}.",
    )
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL", DEFAULT_LOCAL_DSN))
    parser.add_argument("--with-semantic", action="store_true", help="Enable semantic mapping")
    parser.add_argument("--log-level", default=LOG_LEVEL)
    args = parser.parse_args()

    dataset_path = resolve_dataset(args.dataset) if args.dataset else DEFAULT_DATASET
    if not dataset_path.exists():
        parser.error(f"Dataset path does not exist: {dataset_path}")
    if not args.athena_dir.exists():
        parser.error(f"Athena dir does not exist: {args.athena_dir}")
    if not args.mapping_dir.exists():
        parser.error(f"Mapping dir does not exist: {args.mapping_dir}")

    argv = [
        "load",
        "--input",
        str(dataset_path),
        "--outdir",
        str(DATA_ROOT),
        "--trial",
        args.trial,
        "--athena-dir",
        str(args.athena_dir),
        "--mapping-dir",
        str(args.mapping_dir),
        "--dsn",
        args.dsn,
        "--log-level",
        args.log_level,
    ]
    if args.target_biomarker:
        argv += ["--target-biomarker", args.target_biomarker]
    if args.with_semantic:
        argv += ["--with-semantic"]

    print(f"==> dataset={dataset_path} trial={args.trial} target_biomarker={args.target_biomarker}")
    return cli_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
