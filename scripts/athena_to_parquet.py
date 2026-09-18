"""
Convert Athenas raw csv bundle to parquet.
Run this after downloading/updating Athena and after the CPT4 tool
(need API key to update the CONCEPT data with cpt.sh).
Callers prefer parquet files over the raw csv Athena files
when both are present so just running this script is needed to the use parquet files.

Usage: uv run scripts/athena_to_parquet.py --athena-dir /path/to/bundle (and optional: --output-dir /path/to/output)
"""

import argparse
from pathlib import Path
import polars as pl

from omop_etl.db.core.tables import VOCAB_TABLES


def convert(athena_dir: Path, output_dir: Path | None = None) -> None:
    """
    Converts raw Athena bundle to parquet, if no parquet dir is provided
    writes to same dir as raw Athena data.

    quote_char=None: Athena is tab-separated, not RFC-quoted CSV.
    infer_schema_length=0: every column stays a raw string, matching
    what vocabulary/ and db/ already expect from the CSV form
    """
    for table in sorted(VOCAB_TABLES):
        csv_path = athena_dir / f"{table.upper()}.csv"
        if not csv_path.exists():
            print(f"skip {csv_path.name}: not found")
            continue

        parquet_path = output_dir / csv_path.with_suffix(".parquet").name if output_dir is not None else csv_path.with_suffix(".parquet")

        pl.scan_csv(csv_path, separator="\t", infer_schema_length=0, quote_char=None).sink_parquet(parquet_path, compression="zstd")

        print(f"{csv_path.name} -> {parquet_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--athena-dir", "-a", type=Path, required=True)
    parser.add_argument("--output-dir", "-o", type=Path, required=False)
    args = parser.parse_args()
    convert(args.athena_dir, args.output_dir)


if __name__ == "__main__":
    main()
