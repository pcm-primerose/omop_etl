from pathlib import Path
import json
import polars as pl
from omop_etl.infra.types import Layout
from omop_etl.infra.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService


def run_harmonization_test(input_csv: Path, base_root: Path, trial: str = "IMPRESS") -> dict:
    """
    End-to-end test of the harmonization module on a single CSV.
    Returns a dict with paths and quick stats.
    """
    base_root.mkdir(parents=True, exist_ok=True)

    # Service with planner layout.  IMPORTANT: use a neutral base_root (e.g. ".data")
    svc = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_RUN)
    meta = RunMetadata.create(trial)

    # Run both modes, multiple formats to prove fan-out
    formats = ["csv", "parquet", "json"]  # wide supports all; normalized ignores json
    hd = svc.run(
        trial=trial,
        input_path=input_csv,
        formats=formats,
        write_wide=True,
        write_normalized=True,
        meta=meta,
    )

    # Derived directories (planner: <base>/<module>/<trial>/<run_id>/...)
    module = "harmonized"
    run_dir = base_root / module / trial.lower() / meta.run_id

    # Expected WIDE CSV paths (stem="harmonized_wide", fmt="csv")
    wide_csv_dir = run_dir / "harmonized_wide" / "csv"
    wide_csv_file = wide_csv_dir / "data_harmonized_wide.csv"
    wide_manifest = wide_csv_dir / "manifest_harmonized_wide.json"

    # Expected NORMALIZED PARQUET manifest (stem="harmonized_norm", fmt="parquet")
    norm_parquet_dir = run_dir / "harmonized_norm" / "parquet"
    norm_manifest = norm_parquet_dir / "manifest_harmonized_norm.json"

    # Assertions
    if not wide_csv_file.exists():
        raise AssertionError(f"Wide CSV not found: {wide_csv_file}")
    if not wide_manifest.exists():
        raise AssertionError(f"Wide manifest not found: {wide_manifest}")
    if not norm_manifest.exists():
        raise AssertionError(f"Normalized manifest (parquet) not found: {norm_manifest}")

    # Quick stats
    wide_df = pl.read_csv(wide_csv_file)

    # Collect normalized table files from the parquet manifest
    with norm_manifest.open("r", encoding="utf-8") as fp:
        norm_meta = json.load(fp)

    normalized_files = {name: Path(meta_entry.get("file")) for name, meta_entry in norm_meta.get("tables", {}).items() if meta_entry.get("file")}
    if not normalized_files:
        raise AssertionError("No normalized tables were written (manifest had no table files).")

    patients_file = normalized_files.get("patients")
    if patients_file and not patients_file.exists():
        raise AssertionError(f"'patients' table expected but missing: {patients_file}")

    # Also sanity-check the other wide formats we asked for:
    wide_parquet = run_dir / "harmonized_wide" / "parquet" / "data_harmonized_wide.parquet"
    wide_json = run_dir / "harmonized_wide" / "json" / "data_harmonized_wide.json"
    if not wide_parquet.exists():
        raise AssertionError(f"Wide parquet not found: {wide_parquet}")
    if not wide_json.exists():
        raise AssertionError(f"Wide JSON not found: {wide_json}")

    return {
        "run_dir": run_dir,
        "wide_csv": wide_csv_file,
        "wide_rows": wide_df.height,
        "wide_cols": wide_df.width,
        "wide_manifest": wide_manifest,
        "normalized_manifest_parquet": norm_manifest,
        "normalized_files": normalized_files,
        "num_patients_in_memory": len(hd.patients),
    }


if __name__ == "__main__":
    # Example input path
    impress_150_file = Path(__file__).parents[3] / ".data" / "preprocessing" / "impress" / "20250909T144845Z_19d79919" / "data_preprocessed.csv"
    # Use a neutral base root to avoid nested module names
    run_harmonization_test(input_csv=Path(impress_150_file), trial="IMPRESS", base_root=Path(".data"))
