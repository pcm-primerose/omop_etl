import polars as pl
from pathlib import Path
import json

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.harmonization.core.io_export import HarmonizedOutputManager
from omop_etl.infra.run_context import RunContext

# todo: implement cli later
# app = typer.Typer(add_completion=True)
#
# log = getLogger(__name__)
#
#
# def process_impress(file: Path) -> HarmonizedData:
#     df = pl.read_csv(file)
#     return ImpressHarmonizer(df, trial_id="IMPRESS").process()
#
#
# def process_drup(file: Path) -> HarmonizedData:
#     raise NotImplementedError
#
#
# @app.command()
# def impress(file: Path):
#     _ = process_impress(file)
#     typer.echo(f"Harmonized {file}")
#
#
# def main():
#     app()
#
#


def drup_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def impress_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def process_impress(file: Path) -> HarmonizedData:
    data = impress_data(file)
    harmonizer = ImpressHarmonizer(data, trial_id="IMPRESS")
    return harmonizer.process()


# def process_drup(file: Path) -> HarmonizedData:
#     data = drup_data(file)
#     harmonizer = DrupHarmonizer(data=data, trial_id="DRUP")
#     return harmonizer.process()


def run_harmonization_test(input_csv: Path, outdir: Path, trial: str = "IMPRESS") -> dict:
    """
    End-to-end test of the harmonization module on a single CSV.
    Returns a dict with paths and quick stats.
    """
    outdir.mkdir(parents=True, exist_ok=True)

    # set up service + context
    svc = HarmonizationService(out_manager=HarmonizedOutputManager(base_dir=outdir))
    ctx = RunContext.create(trial)

    # run (both wide + normalized)
    hd = svc.run(
        trial=trial,
        input_path=input_csv,
        output_format="csv",
        output_dir=outdir,  # treated as a base directory; service will create a dated run subdir
        write_wide=True,
        write_normalized=True,
        ctx=ctx,
    )

    # resolve the actual run directory
    run_dir = outdir / trial.lower() / f"{ctx.timestamp}_{ctx.run_id}"

    # expected files
    wide_file = run_dir / "data_harmonized_wide.csv"
    wide_manifest = run_dir / "manifest_harmonized_wide.json"
    norm_manifest = run_dir / "manifest_harmonized_norm.json"

    # basic assertions / checks
    if not wide_file.exists():
        raise AssertionError(f"Wide file not found: {wide_file}")
    if not wide_manifest.exists():
        raise AssertionError(f"Wide manifest not found: {wide_manifest}")
    if not norm_manifest.exists():
        raise AssertionError(f"Normalized manifest not found: {norm_manifest}")

    # load some quick stats (optional)
    wide_df = pl.read_csv(wide_file)
    with norm_manifest.open("r", encoding="utf-8") as fp:
        norm_meta = json.load(fp)

    # collect normalized table files from manifest
    normalized_files = {name: Path(meta.get("file")) for name, meta in norm_meta.get("tables", {}).items() if meta.get("file")}
    if not normalized_files:
        raise AssertionError("No normalized tables were written (manifest had no table files).")

    # quick check: “patients” table is commonly present
    patients_csv = normalized_files.get("patients")
    if patients_csv and not patients_csv.exists():
        raise AssertionError(f"Patients table expected but missing: {patients_csv}")

    return {
        "run_dir": run_dir,
        "wide_file": wide_file,
        "wide_rows": wide_df.height,
        "wide_cols": wide_df.width,
        "wide_manifest": wide_manifest,
        "normalized_manifest": norm_manifest,
        "normalized_files": normalized_files,
        "num_patients_in_memory": len(hd.patients),
    }


if __name__ == "__main__":
    impress_150_file = Path(__file__).parents[3] / ".data" / "preprocessing" / "impress" / "20250909T144845Z_19d79919" / "data_preprocessed.csv"
    run_harmonization_test(input_csv=Path(impress_150_file), trial="IMPRESS", outdir=Path(".data/harmonized"))
