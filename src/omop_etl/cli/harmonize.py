from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure


def run_harmonization(input_csv: Path, base_root: Path, trial: str = "IMPRESS") -> HarmonizedData:
    """
    End-to-end test of the harmonization module on a single CSV.
    Returns a dict with paths and quick stats.
    """
    base_root.mkdir(parents=True, exist_ok=True)

    svc = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_RUN)
    meta = RunMetadata.create(trial)

    hd = svc.run(
        trial=trial,
        input_path=input_csv,
        formats="all",
        write_wide=True,
        write_normalized=True,
        meta=meta,
    )

    return hd


if __name__ == "__main__":
    configure(level="DEBUG")
    impress_150_file = (
        Path(__file__).parents[3] / ".data" / "preprocessing" / "impress" / "d4dbfeaa" / "csv" / "impress_d4dbfeaa_20251003T162634Z_preprocessed.csv"
    )
    run_harmonization(input_csv=Path(impress_150_file), trial="IMPRESS", base_root=Path(".data"))
