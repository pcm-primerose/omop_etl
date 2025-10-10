from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.config import DATA_ROOT, LOG_LEVEL
from omop_etl.infra.utils.find_latest_run import find_latest_run_output
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure_logger


def run_harmonization(input_csv: Path, base_root: Path, trial: str = "IMPRESS") -> HarmonizedData:
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
    configure_logger(level=LOG_LEVEL)

    input_file = find_latest_run_output(
        trial="impress",
        fmt="csv",
        module="preprocessed",
    )

    print(f"input file: {input_file}")

    if not input_file:
        raise FileNotFoundError("No preprocessing output found. Run preprocess.py first.")

    run_harmonization(input_csv=input_file, trial="IMPRESS", base_root=DATA_ROOT)
