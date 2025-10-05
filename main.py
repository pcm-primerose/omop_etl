from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure
from omop_etl.preprocessing.api import make_ecrf_config, PreprocessService
from omop_etl.preprocessing.core.models import PreprocessingRunOptions, PreprocessResult


def run_pipeline(preprocessing_input: Path, base_root: Path, trial: str = "IMPRESS") -> HarmonizedData:
    """
    End-to-end test run on synthetic data for implemented modules.
    """
    base_root.mkdir(parents=True, exist_ok=True)

    # set up configs & meta
    run_options = PreprocessingRunOptions(filter_valid_cohort=True, combine_key="SubjectId")
    ecrf_config = make_ecrf_config(trial=trial)
    meta = RunMetadata.create(trial)

    # run preprocessing
    preprocessor = PreprocessService(outdir=base_root, layout=Layout.TRIAL_RUN)
    preprocessing_result: PreprocessResult = preprocessor.run(
        trial=trial,
        input_path=preprocessing_input,
        config=ecrf_config,
        run_options=run_options,
        formats="csv",
        meta=meta,
    )

    # run harmonization
    harmonizer = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_RUN)
    harmonized_result: HarmonizedData = harmonizer.run(
        trial=trial,
        input_path=preprocessing_result.output_path.data_file,
        formats="csv",
        write_wide=True,
        meta=meta,
    )

    return harmonized_result


if __name__ == "__main__":
    configure(level="DEBUG")
    impress_150_file = Path("/Users/gabriebs/projects/omop_etl/.data/synthetic/impress_150")
    run_pipeline(preprocessing_input=impress_150_file, trial="IMPRESS", base_root=Path(".data/test_run"))
