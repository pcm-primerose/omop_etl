from pathlib import Path

from omop_etl.config import DATA_ROOT, IMPRESS_NON_V600
from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure_logger
from omop_etl.preprocessing.api import make_ecrf_config, PreprocessService
from omop_etl.preprocessing.core.models import PreprocessResult
from omop_etl.semantic_mapping.mapper import SemanticService, BatchQueryResult


def run_pipeline(preprocessing_input: Path, base_root: Path, trial: str = "IMPRESS") -> HarmonizedData:
    """
    End-to-end test run on synthetic data for implemented modules.
    """
    base_root.mkdir(parents=True, exist_ok=True)

    # set up configs & meta
    ecrf_config = make_ecrf_config(trial=trial)
    meta = RunMetadata.create(trial)

    # run preprocessing
    preprocessor = PreprocessService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    preprocessing_result: PreprocessResult = preprocessor.run(
        trial=trial,
        input_path=preprocessing_input,
        config=ecrf_config,
        formats="csv",
        meta=meta,
        combine_key="SubjectId",
        filter_valid_cohorts=True,
    )

    # run harmonization
    harmonizer = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    harmonized_result: HarmonizedData = harmonizer.run(
        trial=trial,
        input_path=preprocessing_result.output_path.data_file,
        formats="csv",
        write_wide=True,
        write_normalized=True,
        meta=meta,
    )

    return harmonized_result


if __name__ == "__main__":
    configure_logger(level="DEBUG")

    harmonized_data = run_pipeline(
        preprocessing_input=IMPRESS_NON_V600,
        trial="IMPRESS",
        base_root=DATA_ROOT,
    )  # .filter(predicate=lambda p: p.cohort_name.lower() in ["braf non-v600", "braf non-v600activating"])

    # testing semantic mapping
    semantic_file = Path(__file__).parent / ".data" / "semantic_mapping" / "mapped" / "braf_non-v600_mapped.csv"
    semantic_service = SemanticService(harmonized_data=harmonized_data, semantic_path=semantic_file)
    semantic_mapped: BatchQueryResult = semantic_service.run()

    # pass this to structural mapper ...
