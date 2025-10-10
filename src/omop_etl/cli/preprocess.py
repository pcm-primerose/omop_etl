from pathlib import Path
from typing import Optional, Literal

from omop_etl.config import IMPRESS_150, DATA_ROOT
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.preprocessing.api import (
    make_ecrf_config,
    PreprocessResult,
    PreprocessService,
)


def run_preprocessing(
    preprocessing_input: Path,
    base_root: Path,
    trial: str = "IMPRESS",
    combine_key: str = "SubjectId",
    output_format: Literal["csv", "tsv", "parquet"] = "csv",
    only_cohort: Optional[bool] = True,
    config: Optional[Path] = None,
) -> PreprocessResult:
    # build runtime options
    base_root.mkdir(parents=True, exist_ok=True)

    # set up configs & meta
    ecrf_config = make_ecrf_config(trial=trial, custom_config_path=config)
    meta = RunMetadata.create(trial)

    # run preprocessing
    preprocessor = PreprocessService(outdir=base_root, layout=Layout.TRIAL_RUN)
    preprocessing_result: PreprocessResult = preprocessor.run(
        trial=trial,
        input_path=preprocessing_input,
        config=ecrf_config,
        formats=output_format,
        meta=meta,
        combine_key=combine_key,
        filter_valid_cohorts=only_cohort,
    )

    return preprocessing_result


if __name__ == "__main__":
    run_preprocessing(preprocessing_input=IMPRESS_150, trial="IMPRESS", base_root=DATA_ROOT, output_format="csv")
