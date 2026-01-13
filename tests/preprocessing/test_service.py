import polars as pl
from pathlib import Path
from unittest.mock import Mock
from omop_etl.preprocessing.service import PreprocessService
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.preprocessing.core.models import (
    EcrfConfig,
    SheetConfig,
    PreprocessResult,
)


def test_preprocess_trial_with_real_data(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    (input_dir / "data_subjects.csv").write_text(
        "Header\nSubjectId,Age,Sex\nA001,200,M\nA002,30,F\n",
        encoding="utf-8",
    )
    (input_dir / "data_visits.csv").write_text(
        "Header\nSubjectId,VisitDate\nA001,2023-01-01\nA002,2023-01-02\n",
        encoding="utf-8",
    )
    (input_dir / "data_ECOG.csv").write_text(
        "Header\nSubjectId,EventId\nA001,V00\nA002,S01\n",
        encoding="utf-8",
    )

    # miniconfig
    config = EcrfConfig(
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age", "Sex"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
            SheetConfig(key="ECOG", usecols=["SubjectId", "EventId"]),
        ]
    )

    # mock processor
    mock_processor = Mock()
    processed_df = pl.DataFrame(
        {
            "SubjectId": ["TEST-A001", "TEST-A002"],
            "Trial": ["TEST", "TEST"],
            "Age": [25, 200],
            "Sex": ["M", "F"],
            "ECOG_EventId": ["", "V00"],
            "visits_VisitDate": ["2023-01-01", "2023-01-02"],
        }
    )
    # signature: (df_combined, ecrf_config, run_options) to pl.DataFrame
    mock_processor.return_value = processed_df

    meta = RunMetadata.create("impress")

    svc = PreprocessService(
        outdir=tmp_path / "outputs",
        preprocessor_resolver=lambda _: mock_processor,
    )

    result: PreprocessResult = svc.run(
        trial="impress",
        input_path=input_dir,
        meta=meta,
        config=config,
        formats="csv",
        filter_valid_cohorts=False,
    )

    assert isinstance(result, PreprocessResult)
    assert result.rows == processed_df.height
    assert result.columns == processed_df.width
    assert result.context.trial == "impress"

    # check that files are actually written
    out = result.output_path
    assert out.data_file.exists()
    assert out.manifest_file.exists()
    assert out.log_file.parent.exists()
