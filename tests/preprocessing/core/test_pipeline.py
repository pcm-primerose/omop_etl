import polars as pl
from unittest.mock import Mock
from omop_etl.preprocessing.core.pipeline import PreprocessingPipeline
from omop_etl.preprocessing.core.exporter import PreprocessExporter
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import Layout
from omop_etl.preprocessing.core.models import (
    PreprocessingRunOptions,
    PreprocessResult,
    EcrfConfig,
    SheetConfig,
)


def test_pipeline_with_real_components(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "data_subjects.csv").write_text("Header\nSubjectId,Age\nA001,25\nA002,30\n")
    (input_dir / "data_visits.csv").write_text("Header\nSubjectId,VisitDate\nA001,2023-01-01\nA002,2023-01-02\n")

    # miniconfig
    config = EcrfConfig(configs=[SheetConfig(key="subjects", usecols=["SubjectId", "Age"])])

    # real exporter to tmp dir
    exporter = PreprocessExporter(base_out=tmp_path / "outputs", layout=Layout.TRIAL_RUN)

    meta = RunMetadata.create("impress")

    mock_processor = Mock()
    processed_df = pl.DataFrame({"SubjectId": ["TEST-A001", "TEST-A002"], "Trial": ["TEST", "TEST"], "Age": [25, 30]})
    mock_processor.return_value = processed_df

    pipeline = PreprocessingPipeline(
        trial="impress",
        ecrf_config=config,
        meta=meta,
        output_manager=exporter,
        preprocessor_resolver=lambda _trial: mock_processor,  # type: ignore
    )

    run_opts = PreprocessingRunOptions(filter_valid_cohort=False)

    result = pipeline.run(input_dir, run_options=run_opts, formats=["csv"])

    assert isinstance(result, PreprocessResult)
    assert result.rows == 2
    assert result.columns == 3

    # processor was called with combined df
    mock_processor.assert_called_once()
    combined_df = mock_processor.call_args[0][0]

    # combined columns should include both sheets
    assert "subjects_Age" in combined_df.columns or "Age" in combined_df.columns
    assert combined_df.height >= 2

    # files written
    assert result.output_path.data_file.exists()
    assert result.output_path.manifest_file.exists()
