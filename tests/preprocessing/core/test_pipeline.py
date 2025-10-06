import pytest
import polars as pl
from unittest.mock import Mock

from omop_etl.preprocessing.core.pipeline import PreprocessingPipeline
from omop_etl.preprocessing.core.models import (
    EcrfConfig,
    SheetConfig,
    PreprocessResult,
    OutputPath,
    PreprocessingRunOptions,
)


@pytest.fixture
def sample_config():
    return EcrfConfig(
        trial="test_trial",
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
        ],
    )


def test_pipeline_with_real_components(tmp_path, sample_config):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "data_subjects.csv").write_text(
        "Header\nSubjectId,Age\nA001,25\nA002,30\n",
    )
    (input_dir / "data_visits.csv").write_text(
        "Header\nSubjectId,VisitDate\nA001,2023-01-01\nA002,2023-01-02\n",
    )

    mock_processor = Mock()
    processed_df = pl.DataFrame(
        {"SubjectId": ["TEST-A001", "TEST-A002"], "Trial": ["TEST", "TEST"], "Age": [25, 30]},
    )
    mock_processor.return_value = processed_df

    mock_output_manager = Mock()
    mock_output_path = OutputPath(
        data_file=tmp_path / "output.csv",
        manifest_file=tmp_path / "manifest.json",
        log_file=tmp_path / "output.log",
        directory=tmp_path,
        format="csv",
    )
    mock_output_manager.write.return_value = mock_output_path

    pipeline = PreprocessingPipeline(
        "impress",
        sample_config,
        mock_output_manager,
        processor=mock_processor,
    )

    run_opts = PreprocessingRunOptions()

    result = pipeline.run(input_dir, run_options=run_opts, formats=["csv"])

    assert isinstance(result, PreprocessResult)
    assert result.rows == 2
    assert result.columns == 3

    # verify processor was called with the combined df
    mock_processor.assert_called_once()
    combined_data = mock_processor.call_args[0][0]
    assert "subjects_Age" in combined_data.columns or "Age" in combined_data.columns
    assert combined_data.height == 4
