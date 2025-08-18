import pytest
import polars as pl
from unittest.mock import Mock, patch

from omop_etl.preprocessing.core.pipeline import PreprocessingPipeline
from omop_etl.preprocessing.core.models import (
    EcrfConfig,
    SheetConfig,
    PreprocessResult,
    OutputPath,
)


@pytest.fixture
def sample_config():
    """Sample eCRF configuration"""
    return EcrfConfig(
        trial="test_trial",
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
        ],
    )


def test_pipeline_with_real_components(tmp_path, sample_config):
    """Integration test using real components where possible"""

    # create data
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    subjects_csv = input_dir / "data_subjects.csv"
    subjects_csv.write_text("Header\n" "SubjectId,Age\n" "A001,25\n" "A002,30\n")

    visits_csv = input_dir / "data_visits.csv"
    visits_csv.write_text(
        "Header\n" "SubjectId,VisitDate\n" "A001,2023-01-01\n" "A002,2023-01-02\n"
    )

    # mock trial processor and output manager
    mock_processor = Mock()
    processed_df = pl.DataFrame(
        {
            "SubjectId": ["TEST-A001", "TEST-A002"],
            "Trial": ["TEST", "TEST"],
            "Age": [25, 30],
        }
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

    # run with InputResolver and combine
    with patch(
        "omop_etl.preprocessing.core.pipeline.TRIAL_PROCESSORS",
        {"test_trial": mock_processor},
    ):
        pipeline = PreprocessingPipeline(
            "test_trial", sample_config, mock_output_manager
        )
        result = pipeline.run(input_dir)

    assert isinstance(result, PreprocessResult)
    assert result.rows == 2
    assert result.columns == 3

    # verify processor was called with combined data
    mock_processor.assert_called_once()
    combined_data = mock_processor.call_args[0][0]
    assert "subjects_Age" in combined_data.columns or "Age" in combined_data.columns
    assert combined_data.height == 4
