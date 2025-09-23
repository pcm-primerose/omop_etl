from unittest.mock import patch, Mock
import polars as pl
from omop_etl.preprocessing.api import preprocess_trial, PreprocessResult
from omop_etl.preprocessing.core.models import EcrfConfig, SheetConfig


def test_preprocess_trial_with_real_data(tmp_path):
    # create data
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    subjects_csv = input_dir / "data_subjects.csv"
    subjects_csv.write_text("Header\n" "SubjectId,Age,Sex\n" "A001,200,M\n" "A002,30,F\n")

    visits_csv = input_dir / "data_visits.csv"
    visits_csv.write_text("Header\n" "SubjectId,VisitDate\n" "A001,2023-01-01\n" "A002,2023-01-02\n")

    ecog_csv = input_dir / "data_ECOG.csv"
    ecog_csv.write_text("Header\n" "SubjectId,EventId\n" "A001,V00\n" "A002,S01\n")

    # create real config
    config = EcrfConfig(
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age", "Sex"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
            SheetConfig(key="ECOG", usecols=["SubjectId", "EventId"]),
        ],
    )

    # mock trial processor
    mock_processor = Mock()
    processed_df = pl.DataFrame(
        {
            "SubjectId": ["TEST-A001", "TEST-A002"],
            "Trial": ["TEST", "TEST"],
            "Age": [25, 200],
            "Sex": ["M", "F"],
            "ECOG_EventId": ["", "V00"],
            "visits_VisitDate": ["2023-01-01", "2023-01-02"],
        },
    )
    mock_processor.return_value = processed_df

    # patch the registry directly
    with patch(
        "omop_etl.preprocessing.core.registry.TRIAL_PROCESSORS",
        {"impress": mock_processor},
    ):
        with patch.dict("os.environ", {"DISABLE_LOG_FILE": "1"}):
            # mock the output manager
            with patch("omop_etl.preprocessing.api.OutputManager") as mock_output_manager_class:
                mock_output_manager = Mock()
                mock_result_path = Mock()
                mock_result_path.data_file = tmp_path / "output.csv"
                mock_result_path.manifest_file = tmp_path / "manifest.json"
                mock_result_path.log_file = tmp_path / "output.log"
                mock_result_path.directory = tmp_path
                mock_result_path.format = "csv"
                mock_output_manager.write.return_value = mock_result_path
                mock_output_manager_class.return_value = mock_output_manager

                result = preprocess_trial(
                    trial="impress",
                    input_path=input_dir,
                    config=config,
                    base_output_dir=tmp_path,
                )

    # verify API contract
    assert isinstance(result, PreprocessResult)
    assert result.output_path == mock_result_path

    # verify data
    assert result.rows == 2
    assert result.columns == 6
    assert result.context.trial == "impress"
