from unittest.mock import patch, Mock
import polars as pl
from omop_etl.preprocessing.api import PreprocessService, PreprocessResult
from omop_etl.preprocessing.core.models import EcrfConfig, SheetConfig


def test_preprocess_trial_with_real_data(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "data_subjects.csv").write_text(
        "Header\nSubjectId,Age,Sex\nA001,200,M\nA002,30,F\n",
    )
    (input_dir / "data_visits.csv").write_text(
        "Header\nSubjectId,VisitDate\nA001,2023-01-01\nA002,2023-01-02\n",
    )
    (input_dir / "data_ECOG.csv").write_text(
        "Header\nSubjectId,EventId\nA001,V00\nA002,S01\n",
    )

    config = EcrfConfig(
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age", "Sex"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
            SheetConfig(key="ECOG", usecols=["SubjectId", "EventId"]),
        ],
    )

    # mock processor API resolves to
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

    # patch the API's imported processor (_resolve_processor)
    with patch("omop_etl.preprocessing.api._resolve_processor", return_value=mock_processor):
        with patch.dict("os.environ", {"DISABLE_LOG_FILE": "1"}):
            # stub OutputManager used by API
            with patch("omop_etl.preprocessing.api.OutputManager") as mock_out_mgr_cls:
                mock_out_mgr = Mock()
                mock_result_path = Mock()
                mock_result_path.data_file = tmp_path / "output.csv"
                mock_result_path.manifest_file = tmp_path / "manifest.json"
                mock_result_path.log_file = tmp_path / "output.log"
                mock_result_path.directory = tmp_path
                mock_result_path.format = "csv"
                mock_out_mgr.write.return_value = mock_result_path
                mock_out_mgr_cls.return_value = mock_out_mgr

                pps = PreprocessService(outdir=tmp_path)
                result = pps.run(
                    trial="impress",
                    input_path=input_dir,
                    config=config,
                )

    assert isinstance(result, PreprocessResult)
    assert result.output_path == mock_result_path
    assert result.rows == 2
    assert result.columns == 6
    assert result.context.trial == "impress"
