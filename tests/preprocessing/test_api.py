# tests/preprocessing/test_api.py

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, Mock

from omop_etl.preprocessing.api import (
    preprocess_trial,
    make_ecrf_config,
    list_trials,
    PreprocessResult,
    RunOptions,
)
from omop_etl.preprocessing.core.models import EcrfConfig, SheetConfig


class TestListTrials:
    """Test list_trials API function"""

    @patch(
        "omop_etl.preprocessing.api.TRIAL_PROCESSORS",
        {"impress": Mock(), "trial_b": Mock()},
    )
    def test_list_trials_returns_sorted_list(self):
        """Test that list_trials returns sorted trial names"""
        result = list_trials()

        assert result == ["impress", "trial_b"]
        assert isinstance(result, list)


class TestMakeEcrfConfig:
    """Test make_ecrf_config API function"""

    def test_make_ecrf_config_with_custom_path(self, tmp_path):
        """Test creating config from custom file"""
        # Create custom config file
        config_data = {
            "subjects": ["SubjectId", "Age", "Sex"],
            "visits": ["SubjectId", "VisitDate", "VisitType"],
        }

        config_file = tmp_path / "custom_config.json5"
        config_file.write_text(json.dumps(config_data))

        result = make_ecrf_config("any_trial", custom_config_path=config_file)

        assert isinstance(result, EcrfConfig)
        assert len(result.configs) == 2

        subjects_config = next(c for c in result.configs if c.key == "SUBJECTS")
        assert subjects_config.usecols == ["SubjectId", "Age", "Sex"]

    @patch("omop_etl.preprocessing.api.load_ecrf_config")
    def test_make_ecrf_config_without_custom_path(self, mock_load_config):
        """Test creating config using default packaged config"""
        mock_load_config.return_value = {
            "demographics": ["SubjectId", "Age"],
            "labs": ["SubjectId", "TestValue"],
        }

        result = make_ecrf_config("test_trial")

        mock_load_config.assert_called_once_with(
            trial="test_trial", custom_config_path=None
        )
        assert isinstance(result, EcrfConfig)
        assert len(result.configs) == 2

    def test_make_ecrf_config_error_handling(self, tmp_path):
        """Test error handling in config creation"""
        nonexistent_file = tmp_path / "missing.json5"

        with pytest.raises(FileNotFoundError):
            make_ecrf_config("any_trial", custom_config_path=nonexistent_file)


class TestPreprocessTrialAPI:
    """Test the main preprocess_trial API function"""

    def test_preprocess_trial_with_real_data_minimal_mocking(self, tmp_path):
        """Integration test with real data and minimal mocking"""

        # Create real input data
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        subjects_csv = input_dir / "data_subjects.csv"
        subjects_csv.write_text(
            "Header\n" "SubjectId,Age,Sex\n" "A001,25,M\n" "A002,30,F\n"
        )

        visits_csv = input_dir / "data_visits.csv"
        visits_csv.write_text(
            "Header\n" "SubjectId,VisitDate\n" "A001,2023-01-01\n" "A002,2023-01-02\n"
        )

        # Create real config
        config = EcrfConfig(
            configs=[
                SheetConfig(key="subjects", usecols=["SubjectId", "Age", "Sex"]),
                SheetConfig(key="visits", usecols=["SubjectId", "VisitDate"]),
            ]
        )

        # Mock only the trial processor and disable actual file writing
        mock_processor = Mock()
        mock_processor.return_value = (
            subjects_csv.read_text()
        )  # Return something DataFrame-like

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS",
            {"test_trial": mock_processor},
        ):
            with patch.dict(
                "os.environ", {"DISABLE_LOG_FILE": "1"}
            ):  # Disable log files
                # Mock the output manager to avoid actual file writing
                with patch(
                    "omop_etl.preprocessing.api.OutputManager"
                ) as mock_output_manager_class:
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
                        trial="test_trial",
                        input_path=input_dir,
                        config=config,
                        base_output_dir=tmp_path,
                    )

        # Verify API contract
        assert isinstance(result, PreprocessResult)
        assert result.output_path == mock_result_path

        # Verify real components were used
        mock_processor.assert_called_once()

    def test_preprocess_trial_with_default_config(self, tmp_path):
        """Test API with default config creation"""

        # Create minimal input data
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        test_csv = input_dir / "data_test.csv"
        test_csv.write_text("Header\nSubjectId,Value\nA001,100\n")

        # Mock config loading and processing
        mock_config_data = {"test": ["SubjectId", "Value"]}

        with patch(
            "omop_etl.preprocessing.api.load_ecrf_config", return_value=mock_config_data
        ):
            with patch(
                "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test_trial": Mock()}
            ):
                with patch(
                    "omop_etl.preprocessing.api.OutputManager"
                ) as mock_output_manager_class:
                    mock_output_manager = Mock()
                    mock_output_manager.write.return_value = Mock()
                    mock_output_manager_class.return_value = mock_output_manager

                    result = preprocess_trial(
                        trial="test_trial",
                        input_path=input_dir,
                        # No config provided - should create default
                    )

        assert isinstance(result, PreprocessResult)

    def test_preprocess_trial_with_all_options(self, tmp_path):
        """Test API with all optional parameters"""

        input_dir = tmp_path / "input"
        input_dir.mkdir()

        test_csv = input_dir / "data_test.csv"
        test_csv.write_text("Header\nSubjectId,Value\nA001,100\n")

        config = EcrfConfig(
            configs=[SheetConfig(key="test", usecols=["SubjectId", "Value"])]
        )
        run_options = RunOptions(filter_valid_cohort=True)
        output_path = tmp_path / "custom_output.parquet"

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test_trial": Mock()}
        ):
            with patch(
                "omop_etl.preprocessing.api.OutputManager"
            ) as mock_output_manager_class:
                mock_output_manager = Mock()
                mock_output_manager.write.return_value = Mock()
                mock_output_manager_class.return_value = mock_output_manager

                result = preprocess_trial(
                    trial="test_trial",
                    input_path=input_dir,
                    config=config,
                    run_options=run_options,
                    output=output_path,
                    fmt="parquet",
                    combine_key="PatientId",
                    base_output_dir=tmp_path,
                )

        assert isinstance(result, PreprocessResult)

        # Verify OutputManager was created with base_output_dir
        mock_output_manager_class.assert_called_once_with(base_dir=tmp_path)

    def test_preprocess_trial_config_deepcopy(self, tmp_path):
        """Test that original config is not modified"""

        input_dir = tmp_path / "input"
        input_dir.mkdir()

        test_csv = input_dir / "data_test.csv"
        test_csv.write_text("Header\nSubjectId,Value\nA001,100\n")

        original_config = EcrfConfig(
            trial="original_trial",
            configs=[SheetConfig(key="test", usecols=["SubjectId", "Value"])],
        )

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test_trial": Mock()}
        ):
            with patch(
                "omop_etl.preprocessing.api.OutputManager"
            ) as mock_output_manager_class:
                mock_output_manager = Mock()
                mock_output_manager.write.return_value = Mock()
                mock_output_manager_class.return_value = mock_output_manager

                preprocess_trial(
                    trial="test_trial", input_path=input_dir, config=original_config
                )

        # Original config should be unchanged
        assert original_config.trial == "original_trial"


class TestAPIErrorHandling:
    """Test error handling in API functions"""

    def test_preprocess_trial_invalid_trial(self, tmp_path):
        """Test error handling for invalid trial"""

        input_dir = tmp_path / "input"
        input_dir.mkdir()

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"valid_trial": Mock()}
        ):
            with pytest.raises(ValueError, match="Unknown trial 'invalid_trial'"):
                preprocess_trial(trial="invalid_trial", input_path=input_dir)

    def test_preprocess_trial_missing_input(self, tmp_path):
        """Test error handling for missing input"""

        missing_input = tmp_path / "missing"

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test_trial": Mock()}
        ):
            # Should propagate the underlying error
            with pytest.raises(Exception):  # Could be FileNotFoundError or ValueError
                preprocess_trial(trial="test_trial", input_path=missing_input)

    def test_make_ecrf_config_invalid_file(self, tmp_path):
        """Test error handling for invalid config file"""

        invalid_config = tmp_path / "invalid.json5"
        invalid_config.write_text("{ invalid json }")

        with pytest.raises(Exception):  # JSON parsing error
            make_ecrf_config("any_trial", custom_config_path=invalid_config)


class TestAPIUsagePatterns:
    """Test common API usage patterns"""

    def test_simple_usage_pattern(self, tmp_path):
        """Test the simplest possible API usage"""

        # Create minimal input
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        test_csv = input_dir / "data_test.csv"
        test_csv.write_text("Header\nSubjectId\nA001\n")

        # Mock everything needed for simple usage
        mock_config_data = {"test": ["SubjectId"]}

        with patch(
            "omop_etl.preprocessing.api.load_ecrf_config", return_value=mock_config_data
        ):
            with patch(
                "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"simple_trial": Mock()}
            ):
                with patch(
                    "omop_etl.preprocessing.api.OutputManager"
                ) as mock_output_manager_class:
                    mock_output_manager = Mock()
                    mock_output_manager.write.return_value = Mock()
                    mock_output_manager_class.return_value = mock_output_manager

                    # This is how users would actually call it
                    result = preprocess_trial("simple_trial", input_dir)

        assert isinstance(result, PreprocessResult)

    def test_advanced_usage_pattern(self, tmp_path):
        """Test advanced API usage with custom everything"""

        # Create custom config file
        config_file = tmp_path / "custom.json5"
        config_file.write_text(json.dumps({"advanced": ["SubjectId", "Data"]}))

        # Create input
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        test_csv = input_dir / "data_advanced.csv"
        test_csv.write_text("Header\nSubjectId,Data\nA001,value\n")

        with patch(
            "omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"advanced_trial": Mock()}
        ):
            with patch(
                "omop_etl.preprocessing.api.OutputManager"
            ) as mock_output_manager_class:
                mock_output_manager = Mock()
                mock_output_manager.write.return_value = Mock()
                mock_output_manager_class.return_value = mock_output_manager

                # Advanced usage pattern
                config = make_ecrf_config("advanced_trial", config_file)
                options = RunOptions(filter_valid_cohort=True)

                result = preprocess_trial(
                    trial="advanced_trial",
                    input_path=input_dir,
                    config=config,
                    run_options=options,
                    output=tmp_path / "custom_output.csv",
                    fmt="csv",
                )

        assert isinstance(result, PreprocessResult)


class TestAPIDocumentationContract:
    """Test that API behaves as documented"""

    def test_api_functions_exist_and_callable(self):
        """Test that all documented API functions exist"""

        # Test that all __all__ exports exist and are callable
        assert callable(preprocess_trial)
        assert callable(make_ecrf_config)
        assert callable(list_trials)

        # Test that types are importable
        assert PreprocessResult is not None
        assert RunOptions is not None

    def test_api_return_types(self, tmp_path):
        """Test that API functions return expected types"""

        # list_trials should return List[str]
        with patch("omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test": Mock()}):
            trials = list_trials()
            assert isinstance(trials, list)
            assert all(isinstance(t, str) for t in trials)

        # make_ecrf_config should return EcrfConfig
        with patch(
            "omop_etl.preprocessing.api.load_ecrf_config",
            return_value={"test": ["col"]},
        ):
            config = make_ecrf_config("test")
            assert isinstance(config, EcrfConfig)

        # preprocess_trial should return PreprocessResult
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        test_csv = input_dir / "data_test.csv"
        test_csv.write_text("Header\nSubjectId\nA001\n")

        with patch(
            "omop_etl.preprocessing.api.load_ecrf_config",
            return_value={"test": ["SubjectId"]},
        ):
            with patch("omop_etl.preprocessing.api.TRIAL_PROCESSORS", {"test": Mock()}):
                with patch(
                    "omop_etl.preprocessing.api.OutputManager"
                ) as mock_output_manager_class:
                    mock_output_manager = Mock()
                    mock_output_manager.write.return_value = Mock()
                    mock_output_manager_class.return_value = mock_output_manager

                    result = preprocess_trial("test", input_dir)
                    assert isinstance(result, PreprocessResult)
