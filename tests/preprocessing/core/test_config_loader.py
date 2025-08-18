import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest


from omop_etl.preprocessing.core.config_loader import (
    load_ecrf_config,
    available_trials,
    _validate,
)


@pytest.fixture
def custom_config_file():
    config_data = {
        "dm": ["SubjectId", "DMAGE", "BRTHDAT"],
        "coh": ["SubjectId", "COHORTNAME", "EventDate", "ICD10COD"],
    }
    return config_data


def test_load_custom_config(custom_config_file):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json5", delete=False) as tmp:
        json.dump(custom_config_file, tmp)
        tmp_path = Path(tmp.name)
        tmp.flush()  # write to disk

        result = load_ecrf_config("any_trial", custom_config_path=tmp_path)
        assert result == custom_config_file
        tmp_path.unlink()


def test_custom_config_not_found(custom_config_file):
    non_existent_path = Path("/non_existent/path.json5")
    with pytest.raises(FileNotFoundError, match="Custom config file not found"):
        load_ecrf_config("any_trial", custom_config_path=non_existent_path)


def test_custom_config_invalid_json(custom_config_file):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json5", delete=False) as tmp:
        tmp.write("{ invalid json }")
        tmp_path = Path(tmp.name)
        tmp.flush()
        with pytest.raises(Exception):
            load_ecrf_config("any_trial", custom_config_path=tmp_path)
        tmp_path.unlink()


def test_load_packaged_config_success():
    config_data = {"test": ["col1", "col2"]}

    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / "test_trial.json5"
        config_file.write_text(json.dumps(config_data))

        with patch("omop_etl.preprocessing.core.config_loader._BASE", Path(temp_dir)):
            result = load_ecrf_config("test_trial")
            assert result == config_data


def test_trial_name_case_insensitive():
    config_data = {"test": ["col1", "col2"]}

    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / "TEST_trial.json5"
        config_file.write_text(json.dumps(config_data))

        with patch("omop_etl.preprocessing.core.config_loader._BASE", Path(temp_dir)):
            result = load_ecrf_config("test_trial")
            assert result == config_data


def test_load_packaged_config_not_found():
    config_data = {"test": ["col1", "col2"]}

    with tempfile.TemporaryDirectory() as temp_dir:
        config_file_1 = Path(temp_dir) / "test_trial_1"
        config_file_1.write_text(json.dumps(config_data))

        with patch("omop_etl.preprocessing.core.config_loader._BASE", Path(temp_dir)):
            with pytest.raises(FileNotFoundError):
                load_ecrf_config("nonexistent")


@patch("omop_etl.preprocessing.core.config_loader._BASE")
def test_available_trials_returns_sorted_list(mock_base):
    config_data = {"test": ["col1", "col2"]}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        (temp_path / "test_trial_1.json5").write_text(json.dumps(config_data))
        (temp_path / "test_trial_2.json5").write_text(json.dumps(config_data))
        (temp_path / "test_trial_3.json5").write_text(json.dumps(config_data))
        (temp_path / "not_json.txt").write_text("text")

        mock_base.iterdir.return_value = temp_path.iterdir()

        result = available_trials()
        expected = ["test_trial_1", "test_trial_2", "test_trial_3"]
        assert result == expected


@patch("omop_etl.preprocessing.core.config_loader._BASE")
def test_available_trials_empty_directory(mock_base):
    mock_base.iterdir.return_value = []
    result = available_trials()
    assert result == []


def test_validate_valid_config():
    valid_config = {
        "sheet1": ["col1", "col2"],
        "sheet2": ["col3", "col4", "col5"],
        "sheet3": [],
    }
    result = _validate(valid_config)
    assert result == valid_config


def test_validate_empty_config():
    empty_config = {}
    result = _validate(empty_config)
    assert result == empty_config


def test_validate_non_list_values():
    invalid_configs = [
        {"sheet1": "not_a_list"},
        {"sheet1": ["valid"], "sheet2": {"invalid": "dict"}},
        {"sheet1": ["valid"], "sheet2": 42},
        {"sheet1": ["valid"], "sheet2": None},
    ]

    for invalid_config in invalid_configs:
        with pytest.raises(ValueError, match="Config must be a mapping"):
            _validate(invalid_config)
