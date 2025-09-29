import json
import os
from pathlib import Path
from unittest.mock import patch
import pytest
import polars as pl

from omop_etl.preprocessing.core.io_export import OutputManager
from omop_etl.preprocessing.core.models import RunContext


@pytest.fixture
def sample_dataframe():
    return pl.DataFrame(
        {
            "SubjectId": ["A_001", "A_002", "A_003"],
            "age": [25, 30, 35],
            "sex": ["M", "F", "M"],
        },
    )


@pytest.fixture
def run_context():
    return RunContext(trial="test_trial", timestamp="20231201_143000", run_id="abc123")


@pytest.fixture
def output_manager(tmp_path):
    return OutputManager(base_dir=tmp_path / "outputs")


class TestOutputPathResolution:
    """Test resolving of output paths"""

    def test_explicit_file_path_with_extension(self, output_manager, run_context, tmp_path):
        output_file = tmp_path / "my_output.csv"

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        assert output_path.data_file == output_file
        assert output_path.format == "csv"
        assert output_path.manifest_file.name.startswith("manifest_data_my_output")
        assert output_path.log_file.name.startswith("data_my_output")
        assert output_path.directory == tmp_path

    def test_explicit_file_path_without_extension(self, output_manager, run_context, tmp_path):
        output_file = tmp_path / "some_output"

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file, fmt="parquet")

        # file without extension that doesn't exist,
        # falls back to default base directory (not treated as custom directory)
        expected_dir = output_manager.base_dir / "test_trial" / "20231201_143000_abc123"
        assert output_path.directory == expected_dir
        assert output_path.format == "parquet"
        assert output_path.data_file == expected_dir / "data_preprocessed.parquet"

    def test_path_without_extension_but_exists_as_directory(self, output_manager, run_context, tmp_path):
        output_dir = tmp_path / "some_output"
        output_dir.mkdir()

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_dir, fmt="parquet")

        expected_dir = output_dir / "test_trial" / "20231201_143000_abc123"
        assert output_path.directory == expected_dir
        assert output_path.format == "parquet"

    def test_directory_path_creates_structured_layout(self, output_manager, run_context, tmp_path):
        output_dir = tmp_path / "custom_output"
        output_dir.mkdir()

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_dir)

        expected_dir = output_dir / "test_trial" / "20231201_143000_abc123"
        assert output_path.directory == expected_dir
        assert output_path.data_file == expected_dir / "data_preprocessed.csv"
        assert output_path.manifest_file == expected_dir / "manifest_preprocessed.json"
        assert output_path.log_file == expected_dir / "preprocessed.log"

    def test_nonexistent_directory_uses_default_base(self, output_manager, run_context, tmp_path):
        nonexistent_dir = tmp_path / "does_not_exist"

        output_path = output_manager.resolve_output_path(ctx=run_context, output=nonexistent_dir)

        expected_dir = output_manager.base_dir / "test_trial" / "20231201_143000_abc123"
        assert output_path.directory == expected_dir

    def test_no_output_path_uses_default_structure(self, output_manager, run_context):
        output_path = output_manager.resolve_output_path(ctx=run_context)

        expected_dir = output_manager.base_dir / "test_trial" / "20231201_143000_abc123"
        assert output_path.directory == expected_dir
        assert output_path.format == "csv"  # default format

    def test_custom_filename_stem(self, output_manager, run_context):
        output_path = output_manager.resolve_output_path(ctx=run_context, filename_stem="custom_name")

        assert "custom_name" in output_path.data_file.name
        assert "custom_name" in output_path.manifest_file.name
        assert "custom_name" in output_path.log_file.name

    def test_format_override(self, output_manager, run_context):
        output_path = output_manager.resolve_output_path(ctx=run_context, fmt="tsv")

        assert output_path.format == "tsv"
        assert output_path.data_file.suffix == ".tsv"


class TestFileWriting:
    """Test DataFrame writing to supported formats"""

    def test_write_csv_format(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.csv"
        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_dataframe(sample_dataframe, output_path)

        assert output_path.data_file.exists()

        df_read = pl.read_csv(output_path.data_file)
        assert df_read.shape == sample_dataframe.shape
        assert df_read.columns == sample_dataframe.columns

    def test_write_tsv_format(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.tsv"
        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_dataframe(sample_dataframe, output_path)

        assert output_path.data_file.exists()

        df_read = pl.read_csv(output_path.data_file, separator="\t")
        assert df_read.shape == sample_dataframe.shape

    def test_write_parquet_format(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.parquet"
        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_dataframe(sample_dataframe, output_path)

        assert output_path.data_file.exists()

        df_read = pl.read_parquet(output_path.data_file)
        assert df_read.shape == sample_dataframe.shape

    def test_write_unsupported_format_raises_error(self, output_manager, run_context, sample_dataframe, tmp_path):
        with pytest.raises(ValueError, match="Unsupported format 'xlsx'"):
            output_manager.resolve_output_path(ctx=run_context, output=None, fmt="xlsx")


class TestManifestCreation:
    """Test manifest file creation and content"""

    def test_manifest_contains_required_fields(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.csv"
        input_path = tmp_path / "input.csv"
        input_path.touch()

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_manifest(
            output_path=output_path,
            ctx=run_context,
            df=sample_dataframe,
            input_path=input_path,
            log_file_created=True,
            options={"test_option": "test_value"},
        )

        assert output_path.manifest_file.exists()

        manifest = json.loads(output_path.manifest_file.read_text())

        assert manifest["trial"] == "test_trial"
        assert manifest["timestamp"] == "20231201_143000"
        assert manifest["run_id"] == "abc123"
        assert manifest["format"] == "csv"
        assert manifest["statistics"]["rows"] == 3
        assert manifest["statistics"]["columns"] == 3
        assert manifest["options"]["test_option"] == "test_value"
        assert str(input_path.absolute()) in manifest["input"]

    def test_manifest_schema_includes_all_columns(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.csv"
        input_path = tmp_path / "input.csv"
        input_path.touch()

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_manifest(
            output_path=output_path,
            ctx=run_context,
            df=sample_dataframe,
            input_path=input_path,
        )

        manifest = json.loads(output_path.manifest_file.read_text())
        schema = manifest["schema"]

        assert "SubjectId" in schema
        assert "age" in schema
        assert "sex" in schema
        assert len(schema) == 3

    def test_manifest_without_log_file(self, output_manager, run_context, sample_dataframe, tmp_path):
        output_file = tmp_path / "test.csv"
        input_path = tmp_path / "input.csv"
        input_path.touch()

        output_path = output_manager.resolve_output_path(ctx=run_context, output=output_file)

        OutputManager.write_manifest(
            output_path=output_path,
            ctx=run_context,
            df=sample_dataframe,
            input_path=input_path,
            log_file_created=False,
        )

        manifest = json.loads(output_path.manifest_file.read_text())
        assert manifest["log_file"] is None


class TestCompleteWriteOperation:
    """Test the write operation with components"""

    def test_write_creates_all_files(self, output_manager, run_context, sample_dataframe, tmp_path):
        input_path = tmp_path / "input.csv"
        input_path.touch()

        with patch.dict(os.environ, {"DISABLE_LOG_FILE": "1"}):
            output_path = output_manager.write(
                df=sample_dataframe,
                ctx=run_context,
                input_path=input_path,
                options={"filter_cohort": True},
            )

        assert output_path.data_file.exists()
        assert output_path.manifest_file.exists()
        df_read = pl.read_csv(output_path.data_file)
        assert df_read.shape == sample_dataframe.shape

    def test_write_with_custom_output_path(self, output_manager, run_context, sample_dataframe, tmp_path):
        input_path = tmp_path / "input.csv"
        input_path.touch()
        custom_output = tmp_path / "custom" / "output.parquet"

        with patch.dict(os.environ, {"DISABLE_LOG_FILE": "1"}):
            output_path = output_manager.write(
                df=sample_dataframe,
                ctx=run_context,
                input_path=input_path,
                output=custom_output,
            )

        assert output_path.data_file == custom_output
        assert output_path.format == "parquet"
        assert output_path.data_file.exists()

    def test_write_with_log_file_enabled(self, output_manager, run_context, sample_dataframe, tmp_path):
        input_path = tmp_path / "input.csv"
        input_path.touch()

        with patch.dict(os.environ, {}, clear=True):
            output_path = output_manager.write(df=sample_dataframe, ctx=run_context, input_path=input_path)

        # log file should be created
        assert output_path.log_file is not None
        assert output_path.log_file.is_file()
        assert output_path.log_file.parent.exists()


class TestFormatHandling:
    """Test format inference and normalization"""

    def test_infer_format_from_extensions(self, output_manager):
        assert output_manager._infer_format(Path("file.csv")) == "csv"
        assert output_manager._infer_format(Path("file.tsv")) == "tsv"
        assert output_manager._infer_format(Path("file.parquet")) == "parquet"
        assert output_manager._infer_format(Path("file.txt")) == "tsv"  # txt to tsv
        assert output_manager._infer_format(Path("file.unknown")) is None

    def test_normalize_format_valid_formats(self, output_manager):
        assert output_manager._normalize_format("CSV") == "csv"
        assert output_manager._normalize_format("tsv") == "tsv"
        assert output_manager._normalize_format("PARQUET") == "parquet"
        assert output_manager._normalize_format("txt") == "tsv"
        assert output_manager._normalize_format(None) == "csv"  # default

    def test_normalize_format_invalid_raises_error(self, output_manager):
        with pytest.raises(ValueError, match="Unsupported format 'xml'"):
            output_manager._normalize_format("xml")


class TestEnvironmentVariableHandling:
    """Test environment variable configuration"""

    def test_disable_log_file_environment_variable(self, output_manager, run_context, sample_dataframe, tmp_path):
        input_path = tmp_path / "input.csv"
        input_path.touch()

        with patch.dict(os.environ, {"DISABLE_LOG_FILE": "1"}):
            output_path = output_manager.write(df=sample_dataframe, ctx=run_context, input_path=input_path)

        # no log file in manifest
        manifest = json.loads(output_path.manifest_file.read_text())
        assert manifest["log_file"] is None

    def test_log_file_json_environment_variable(self, output_manager, run_context, sample_dataframe, tmp_path):
        input_path = tmp_path / "input.csv"
        input_path.touch()

        with patch.dict(os.environ, {"LOG_FILE_JSON": "1"}):
            output_path = output_manager.write(df=sample_dataframe, ctx=run_context, input_path=input_path)

        assert output_path.data_file.exists()
