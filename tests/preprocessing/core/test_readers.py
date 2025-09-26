import pytest
import polars as pl
from unittest.mock import Mock

from omop_etl.preprocessing.core.readers import (
    BaseReader,
    ExcelReader,
    CsvDirectoryReader,
    InputResolver,
)
from omop_etl.preprocessing.core.models import EcrfConfig, SheetConfig


@pytest.fixture
def sample_config():
    return EcrfConfig(
        trial="test_trial",
        configs=[
            SheetConfig(key="subjects", usecols=["SubjectId", "Age", "Sex"]),
            SheetConfig(key="demographics", usecols=["SubjectId", "Race", "Ethnicity"]),
            SheetConfig(key="labs", usecols=["SubjectId", "TestName", "TestValue"]),
        ],
    )


@pytest.fixture
def sample_dataframe():
    return pl.DataFrame(
        {
            "SUBJECTID": ["001", "002", "003"],
            "age": [25, 30, 35],
            "Sex": ["M", "F", "M"],
            "extra_col": ["A", "B", "C"],
        },
    )


class TestBaseReaderNormalization:
    """Test BaseReader normalization methods"""

    def test_normalize_dataframe_case_insensitive_matching(self, sample_dataframe):
        expected_cols = ["SubjectId", "Age", "Sex"]
        result = BaseReader.normalize_dataframe(sample_dataframe, expected_cols)

        assert result.columns == expected_cols, "should have correct columns in correct order"
        assert result.height == 3
        assert result["SubjectId"].to_list() == ["001", "002", "003"]
        assert result["Age"].to_list() == [25, 30, 35]

    def test_normalize_dataframe_missing_columns_raises_error(self, sample_dataframe):
        expected_cols = ["SubjectId", "Age", "MissingColumn"]
        with pytest.raises(ValueError, match="Missing required columns: \\['MissingColumn'\\]"):
            BaseReader.normalize_dataframe(sample_dataframe, expected_cols)

    def test_normalize_dataframe_column_order_preserved(self, sample_dataframe):
        expected_cols = ["Sex", "SubjectId", "Age"]
        result = BaseReader.normalize_dataframe(sample_dataframe, expected_cols)
        assert result.columns == expected_cols

    def test_normalize_dataframe_extra_columns_ignored(self, sample_dataframe):
        expected_cols = ["SubjectId", "Age"]
        result = BaseReader.normalize_dataframe(sample_dataframe, expected_cols)

        assert "extra_col" not in result.columns, "Omits extra cols"
        assert "Sex" not in result.columns
        assert result.columns == expected_cols

    def test_normalize_numeric_types_converts_integer_strings(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["001", "002", "003"],
                "Name": ["Alice", "Bob", "Charlie"],
                "Score": ["85", "92", "78"],
                "Notes": ["Good", "Excellent", "Fair"],
            },
        )

        result = BaseReader.normalize_numeric_types(df)

        assert result.schema["SubjectId"] == pl.Int64, "Should convert to int as numeric only"
        assert result.schema["Name"] == pl.Utf8, "str"
        assert result.schema["Score"] == pl.Int64, "int"
        assert result.schema["Notes"] == pl.Utf8, "str"
        assert result["SubjectId"].to_list() == [
            1,
            2,
            3,
        ], "Leading zeroes should be stripped"
        assert result["Score"].to_list() == [85, 92, 78], "int"

    def test_normalize_numeric_types_preserves_id_columns_with_letters(self):
        df = pl.DataFrame({"SubjectId": ["A001", "B002", "C003"]})

        result = BaseReader.normalize_numeric_types(df)

        assert result.schema["SubjectId"] == pl.Utf8
        assert result["SubjectId"].to_list() == ["A001", "B002", "C003"]

    def test_normalize_numeric_types_preserves_non_integer_strings(self):
        df = pl.DataFrame(
            {
                "Id": ["A01", "B02", "C03"],
                "Value": ["12.5", "13.7", "14.2"],
                "Code": ["", None, "N/A"],
            },
        )

        result = BaseReader.normalize_numeric_types(df)

        assert result.schema["Id"] == pl.Utf8
        assert result.schema["Value"] == pl.Utf8
        assert result.schema["Code"] == pl.Utf8

    def test_normalize_numeric_types_handles_nulls(self):
        df = pl.DataFrame({"Numbers": ["123", None, "456"], "Mixed": ["123", "abc", None]})

        result = BaseReader.normalize_numeric_types(df)

        # numbers column should convert, nulls are allowed
        assert result.schema["Numbers"] == pl.Int64
        assert result["Numbers"].to_list() == [123, None, 456]

        # mixed column should stay string, has non-numeric val
        assert result.schema["Mixed"] == pl.Utf8


class TestExcelReader:
    """Test ExcelReader functionality"""

    def test_can_read_excel_files(self, tmp_path):
        reader = ExcelReader()

        excel_file = tmp_path / "test.xlsx"
        csv_file = tmp_path / "test.csv"
        txt_file = tmp_path / "test.txt"

        excel_file.touch()
        csv_file.touch()
        txt_file.touch()

        assert reader.can_read(excel_file) is True
        assert reader.can_read(csv_file) is False
        assert reader.can_read(txt_file) is False

    def test_can_read_different_excel_extensions(self, tmp_path):
        reader = ExcelReader()
        for ext in [".xls", ".xlsx", ".xlsb"]:
            excel_file = tmp_path / f"test{ext}"
            excel_file.touch()
            assert reader.can_read(excel_file) is True

    def test_can_read_nonexistent_file(self, tmp_path):
        reader = ExcelReader()
        nonexistent = tmp_path / "does_not_exist.xlsx"
        assert reader.can_read(nonexistent) is False

    def test_can_read_directory(self, tmp_path):
        reader = ExcelReader()
        directory = tmp_path / "test_dir"
        directory.mkdir()
        assert reader.can_read(directory) is False


class TestCsvDirectoryReader:
    """Test CsvDirectoryReader functionality"""

    def test_can_read_directory(self, tmp_path):
        reader = CsvDirectoryReader()

        # create test directory and file
        test_dir = tmp_path / "csv_dir"
        test_file = tmp_path / "test.csv"

        test_dir.mkdir()
        test_file.touch()

        assert reader.can_read(test_dir) is True
        assert reader.can_read(test_file) is False

    def test_can_read_nonexistent_directory(self, tmp_path):
        reader = CsvDirectoryReader()
        nonexistent = tmp_path / "does_not_exist"
        assert reader.can_read(nonexistent) is False

    def test_index_csv_files_basic(self, tmp_path):
        (tmp_path / "data_subjects.csv").touch()
        (tmp_path / "data_demographics.csv").touch()
        (tmp_path / "data_labs.csv").touch()
        (tmp_path / "not_csv.txt").touch()

        index = CsvDirectoryReader._index_csv_files(tmp_path)

        expected_keys = {"subjects", "demographics", "labs"}
        assert set(index.keys()) == expected_keys

        assert index["subjects"].name == "data_subjects.csv"
        assert index["demographics"].name == "data_demographics.csv"
        assert index["labs"].name == "data_labs.csv"

    def test_index_csv_files_no_underscore(self, tmp_path):
        (tmp_path / "subjects.csv").touch()
        (tmp_path / "demographics.csv").touch()

        index = CsvDirectoryReader._index_csv_files(tmp_path)

        assert "subjects" in index
        assert "demographics" in index

    def test_index_csv_files_multiple_underscores(self, tmp_path):
        (tmp_path / "trial_data_v2_subjects.csv").touch()
        (tmp_path / "final_version_demographics.csv").touch()

        index = CsvDirectoryReader._index_csv_files(tmp_path)

        assert "subjects" in index
        assert "demographics" in index

    def test_index_csv_files_case_insensitive(self, tmp_path):
        (tmp_path / "data_SUBJECTS.csv").touch()
        (tmp_path / "data_Demographics.csv").touch()

        index = CsvDirectoryReader._index_csv_files(tmp_path)

        assert "subjects" in index
        assert "demographics" in index

    def test_index_csv_files_empty_directory(self, tmp_path):
        index = CsvDirectoryReader._index_csv_files(tmp_path)
        assert index == {}

    def test_load_csv_directory_success(self, tmp_path, sample_config):
        reader = CsvDirectoryReader()
        subjects_csv = tmp_path / "data_subjects.csv"
        demographics_csv = tmp_path / "data_demographics.csv"
        labs_csv = tmp_path / "data_labs.csv"

        # write csv content
        subjects_csv.write_text("Header Row (skipped)\nSubjectId,Age,Sex\nA001,25,M\nA002,30,F\n")

        demographics_csv.write_text("Header Row (skipped)\nSubjectId,Race,Ethnicity\nA001,White,Non-Hispanic\nA002,Black,Hispanic\n")

        labs_csv.write_text("Header Row (skipped)\nSubjectId,TestName,TestValue\nA001,Glucose,85\nA002,Glucose,92\n")

        result = reader.load(tmp_path, sample_config)
        subjects_data = next(d for d in result.data if d.key == "subjects")

        # should load all correctly
        assert len(result.data) == 3
        assert subjects_data.data.height == 2
        assert "SubjectId" in subjects_data.data.columns
        assert subjects_data.data["SubjectId"].to_list() == ["A001", "A002"]

    def test_load_csv_directory_missing_file(self, tmp_path, sample_config):
        reader = CsvDirectoryReader()
        subjects_csv = tmp_path / "data_subjects.csv"
        subjects_csv.write_text("Header Row\nSubjectId,Age,Sex\nA001,25,M\n")
        with pytest.raises(FileNotFoundError, match="No CSV file for key 'demographics'"):
            reader.load(tmp_path, sample_config)

    def test_load_csv_directory_no_csv_files(self, tmp_path, sample_config):
        reader = CsvDirectoryReader()
        (tmp_path / "data.txt").touch()
        (tmp_path / "info.json").touch()
        with pytest.raises(FileNotFoundError, match="No CSV files found"):
            reader.load(tmp_path, sample_config)


class TestInputResolver:
    """Test InputResolver functionality"""

    def test_resolver_uses_correct_reader_for_excel(self, tmp_path, sample_config):
        resolver = InputResolver()
        excel_file = tmp_path / "test.xlsx"
        excel_file.touch()

        mock_excel_reader = Mock()
        mock_excel_reader.can_read.return_value = True
        mock_excel_reader.load.return_value = sample_config

        resolver.readers = [mock_excel_reader]
        _ = resolver.resolve(excel_file, sample_config)

        mock_excel_reader.can_read.assert_called_once_with(excel_file)
        mock_excel_reader.load.assert_called_once_with(excel_file, sample_config)

    def test_resolver_uses_correct_reader_for_directory(self, tmp_path, sample_config):
        resolver = InputResolver()
        csv_dir = tmp_path / "csv_data"
        csv_dir.mkdir()

        mock_csv_reader = Mock()
        mock_csv_reader.can_read.return_value = True
        mock_csv_reader.load.return_value = sample_config

        resolver.readers = [mock_csv_reader]
        _ = resolver.resolve(csv_dir, sample_config)

        mock_csv_reader.can_read.assert_called_once_with(csv_dir)
        mock_csv_reader.load.assert_called_once_with(csv_dir, sample_config)

    def test_resolver_tries_readers_in_order(self, tmp_path, sample_config):
        resolver = InputResolver()
        test_path = tmp_path / "test_file"
        test_path.touch()

        mock_reader1 = Mock()
        mock_reader1.can_read.return_value = False

        mock_reader2 = Mock()
        mock_reader2.can_read.return_value = True
        mock_reader2.load.return_value = sample_config

        resolver.readers = [mock_reader1, mock_reader2]
        _ = resolver.resolve(test_path, sample_config)

        # both readers should be checked
        mock_reader1.can_read.assert_called_once_with(test_path)
        mock_reader2.can_read.assert_called_once_with(test_path)

        # but only second reader should load
        mock_reader1.load.assert_not_called()
        mock_reader2.load.assert_called_once_with(test_path, sample_config)

    def test_resolver_raises_error_when_no_reader_found(self, tmp_path, sample_config):
        resolver = InputResolver()
        unsupported_file = tmp_path / "test.unknown"
        unsupported_file.touch()

        mock_reader = Mock()
        mock_reader.can_read.return_value = False
        mock_reader.SUPPORTED_EXTENSIONS = [".xlsx"]

        resolver.readers = [mock_reader]
        with pytest.raises(ValueError, match="Unsupported input type"):
            resolver.resolve(unsupported_file, sample_config)


def test_full_csv_workflow(tmp_path):
    """Test complete CSV loading workflow"""
    config = EcrfConfig(
        trial="integration_test",
        configs=[
            SheetConfig(key="patients", usecols=["SubjectId", "Age", "Sex"]),
            SheetConfig(key="visits", usecols=["SubjectId", "VisitDate", "VisitType"]),
        ],
    )

    patients_csv = tmp_path / "study_patients.csv"
    visits_csv = tmp_path / "study_visits.csv"

    patients_csv.write_text(
        "Study Metadata (skip)\nSUBJECTID,AGE,SEX\nP001,25,M\nP002,30,F\nP003,35,M\n",
    )

    visits_csv.write_text(
        "Study Metadata (skip)\nSubjectId,VisitDate,VisitType\nP001,2023-01-15,Baseline\nP001,2023-02-15,Follow-up\nP002,2023-01-20,Baseline\n",
    )

    resolver = InputResolver()
    result = resolver.resolve(tmp_path, config)

    patients_data = next(d for d in result.data if d.key == "patients")
    visits_data = next(d for d in result.data if d.key == "visits")

    assert len(result.data) == 2
    assert patients_data.data.height == 3
    assert patients_data.data.columns == ["SubjectId", "Age", "Sex"]
    assert patients_data.data["SubjectId"].to_list() == ["P001", "P002", "P003"]

    assert visits_data.data.height == 3
    assert visits_data.data.columns == ["SubjectId", "VisitDate", "VisitType"]

    if patients_data.data.schema["Age"] == pl.Int64:
        assert patients_data.data["Age"].to_list() == [25, 30, 35]


def test_mixed_case_column_handling(tmp_path):
    """Test handling of mixed case columns across different scenarios"""
    config = EcrfConfig(
        trial="case_test",
        configs=[SheetConfig(key="data", usecols=["SubjectId", "TestValue", "Result_Code"])],
    )

    csv_file = tmp_path / "mixed_data.csv"
    csv_file.write_text("Skip this header\nsubjectid,TESTVALUE,result_code\nS001,85,PASS\nS002,92,FAIL\n")

    resolver = InputResolver()
    result = resolver.resolve(tmp_path, config)
    data = result.data[0]

    assert data.data.columns == ["SubjectId", "TestValue", "Result_Code"]
    assert data.data.height == 2
