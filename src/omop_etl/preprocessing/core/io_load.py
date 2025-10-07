import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Sequence, Dict, Optional
import polars as pl
from logging import getLogger

from .models import EcrfConfig, SheetData

log = getLogger(__name__)


class BaseReader(ABC):
    """Base class for data readers with common functionality."""

    @abstractmethod
    def can_read(self, path: Path) -> bool:
        """Check if this reader can handle the given path."""
        pass

    @abstractmethod
    def load(self, path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        """Load data from path into the eCRF config."""
        pass

    @staticmethod
    def normalize_dataframe(df: pl.DataFrame, expected_cols: Sequence[str]) -> pl.DataFrame:
        """
        Normalize a dataframe to match expected schema.
        """
        column_map = {col.upper(): col for col in df.columns}

        # find present and missing columns
        present = []
        missing = []
        rename_map = {}

        for expected in expected_cols:
            expected_upper = expected.upper()
            if expected_upper in column_map:
                actual_col = column_map[expected_upper]
                present.append(actual_col)
                if actual_col != expected:
                    rename_map[actual_col] = expected
            else:
                missing.append(expected)

        if missing:
            raise ValueError(f"Missing required columns: {missing}. Available columns: {list(df.columns)}")

        # rename cols
        result = df.select(present)
        if rename_map:
            result = result.rename(rename_map)

        # ensure col order matches expected
        return result.select(expected_cols[: len(present)])

    @staticmethod
    def normalize_numeric_types(df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize numeric string columns to proper numeric types.

        This ensures consistency between CSV and Excel inputs by:
        - Converting integer-like strings to Int64 (e.g., "01" -> 1)
        - Preserving nulls and non-numeric strings
        """

        int_pattern = re.compile(r"^[+-]?\d+$")

        # find string columns that contain only integers
        int_columns = []

        for col_name, dtype in df.schema.items():
            if dtype != pl.Utf8:
                continue

            # check if all non-null values are integer strings
            is_int_col = df.select((pl.col(col_name).is_null() | pl.col(col_name).str.strip_chars().str.contains(int_pattern.pattern)).all()).item()

            if is_int_col:
                int_columns.append(col_name)

        # convert integer string columns to Int64
        if int_columns:
            log.debug(f"Converting string columns to integers: {int_columns}")
            return df.with_columns([pl.col(col).cast(pl.Int64) for col in int_columns])

        return df


class ExcelReader(BaseReader):
    """Reader for Excel files (.xls, .xlsx, .xlsb)."""

    SUPPORTED_EXTENSIONS = {".xls", ".xlsx", ".xlsb"}

    def can_read(self, path: Path) -> bool:
        """Check if path is a readable Excel file."""
        return path.exists() and path.is_file() and path.suffix.lower() in self.SUPPORTED_EXTENSIONS

    def load(self, path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        """
        Load data from Excel file into eCRF config.
        """
        log.info(f"Loading Excel file: {path}")

        all_sheets = pl.read_excel(path, sheet_id=0, has_header=True, read_options={"header_row": 1})

        loaded_sheets = []

        for source_config in ecfg.configs:
            sheet_name = source_config.key

            if sheet_name not in all_sheets:
                log.warning(f"Sheet '{sheet_name}' not found in {path}")
                continue

            df = all_sheets[sheet_name]

            # normalize schema and types
            df = self.normalize_dataframe(df, source_config.usecols)
            df = self.normalize_numeric_types(df)

            sheet_data = SheetData(key=source_config.key, data=df, input_path=path)
            loaded_sheets.append(sheet_data)

            log.debug(f"Loaded sheet '{sheet_name}': {df.height} rows, {df.width} columns")

        ecfg.data = (ecfg.data or []) + loaded_sheets
        return ecfg


class CsvDirectoryReader(BaseReader):
    """Reader for directories containing CSV files."""

    def can_read(self, path: Path) -> bool:
        """Check if path is a directory."""
        return path.exists() and path.is_dir()

    def load(self, path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        """
        Load data from CSV files in directory into eCRF config.
        """
        log.info(f"Loading CSV files from directory: {path}")

        # index all csv files in dir
        file_index = self._index_csv_files(path)

        if not file_index:
            raise FileNotFoundError(f"No CSV files found in {path}")

        loaded_sheets = []

        for source_config in ecfg.configs:
            key_lower = source_config.key.casefold()

            if key_lower not in file_index:
                raise FileNotFoundError(f"No CSV file for key '{source_config.key}' in {path}. Available files: {list(file_index.values())}")

            csv_path = file_index[key_lower]
            log.debug(f"Reading CSV file: {csv_path}")

            # read csv and normalize schema
            df = pl.read_csv(csv_path, skip_rows=1, infer_schema_length=10000)
            df = self.normalize_dataframe(df, source_config.usecols)

            sheet_data = SheetData(key=source_config.key, data=df, input_path=csv_path)
            loaded_sheets.append(sheet_data)

            log.debug(f"Loaded CSV '{source_config.key}': {df.height} rows, {df.width} columns")

        ecfg.data = (ecfg.data or []) + loaded_sheets
        return ecfg

    @staticmethod
    def _index_csv_files(directory: Path) -> Dict[str, Path]:
        index = {}

        for file_path in directory.iterdir():
            if not file_path.is_file():
                continue

            if file_path.suffix.lower() != ".csv":
                continue

            # extract key from filename, use part after last underscore
            # if no underscore use whole stem
            stem_parts = file_path.stem.split("_")
            key = stem_parts[-1].lower()

            if key in index:
                log.warning(f"Duplicate key '{key}' found: {index[key]} and {file_path}")

            index[key] = file_path

        return index


class InputResolver:
    """Resolves and loads input data using appropriate reader."""

    def __init__(self, readers: Optional[Sequence[BaseReader]] = None):
        self.readers = readers or [ExcelReader(), CsvDirectoryReader()]

    def resolve(self, path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        """
        Resolve input path and load data using correct reader.
        """
        for reader in self.readers:
            if reader.can_read(path):
                log.debug(f"Using {reader.__class__.__name__} for {path}")
                return reader.load(path, ecfg)

        # no reader found
        supported = []
        for reader in self.readers:
            if hasattr(reader, "SUPPORTED_EXTENSIONS"):
                supported.extend(reader.SUPPORTED_EXTENSIONS)
            else:
                supported.append(reader.__class__.__name__)

        raise ValueError(f"Unsupported input type: {path}. Supported: {', '.join(supported)} or directory of CSVs")
