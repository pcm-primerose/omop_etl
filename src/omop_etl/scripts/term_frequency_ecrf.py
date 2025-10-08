import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import Sequence, Optional, Mapping, Dict, List
import polars as pl
from polars import any_horizontal

log = getLogger(__name__)


@dataclass(frozen=True)
class SheetConfig:
    key: str
    usecols: Sequence[str]


@dataclass
class SheetData:
    key: str
    data: pl.DataFrame
    input_path: Optional[Path] = None


@dataclass
class EcrfConfig:
    configs: list[SheetConfig]
    data: list[SheetData] | None = None
    trial: str | None = None
    source_type: str | None = None

    @classmethod
    def from_mapping(cls, m: Mapping[str, list[str]]) -> "EcrfConfig":
        return cls([SheetConfig(key=k.upper(), usecols=v) for k, v in m.items()])


def combine(ecfg: EcrfConfig, on: str = "SubjectId") -> pl.DataFrame:
    if not ecfg.data:
        raise ValueError("No eCRF config data loaded")

    frames: list[pl.DataFrame] = []
    for sheet_data in ecfg.data:
        df = sheet_data.data
        if on not in df.columns:
            raise ValueError(f"'{on}' not in sheet {sheet_data.key}")

        # normalize key dtype across sheets to avoid concat type errors
        df = df.with_columns(pl.col(on).cast(pl.Utf8))

        # prefix everything except the key
        mapping = {c: f"{sheet_data.key}_{c}" for c in df.columns if c != on}
        frames.append(df.rename(mapping))

    # union-of-columns vertical concat, fill missing with nulls
    out = pl.concat(frames, how="diagonal", rechunk=True).sort(on, nulls_last=True)
    return out


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
        return path.exists() and path.is_file() and path.suffix.casefold() in self.SUPPORTED_EXTENSIONS

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

            if file_path.suffix.casefold() != ".csv":
                continue

            # extract key from filename, use part after last underscore
            # if no underscore use whole stem
            stem_parts = file_path.stem.split("_")
            key = stem_parts[-1].casefold()

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


def get_config() -> dict:
    """
    Returns config with only columns in need of semantic mapping.

    If columns do not add semantic information over the synthetic data, they are dropped (can be mapped locally),
    or if a column itself is mapped instead of it's values (handled in structural mapping).
    Examples:
        - Pure numerical measurments & observations (e.g. age, target tumor size, number of lesions)
        - Identifiers (e.g. SubjectId)
        - Questionnaires (e.g. QLQ-C30, EQ5D)
    """
    return {
        "coh": [
            "SubjectId",
            "ICD10COD",
            "ICD10DES",
            "COHTTYPE",
            "COHTTYPE__2",
            "COHTT",
            "COHTTOSP",
            "GENMUT1",
            "COHCTN",
            "COHTMN",
        ],
        "ct": [
            "SubjectId",
            "CTTYPE",
            "CTTYPESP",
        ],
        "tr": [
            "SubjectId",
            "TRNAME",
        ],
        "cm": [
            "SubjectId",
            "CMTRT",
        ],
        "ae": ["SubjectId", "AECTCAET"],
        "mh": [
            "SubjectId",
            "MHTERM",
        ],
    }


# TODO:
# [x] make config of non-sensitive cols
# [x] use readers
# [x] combiner
# [x] datmodels, readers
# [x] after combining: drop sensitive data
# [ ] restructure filtered data to output format
#       - genererate uid per unique term
# [ ] write to output dir
# [ ] works locally
# [ ] write test
#   [ ] assert no subject id
#   [ ] assert no cols with all nulls
#   [ ] assert merging works as intended
#   [ ] assert values are uniquue
# [ ] add cli, run on tsd


def _drop_subject_id(data: pl.DataFrame, col: str) -> pl.DataFrame:
    return data.drop(col)


def _drop_nulls(data: pl.DataFrame) -> pl.DataFrame:
    return data.filter(any_horizontal(pl.col("*").is_not_null()))


def _get_unique(data: pl.DataFrame) -> pl.DataFrame:
    return data.unique()


def filter_df(data: pl.DataFrame, drop_col: str) -> pl.DataFrame:
    """
    Apply filtering functions: get_unique, drop nulls and drop SubjectId col
    """
    data = _get_unique(data)
    data = _drop_nulls(data)
    data = _drop_subject_id(data, col=drop_col)

    return data


def run(
    input_path: Path,
    output_path: Path,
    drop_sensitive_data: bool = True,
    # trial, otuput filename
):
    # make config
    config = EcrfConfig.from_mapping(get_config())

    # resolve input
    resolver = InputResolver()
    resolver.resolve(path=input_path, ecfg=config)

    col_with_unique_data: List[pl.Series] = list(pl.Series())

    # want to have df_out:
    #   source_col = column name in filtered df
    #   source_term = all terms in all filtered df

    for sheet in config.data:
        filtered = filter_df(sheet.data, drop_col="SubjectId")
        col_with_unique_data.append(filtered.to_series())

    print(f"series: {col_with_unique_data}")


if __name__ == "__main__":
    run(
        input_path=Path("/Users/gabriebs/projects/omop_etl/.data/synthetic/impress_150"),
        output_path=Path("/Users/gabriebs/projects/omop_etl/.data/semantic_input"),
    )
