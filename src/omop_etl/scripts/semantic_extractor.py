import argparse
import re
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import Sequence, Optional, Mapping, Dict, Literal
import polars as pl

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
            is_int_col = df.select(
                (pl.col(col_name).is_null() | pl.col(col_name).str.strip_chars().str.contains(int_pattern.pattern)).all()
            ).item()

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
                raise FileNotFoundError(
                    f"No CSV file for key '{source_config.key}' in {path}. Available files: {list(file_index.values())}"
                )

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
    Examples of what's not included:
        - Pure numerical measurments & observations (e.g. age, target tumor size, number of lesions)
        - Identifiers (e.g. SubjectId)
        - Questionnaires (e.g. QLQ-C30, EQ5D)
        - (most) Drop-down lists (samplespace is available in synthetic data)
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
        "ae": [
            "SubjectId",
            "AECTCAET",
        ],
        "mh": [
            "SubjectId",
            "MHTERM",
        ],
    }


def _drop_subject_id(data: pl.DataFrame, drop: str) -> pl.DataFrame:
    return data.drop(drop)


def _drop_nulls(data: pl.DataFrame) -> pl.DataFrame:
    return data.filter(pl.any_horizontal(pl.col("*").is_not_null()))


def _get_unique(data: pl.DataFrame) -> pl.DataFrame:
    return data.unique()


def filter_df(data: pl.DataFrame, drop: str) -> pl.DataFrame:
    """
    Apply filtering functions: get_unique, drop nulls and drop SubjectId col
    """
    data = _get_unique(data)
    data = _drop_nulls(data)
    data = _drop_subject_id(data, drop=drop)

    return data


def frames_to_dict(config: EcrfConfig) -> dict[str, pl.Series]:
    out: dict[str, pl.Series] = {}
    for sheet in config.data:
        df = filter_df(sheet.data, drop="SubjectId")
        sheet_key = (sheet.key or "").upper()
        for s in df.get_columns():
            out[f"{sheet_key}_{s.name}"] = s
    return out


def dict_to_counts(d: dict[str, pl.Series]) -> pl.DataFrame:
    parts = []
    for key, s in d.items():
        vc = (
            s.drop_nulls()
            .value_counts()
            .rename({s.name: "source_term", "count": "frequency"})
            .with_columns(pl.lit(key).alias("source_col"))
            .select("source_col", "source_term", "frequency")
            .with_columns(pl.col("frequency").cast(pl.Int64))
        )
        parts.append(vc)
    return pl.concat(parts, how="vertical_relaxed")


Scope = Literal["per_scope", "global"]

# TODO:
#   [ ] make uuid 5 deterministic:
#       - make template uuid string
#       - concatenate term str + tempalte
#       - hash this and take first 128 bytes
#       - should be unique for all input strings, but deterministic: cool


def add_term_id(data: pl.DataFrame, id_scope: Scope = "per_scope") -> pl.DataFrame:
    """
    Adds uuid5 per unique term scoped to sheet or globally as new column in dataframe.
    """

    term = pl.col("source_term").cast(pl.Utf8)
    _namespace = "5c630d6e-a4f6-11f0-aeff-325096b39f47"

    if id_scope == "global":
        namespace = uuid.UUID(_namespace)
        output = data.with_columns(
            term.map_elements(lambda string: str(uuid.uuid5(namespace, string)), return_dtype=pl.Utf8).alias("term_id")
        ).select("term_id", "source_col", "source_term", "frequency")
        return output

    if id_scope == "per_scope":
        namespace = uuid.UUID(_namespace)
        output = data.with_columns(
            (pl.col("source_col").cast(pl.Utf8) + ":" + term)
            .map_elements(lambda string: str(uuid.uuid5(namespace, string)), return_dtype=pl.Utf8)
            .alias("term_id")
        ).select("term_id", "source_col", "source_term", "frequency")
        return output

    else:
        raise ValueError(f"id_scope not valid: `{id_scope}`. Needs to be: `{Scope}`")


def run(
    input_path: Path,
    output_dir: Path,
    trial: str = "impress",
):
    start_time = time.time()
    config = EcrfConfig.from_mapping(get_config())
    InputResolver().resolve(path=input_path, ecfg=config)

    series_by_key = frames_to_dict(config)
    combined_df = dict_to_counts(series_by_key).sort(["source_col", "frequency"], descending=[False, True])
    output_df = add_term_id(combined_df)

    output_dir.mkdir(parents=True, exist_ok=True)
    outfile = output_dir / f"semantic_terms_{trial}_{start_time}.csv"
    output_df.write_csv(outfile)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="semantic-extractor", description="Compute term frequencies per sheet/column from eCRF extracts.")
    parser.add_argument(
        "-i", "--input-path", type=Path, required=True, help="Input directory or file to resolve (as expected by InputResolver)."
    )
    parser.add_argument("-o", "--output-dir", type=Path, required=True, help="Directory to write CSV output into.")
    parser.add_argument("-t", "--trial", default="impress", help="Trial name used in output filename. Default: %(default)s")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    _args = parser.parse_args(argv)

    if not _args.input_path.exists():
        parser.error(f"--input-path not found: {_args.input_path}")

    run(
        input_path=_args.input_path,
        output_dir=_args.output_dir,
        trial=_args.trial,
    )
    return 0


def run_ide():
    run(
        input_path=Path("/Users/gabriebs/projects/omop_etl/.data/synthetic/impress_150"),
        output_dir=Path("/Users/gabriebs/projects/omop_etl/.data/semantic_input"),
    )


if __name__ == "__main__":
    run_ide()
    # raise SystemExit(main())
