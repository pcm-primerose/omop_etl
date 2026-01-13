from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence, Literal
import polars as pl

from omop_etl.infra.utils.run_context import RunMetadata


@dataclass(frozen=True)
class SheetConfig:
    key: str
    usecols: Sequence[str]


@dataclass
class SheetData:
    key: str
    data: pl.DataFrame
    input_path: Path | None = None


@dataclass
class EcrfConfig:
    configs: list[SheetConfig]
    data: list[SheetData] | None = None
    trial: str | None = None
    source_type: str | None = None

    @classmethod
    def from_mapping(cls, m: Mapping[str, list[str]]) -> EcrfConfig:
        return cls([SheetConfig(key=k.upper(), usecols=v) for k, v in m.items()])


@dataclass(frozen=True)
class PreprocessingRunOptions:
    filter_valid_cohort: bool = True
    combine_key: str = "SubjectId"


OutputFormat = Literal["csv", "tsv", "parquet"]


@dataclass(frozen=True)
class OutputPath:
    """Resolved output paths for data and manifest."""

    data_file: Path
    manifest_file: Path
    log_file: Path
    directory: Path
    format: OutputFormat


@dataclass(frozen=True)
class PreprocessResult:
    """Result of preprocessing operation."""

    output_path: OutputPath
    rows: int
    columns: int
    context: RunMetadata
