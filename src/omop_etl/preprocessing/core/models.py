from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Mapping, Sequence, Literal
import polars as pl


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


@dataclass(frozen=True)
class RunOptions:
    filter_valid_cohort: bool = False


OutputFormat = Literal["csv", "tsv", "parquet"]


@dataclass(frozen=True)
class OutputSpec:
    base_dir: Path
    fmt: Optional[OutputFormat] = "csv"
    subdir: str = "{trial}/{ts}/{run_id}"
    filename: str = "preprocessed"


@dataclass(frozen=True)
class PreprocessResult:
    path: Path
    rows: int
    cols: int
