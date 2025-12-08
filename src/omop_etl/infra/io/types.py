from enum import StrEnum
from typing import Final, Literal, Mapping, TypeAlias, Sequence
import polars as pl

from types import MappingProxyType, NoneType


class OutputMode(StrEnum):
    WIDE = "wide"
    NORMALIZED = "normalized"


class Layout(StrEnum):
    TRIAL_ONLY = "trial_only"
    TRIAL_RUN = "trial_run"
    TRIAL_TIMESTAMP_RUN = "trial_timestamp_run"


TabularFormat = Literal["csv", "tsv", "parquet"]
WideFormat = Literal["csv", "tsv", "parquet", "json"]
AnyFormatToken = Literal["csv", "tsv", "parquet", "json", "all"]

RunSource = Literal["api", "cli"]

TABULAR_FORMATS: Final[tuple[TabularFormat, ...]] = ("csv", "tsv", "parquet")
WIDE_FORMATS: Final[tuple[WideFormat, ...]] = ("csv", "tsv", "parquet", "json")
RUN_SOURCES: Final[frozenset[str]] = frozenset({"api", "cli"})

ALIASES: Final[Mapping[str, str]] = MappingProxyType({"txt": "tsv"})

FORMATS_BY_MODE: Final[Mapping[OutputMode, Sequence[str]]] = MappingProxyType(
    {OutputMode.WIDE: WIDE_FORMATS, OutputMode.NORMALIZED: TABULAR_FORMATS},
)


class SerializeTypes:
    NONE_TYPE: Final[type[None]] = NoneType
    ID_COLUMNS: Final[tuple[str, ...]] = ("patient_id", "trial_id")
    IDENTITY_FIELDS: Final[frozenset[str]] = frozenset(ID_COLUMNS)
    COL_SEP: Final[str] = "."
    LIST_OR_TUPLE: Final[tuple[type, ...]] = (list, tuple)


ParquetCompression: TypeAlias = Literal[
    "lz4",
    "uncompressed",
    "snappy",
    "gzip",
    "lzo",
    "brotli",
    "zstd",
]

POLARS_DTYPE_TO_NAME: dict[pl.DataType, str] = {
    pl.String: "string",
    pl.Int64: "int64",
    pl.Int32: "int32",
    pl.UInt64: "uint64",
    pl.UInt32: "uint32",
    pl.Float64: "float64",
    pl.Float32: "float32",
    pl.Boolean: "bool",
    pl.Date: "date",
    pl.Datetime: "datetime",
    pl.Time: "time",
    pl.Utf8: "utf8",
}

NAME_TO_POLARS_DTYPE: dict[str, pl.DataType] = {v: k for k, v in POLARS_DTYPE_TO_NAME.items()}
