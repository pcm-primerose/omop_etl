from enum import StrEnum
from typing import Final, Literal, Mapping, TypeAlias, Sequence
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
