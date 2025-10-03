from enum import StrEnum
from typing import Final, Literal, Mapping, TypeAlias
from types import MappingProxyType, NoneType


class OutputMode(StrEnum):
    WIDE = "wide"
    NORMALIZED = "normalized"


class Layout(StrEnum):
    TRIAL_ONLY = "trial_only"
    TRIAL_RUN = "trial_run"
    TRIAL_TIMESTAMP_RUN = "trial_timestamp_run"


Format = Literal["csv", "tsv", "parquet", "json", "all"]
TabularFormat = Literal["csv", "tsv", "parquet"]
RunSource = Literal["api", "cli"]

WIDE_FORMATS: Final[frozenset[str]] = frozenset({"csv", "tsv", "parquet", "json"})
NORMALIZED_FORMATS: Final[frozenset[str]] = frozenset({"csv", "tsv", "parquet"})
RUN_SOURCES: Final[frozenset[str]] = frozenset({"api", "cli"})

ALIASES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "txt": "tsv",
    },
)

FORMATS_BY_MODE: Final[Mapping[OutputMode, frozenset[str]]] = MappingProxyType(
    {
        OutputMode.WIDE: WIDE_FORMATS,
        OutputMode.NORMALIZED: NORMALIZED_FORMATS,
    },
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
