from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CsvOptions:
    include_header: bool = True
    null_value: Optional[str] = None
    float_precision: int = 6
    separator: str = ","  # tsv overrides to "\t"


@dataclass(frozen=True)
class ParquetOptions:
    compression: str = "zstd"
    statistics: bool = True


@dataclass(frozen=True)
class JsonOptions:
    indent: int = 2
    ensure_ascii: bool = False
    datetime: str = "iso"


@dataclass(frozen=True)
class WriterOptions:
    csv: CsvOptions = CsvOptions()
    tsv: CsvOptions = CsvOptions(separator="\t")
    parquet: ParquetOptions = ParquetOptions()
    json: JsonOptions = JsonOptions()
