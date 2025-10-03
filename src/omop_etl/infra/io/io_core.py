from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import SupportsWrite
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, cast
import polars as pl
import json

from omop_etl.infra.io.json_encoder import ISOJSONEncoder
from omop_etl.infra.io.options import (
    JsonOptions,
    ParquetOptions,
    CsvOptions,
)
from omop_etl.infra.io.types import Format


@dataclass(frozen=True)
class TableMeta:
    rows: int
    cols: int
    schema: Dict[str, str]


@dataclass(frozen=True)
class WriterResult:
    main_file: Path
    table_files: Dict[str, Path]
    tables: Dict[str, TableMeta]


def write_frame(
    df: pl.DataFrame,
    path: Path,
    fmt: Format,
    opts: CsvOptions | ParquetOptions | None = None,
) -> WriterResult:
    path.parent.mkdir(parents=True, exist_ok=True)

    if fmt in ("csv", "tsv"):
        if not isinstance(opts, CsvOptions):
            opts = CsvOptions(separator="\t" if fmt == "tsv" else ",")
        c = opts
        df.write_csv(
            path,
            include_header=c.include_header,
            null_value=c.null_value,
            float_precision=c.float_precision,
            separator=c.separator,
        )

    elif fmt == "parquet":
        if not isinstance(opts, ParquetOptions):
            opts = ParquetOptions()
        p = opts
        df.write_parquet(path, compression=p.compression, statistics=p.statistics)

    else:
        raise ValueError(f"Unsupported tabular fmt: {fmt}")

    schema_map = {col: str(dtype_) for col, dtype_ in df.schema.items()}
    meta = TableMeta(df.height, df.width, schema_map)
    return WriterResult(main_file=path, table_files={}, tables={"wide": meta})


def write_frames_dir(
    frames: Dict[str, pl.DataFrame],
    dirpath: Path,
    fmt: Format,
    opts: CsvOptions | ParquetOptions | None = None,
) -> WriterResult:
    dirpath.mkdir(parents=True, exist_ok=True)
    files, metas = {}, {}
    for name, df in frames.items():
        if df.height == 0:
            continue
        ext = ".tsv" if fmt == "tsv" else (".csv" if fmt == "csv" else ".parquet")
        p = dirpath / f"{name}{ext}"
        write_frame(df, p, fmt, opts)
        files[name] = p
        metas[name] = TableMeta(df.height, df.width, {c: str(t) for c, t in df.schema.items()})
    main = files.get("patients") or next(iter(files.values()))
    return WriterResult(main_file=main, table_files=files, tables=metas)


def write_json(obj: dict, path: Path, opts: JsonOptions | None = None) -> WriterResult:
    j = opts or JsonOptions()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(obj, cast(SupportsWrite[str], fp), cls=ISOJSONEncoder, ensure_ascii=j.ensure_ascii, indent=j.indent)
    return WriterResult(main_file=path, table_files={}, tables={})


def write_manifest(doc: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
