from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import polars as pl
import json

from omop_etl.infra.io.json_encoder import ISOJSONEncoder
from omop_etl.infra.io.options import (
    JsonOptions,
    ParquetOptions,
    CsvOptions,
)
from omop_etl.infra.io.types import TabularFormat, POLARS_DTYPE_TO_NAME


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
    fmt: TabularFormat,  # fixme: Type hint is invalid or refers to the expression which is not a correct type
    opts: CsvOptions | ParquetOptions | None = None,
) -> WriterResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema_map = _schema_to_manifest(df.schema)

    if fmt in ("csv", "tsv"):
        if not isinstance(opts, CsvOptions):
            opts = CsvOptions(separator="\t" if fmt == "tsv" else ",")

        df.write_csv(
            path,
            include_header=opts.include_header,
            null_value=opts.null_value,
            float_precision=opts.float_precision,
            separator=opts.separator,
        )

    elif fmt == "parquet":
        if not isinstance(opts, ParquetOptions):
            opts = ParquetOptions()
        df.write_parquet(path, compression=opts.compression, statistics=opts.statistics)

    else:
        raise ValueError(f"Unsupported tabular fmt: {fmt}")

    meta = TableMeta(df.height, df.width, schema_map)
    return WriterResult(main_file=path, table_files={}, tables={"wide": meta})


def write_frames_dir(
    frames: Dict[str, pl.DataFrame],
    dirpath: Path,
    fmt: TabularFormat,
    opts: CsvOptions | ParquetOptions | None = None,
) -> WriterResult:
    dirpath.mkdir(parents=True, exist_ok=True)
    files, metas = {}, {}
    for name, df in frames.items():
        if df.height == 0:
            continue

        ext = ".tsv" if fmt == "tsv" else (".csv" if fmt == "csv" else ".parquet")
        path = dirpath / f"{name}{ext}"
        write_frame(df, path, fmt, opts)

        files[name] = path
        metas[name] = TableMeta(df.height, df.width, {c: str(t) for c, t in df.schema.items()})

    main = files.get("patients") or next(iter(files.values()))
    return WriterResult(main_file=main, table_files=files, tables=metas)


def write_json(obj: dict | list, path: Path, opts: JsonOptions | None = None) -> WriterResult:
    j = opts or JsonOptions()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(obj, fp, cls=ISOJSONEncoder, ensure_ascii=j.ensure_ascii, indent=j.indent)  # type: ignore
    return WriterResult(main_file=path, table_files={}, tables={})


def write_manifest(doc: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


def _schema_to_manifest(schema: pl.Schema) -> dict[str, str]:
    """Converts pl.Schema to json-serialized mapping"""
    out: dict[str, str] = {}
    for col, dtype in schema.items():
        try:
            out[col] = POLARS_DTYPE_TO_NAME[dtype]
        except KeyError:
            raise ValueError(f"Unsupported drype {dtype} for column {col}, add to POALRS_DTYPE_TO_NAME and NAME_TO_POLARS_DTYPE")

    return out
