# infra/io_core.py
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable
import polars as pl
import json

from omop_etl.infra.options import JsonOptions, NdjsonOptions, ParquetOptions, CsvOptions
from omop_etl.infra.types import Format


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


def write_frame(df: pl.DataFrame, path: Path, fmt: Format, opts: CsvOptions | ParquetOptions | None = None) -> WriterResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv" or fmt == "tsv":
        c: CsvOptions = opts or CsvOptions(separator="\t" if fmt == "tsv" else ",")
        df.write_csv(path, include_header=c.include_header, null_value=c.null_value, float_precision=c.float_precision, separator=c.separator)
    elif fmt == "parquet":
        p: ParquetOptions = opts or ParquetOptions()
        # todo: fix typerrror: Expected type 'Literal["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]', got 'str' instead
        df.write_parquet(path, compression=p.compression, statistics=p.statistics)
    else:
        raise ValueError(f"Unsupported tabular fmt: {fmt}")

    # todo: fix typerror: Expected type 'dict[str, str]', got 'dict[CsvOptions, str]' instead
    meta = TableMeta(df.height, df.width, {c: str(t) for c, t in df.schema.items()})
    return WriterResult(main_file=path, table_files={}, tables={"wide": meta})


def write_frames_dir(frames: Dict[str, pl.DataFrame], dirpath: Path, fmt: Format, opts: CsvOptions | ParquetOptions | None = None) -> WriterResult:
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


def write_json_text(text: str, path: Path, opts: JsonOptions | None = None) -> WriterResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    j = opts or JsonOptions()
    # options only affect *producing* text; here we assume text already prepared
    path.write_text(text, encoding="utf-8")
    return WriterResult(main_file=path, table_files={}, tables={})


def write_ndjson(objs: Iterable[dict], path: Path, opts: NdjsonOptions | None = None) -> WriterResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = opts or NdjsonOptions()
    with path.open("w", encoding="utf-8") as fp:
        for o in objs:
            fp.write(json.dumps(o, ensure_ascii=n.ensure_ascii) + "\n")
    return WriterResult(main_file=path, table_files={}, tables={})


def write_manifest(doc: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


# # infra/io_core.py
# from dataclasses import dataclass
# from pathlib import Path
# from typing import Dict, Optional
# import json
# import polars as pl
#
# from omop_etl.infra.run_context import RunMetadata
#
# SUPPORTED_FORMATS = {"csv", "tsv", "parquet", "json", "ndjson"}
# FORMAT_EXT = {"csv": ".csv", "tsv": ".tsv", "parquet": ".parquet", "json": ".json", "ndjson": ".ndjson"}
#
# # OK
# # TODO:
# #   here: stateless writers that recieve data and path
#
#
#
#
# @dataclass(frozen=True)
# class OutputPath:
#     data_file: Path
#     manifest_file: Path
#     log_file: Path
#     directory: Path
#     format: str
#
# from enum import Enum
#
# class _Layout(Enum):
#     TRIAL_ONLY = "trial_only"        # <base>/<trial>/
#     TRIAL_RUN  = "trial_run"         # <base>/<trial>/<run_id>/
#     TRIAL_TIME_RUN = "trial_time_run"  # <base>/<trial>/<started>_<run_id>/
#
# def _run_root(base_dir: Path, meta: RunMetadata, layout: _Layout) -> Path:
#     if layout is _Layout.TRIAL_ONLY:
#         root = base_dir / meta.trial.lower()
#     elif layout is _Layout.TRIAL_RUN:
#         root = base_dir / meta.trial.lower() / meta.run_id
#     else:
#         root = base_dir / meta.trial.lower() / f"{meta.started_at}_{meta.run_id}"
#     root.mkdir(parents=True, exist_ok=True)
#     return root
#
#
# def _normalize_format(fmt: Optional[str], default: str = "csv") -> str:
#     if not fmt:
#         return default
#     f = fmt.lower()
#     if f == "txt":
#         f = "tsv"
#     if f not in SUPPORTED_FORMATS:
#         raise ValueError(f"Unsupported format '{fmt}'. Supported: {', '.join(sorted(SUPPORTED_FORMATS))}")
#     return f
#
#
# def _infer_format(path: Path) -> Optional[str]:
#     suf = path.suffix.lower().lstrip(".")
#     if suf == "txt":
#         suf = "tsv"
#     return suf if suf in SUPPORTED_FORMATS else None
#
# # def write_normalized_dataframe(data: pl.DataFrame):
#
#
#
# def resolve_output_path(
#     trial: str,
#     timestamp: str,
#     run_id: str,
#     base_dir: Path,
#     output: Optional[Path] = None,
#     fmt: Optional[str] = None,
#     filename_stem: str = "data",
# ) -> OutputPath:
#     if output and output.suffix:
#         f = _infer_format(output) or _normalize_format(fmt)
#         out = output.with_suffix(FORMAT_EXT[f])
#         out.parent.mkdir(parents=True, exist_ok=True)
#         manifest = out.parent / f"manifest_{out.stem}.json"
#         logf = out.parent / f"{out.stem}.log"
#         return OutputPath(out, manifest, logf, out.parent, f)
#
#     base = output if (output and output.is_dir()) else base_dir
#     outdir = base / trial.lower() / f"{timestamp}_{run_id}"
#     outdir.mkdir(parents=True, exist_ok=True)
#     f = _normalize_format(fmt)
#     data = (outdir / f"data_{filename_stem}").with_suffix(FORMAT_EXT[f])
#     manifest = outdir / f"manifest_{filename_stem}.json"
#     logf = outdir / f"{filename_stem}.log"
#     return OutputPath(data, manifest, logf, outdir, f)
#
#
# def write_single_frame(df: pl.DataFrame, path: Path, fmt: str) -> None:
#     if fmt == "csv":
#         df.write_csv(path, include_header=True, null_value=None, float_precision=6)
#     elif fmt == "tsv":
#         df.write_csv(path, include_header=True, null_value=None, float_precision=6, separator="\t")
#     elif fmt == "parquet":
#         df.write_parquet(path, compression="zstd", statistics=True)
#     else:
#         raise ValueError(f"Unsupported frame format for single-file export: {fmt}")
#
#
# def write_frames_dir(frames: Dict[str, pl.DataFrame], outdir: Path, fmt: str) -> Dict[str, Path]:
#     files: Dict[str, Path] = {}
#     outdir.mkdir(parents=True, exist_ok=True)
#     for name, df in frames.items():
#         if df.height == 0:
#             continue
#         path = outdir / f"{name}{FORMAT_EXT[fmt]}"
#         write_single_frame(df, path, fmt)
#         files[name] = path
#     return files
#
#
# def write_manifest(
#     path: Path,
#     trial: str,
#     timestamp: str,
#     run_id: str,
#     input_path: Path,
#     output_file: Path,
#     directory: Path,
#     fmt: str,
#     mode: str,
#     tables: Dict[str, pl.DataFrame],
#     table_files: Optional[Dict[str, Path]] = None,
#     options: Optional[dict] = None,
# ) -> None:
#     manifest = {
#         "trial": trial,
#         "timestamp": timestamp,
#         "run_id": run_id,
#         "input": str(input_path.absolute()),
#         "output": str(output_file.absolute()),
#         "directory": str(directory.absolute()),
#         "format": fmt,
#         "mode": mode,
#         "tables": {
#             name: {
#                 "rows": df.height,
#                 "cols": df.width,
#                 "schema": {c: str(t) for c, t in df.schema.items()},
#                 "file": str(table_files[name].absolute()) if table_files and name in table_files else None,
#             }
#             for name, df in tables.items()
#         },
#         "options": options or {},
#     }
#     path.parent.mkdir(parents=True, exist_ok=True)
#     path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
#
#
