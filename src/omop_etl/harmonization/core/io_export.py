# harmonization/io_export.py
from pathlib import Path
from typing import Sequence, Dict
from ..datamodels import HarmonizedData
from ...infra.types import Layout, Format, WIDE_FORMATS, NORMALIZED_FORMATS
from ...infra.options import WriterOptions
from ...infra.path_planner import plan_paths, WriterContext
from ...infra.io_core import write_frame, write_frames_dir, write_json_text, write_ndjson, write_manifest
from ...infra.manifest_builder import build_manifest


class HarmonizedExporter:
    def __init__(self, base_out: Path, layout: Layout = Layout.TRIAL_RUN):
        self.base_out = base_out
        self.layout = layout
        self.module_name = "harmonized"

    def export_wide(
        self,
        hd: HarmonizedData,
        meta,
        input_path: Path,
        formats: Sequence[Format],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out = {}
        opts = opts or WriterOptions()
        for fmt in formats:
            if fmt not in WIDE_FORMATS:
                raise ValueError(f"Unsupported wide fmt: {fmt}")
            ctx = plan_paths(
                base_out=self.base_out,
                module=self.module_name,
                trial=meta.trial,
                run_id=meta.run_id,
                stem="harmonized_wide",
                fmt=fmt,
                layout=self.layout,
                started_at=meta.started_at,
            )
            if fmt in {"csv", "tsv", "parquet"}:
                df = hd.to_dataframe_wide()
                result = write_frame(df, ctx.data_path, fmt, opts.csv if fmt == "csv" else (opts.tsv if fmt == "tsv" else opts.parquet))
            elif fmt == "json":
                result = write_json_text(hd.to_json(indent=opts.json.indent), ctx.data_path, opts.json)
            else:  # ndjson
                result = write_ndjson(hd.to_dict().get("patients", []), ctx.data_path, opts.ndjson)

            manifest = build_manifest(
                trial=meta.trial,
                run_id=meta.run_id,
                started_at=meta.started_at,
                input_path=input_path,
                directory=ctx.base_dir,
                fmt=fmt,
                mode="wide",
                result=result,
            )
            write_manifest(manifest, ctx.manifest_path)
            out[fmt] = ctx
        return out

    def export_normalized(
        self,
        hd: HarmonizedData,
        meta,
        input_path: Path,
        formats: Sequence[Format],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out = {}
        opts = opts or WriterOptions()
        frames = hd.to_frames_normalized()
        for fmt in formats:
            if fmt not in NORMALIZED_FORMATS:
                raise ValueError("Normalized supports csv/tsv/parquet only")
            ctx = plan_paths(
                base_out=self.base_out,
                module=self.module_name,
                trial=meta.trial,
                run_id=meta.run_id,
                stem="harmonized_norm",
                fmt=fmt,
                layout=self.layout,
                started_at=meta.started_at,
            )
            result = write_frames_dir(frames, ctx.data_dir, fmt, opts.csv if fmt == "csv" else (opts.tsv if fmt == "tsv" else opts.parquet))
            manifest = build_manifest(
                trial=meta.trial,
                run_id=meta.run_id,
                started_at=meta.started_at,
                input_path=input_path,
                directory=ctx.base_dir,
                fmt=fmt,
                mode="normalized",
                result=result,
            )
            write_manifest(manifest, ctx.manifest_path)
            out[fmt] = ctx
        return out


# # harmonization/io_export.py
# from __future__ import annotations
# from pathlib import Path
# from typing import Optional
# import json
# import polars as pl
#
# from ..datamodels import HarmonizedData
# from ...infra.run_context import RunMetadata
# from ...infra.io_core import (
#     OutputPath,
#     resolve_output_path,
#     write_single_frame,
#     write_frames_dir,
#     write_manifest,
# )
#
#
# class HarmonizedOutputManager:
#     DEFAULT_BASE_DIR = Path(".data/harmonized")
#
#     def __init__(self, base_dir: Optional[Path] = None, default_format: str = "csv"):
#         self.base_dir = base_dir or self.DEFAULT_BASE_DIR
#         self.default_format = default_format
#
#     def write_wide(
#         self,
#         hd: HarmonizedData,
#         ctx: RunMetadata,
#         input_path: Path,
#         output: Optional[Path] = None,
#         fmt: Optional[str] = None,
#         options: Optional[dict] = None,
#     ) -> OutputPath:
#         op = resolve_output_path(
#             trial=ctx.trial,
#             timestamp=ctx.started_at,
#             run_id=ctx.run_id,
#             base_dir=self.base_dir,
#             output=output,
#             fmt=fmt or self.default_format,
#             filename_stem="harmonized_wide",
#         )
#
#
#         df = hd.to_dataframe_wide()
#         # wide supports csv/tsv/parquet; json/ndjson from hd.to_dict()
#         if op.format in {"csv", "tsv", "parquet"}:
#             write_single_frame(df, op.data_file, op.format)
#             tables = {"wide": df}
#         elif op.format == "json":
#             op.data_file.write_text(hd.to_json(indent=2), encoding="utf-8")
#             tables = {"wide": pl.DataFrame({"rows": [len(hd.patients)]})}
#         elif op.format == "ndjson":
#             with op.data_file.open("w", encoding="utf-8") as fp:
#                 for obj in hd.to_dict().get("patients", []):
#                     fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
#             tables = {"wide": pl.DataFrame({"rows": [len(hd.patients)]})}
#         else:
#             raise ValueError(f"Unsupported wide format: {op.format}")
#
#         write_manifest(
#             op.manifest_file,
#             trial=ctx.trial,
#             timestamp=ctx.started_at,
#             run_id=ctx.run_id,
#             input_path=input_path,
#             output_file=op.data_file,
#             directory=op.directory,
#             fmt=op.format,
#             mode="wide",
#             tables=tables,
#             options=options,
#         )
#         return op
#
#     def write_normalized_dir(
#         self,
#         hd: HarmonizedData,
#         ctx: RunMetadata,
#         input_path: Path,
#         output: Optional[Path] = None,
#         fmt: Optional[str] = None,
#         options: Optional[dict] = None,
#     ) -> OutputPath:
#         op = resolve_output_path(
#             trial=ctx.trial,
#             timestamp=ctx.started_at,
#             run_id=ctx.run_id,
#             base_dir=self.base_dir,
#             output=output,
#             fmt=fmt or self.default_format,
#             filename_stem="harmonized_norm",
#         )
#
#         if op.format not in {"csv", "tsv", "parquet"}:
#             raise ValueError("Normalized export supports csv/tsv/parquet only.")
#
#         frames = hd.to_frames_normalized()
#         files = write_frames_dir(frames, op.directory, op.format)
#         # set main data_file for convenience
#         main_table = "patients" if "patients" in files else (next(iter(files.values())) if files else op.data_file)
#         if isinstance(main_table, Path):
#             op = OutputPath(data_file=main_table, manifest_file=op.manifest_file, log_file=op.log_file, directory=op.directory, format=op.format)
#
#         write_manifest(
#             op.manifest_file,
#             trial=ctx.trial,
#             timestamp=ctx.started_at,
#             run_id=ctx.run_id,
#             input_path=input_path,
#             output_file=op.data_file,
#             directory=op.directory,
#             fmt=op.format,
#             mode="normalized",
#             tables=frames,
#             table_files=files,
#             options=options,
#         )
#         return op
