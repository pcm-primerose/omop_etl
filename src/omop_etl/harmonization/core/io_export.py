from pathlib import Path
from typing import Dict, Sequence
import logging

from ..datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout, WIDE_FORMATS, TABULAR_FORMATS, TabularFormat, WideFormat
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.io_core import write_frame, write_frames_dir, write_json, write_manifest
from omop_etl.infra.io.path_planner import plan_single_file, plan_table_dir, WriterContext
from omop_etl.infra.logging.scoped import file_logging
from omop_etl.infra.logging.adapters import with_extra

log = logging.getLogger(__name__)


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
        formats: Sequence[WideFormat],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out: Dict[str, WriterContext] = {}
        opts = opts or WriterOptions()

        for fmt in formats:
            if fmt not in WIDE_FORMATS:
                raise ValueError(f"Unsupported wide fmt: {fmt}")

            ctx = plan_single_file(
                base_out=self.base_out,
                meta=meta,
                module=self.module_name,
                trial=meta.trial,
                dataset="harmonized_wide",
                fmt=fmt,
                filename_base="{trial}_{run_id}_{started_at}_{mode}",
            )

            with file_logging(ctx.log_path) as root_logger:
                run_log = with_extra(
                    root_logger,
                    {
                        "trial": meta.trial,
                        "run_id": meta.run_id,
                        "timestamp": meta.started_at,
                        "fmt": fmt,
                        "mode": "wide",
                        "component": self.module_name,
                    },
                )
                run_log.info(
                    "harmonize.export_wide.start",
                    extra={"input": str(input_path), "output_dir": str(ctx.base_dir)},
                )

                if fmt in {"csv", "tsv"}:
                    df = hd.to_dataframe_wide()
                    opt = opts.csv if fmt == "csv" else opts.tsv
                    result = write_frame(df, ctx.data_path, fmt, opt)
                elif fmt == "parquet":
                    df = hd.to_dataframe_wide()
                    result = write_frame(df, ctx.data_path, fmt, opts.parquet)
                elif fmt == "json":
                    result = write_json(hd.to_dict(), ctx.data_path, opts.json)
                else:
                    raise AssertionError(f"unhandled fmt: {fmt}")

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

                run_log.info(
                    "harmonize.export_wide.done",
                    extra={
                        "rows": (df.height if fmt != "json" else len(hd.patients)),
                        "cols": (df.width if fmt != "json" else None),
                        "data_path": str(ctx.data_path),
                        "manifest_path": str(ctx.manifest_path),
                        "log_path": str(ctx.log_path),
                    },
                )

            out[fmt] = ctx

        return out

    def export_normalized(
        self,
        hd: HarmonizedData,
        meta,
        input_path: Path,
        formats: Sequence[TabularFormat],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out: Dict[str, WriterContext] = {}
        opts = opts or WriterOptions()
        frames = hd.to_frames_normalized()

        for fmt in formats:
            if fmt not in TABULAR_FORMATS:
                raise ValueError(
                    f"Unsupported fmt:{fmt}. HarmonizedExporter.export_normalized supports: {TABULAR_FORMATS}",
                )

            ctx = plan_table_dir(
                base_out=self.base_out,
                meta=meta,
                module=self.module_name,
                trial=meta.trial,
                mode="harmonized_norm",
                fmt=fmt,
            )

            with file_logging(ctx.log_path) as root_logger:
                run_log = with_extra(
                    root_logger,
                    {
                        "trial": meta.trial,
                        "run_id": meta.run_id,
                        "timestamp": meta.started_at,
                        "fmt": fmt,
                        "mode": "normalized",
                        "component": self.module_name,
                    },
                )
                run_log.info(
                    "harmonize.export_normalized.start",
                    extra={"input": str(input_path), "output_dir": str(ctx.base_dir)},
                )

                if fmt == "csv":
                    result = write_frames_dir(frames, ctx.data_dir, fmt, opts.csv)
                elif fmt == "tsv":
                    result = write_frames_dir(frames, ctx.data_dir, fmt, opts.tsv)
                elif fmt == "parquet":
                    result = write_frames_dir(frames, ctx.data_dir, fmt, opts.parquet)
                else:
                    raise AssertionError(f"unhandled fmt: {fmt}")

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
                run_log.info(
                    "harmonize.export_normalized.done",
                    extra={
                        "tables": list(result.table_files.keys()),
                        "data_dir": str(ctx.data_dir),
                        "manifest_path": str(ctx.manifest_path),
                        "log_path": str(ctx.log_path),
                    },
                )

            out[fmt] = ctx

        return out
