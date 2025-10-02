from pathlib import Path
from typing import Sequence, Dict
from ..datamodels import HarmonizedData
from omop_etl.infra.io.types import Formats, Layout
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.path_planner import (
    plan_paths,
    WriterContext,
)
from omop_etl.infra.io.io_core import (
    write_frame,
    write_frames_dir,
    write_json,
    write_manifest,
)


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
        formats: Sequence[Formats.Format],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out = {}
        opts = opts or WriterOptions()
        for fmt in formats:
            if fmt not in Formats.WIDE_FORMATS:
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

            if fmt not in Formats.WIDE_FORMATS:
                raise ValueError(f"Unsupported fmt:{fmt}. HarmonizedExporter.export_wide supports {Formats.WIDE_FORMATS}")
            if fmt in {"csv", "tsv"}:
                df = hd.to_dataframe_wide()
                result = write_frame(df, ctx.data_path, fmt, opts.csv)
            if fmt == "parquet":
                df = hd.to_dataframe_wide()
                result = write_frame(df, ctx.data_path, fmt, opts.parquet)
            elif fmt == "json":
                result = write_json(hd.to_dict(), ctx.data_path, opts.json)

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
        formats: Sequence[Formats.Format],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        out = {}
        opts = opts or WriterOptions()
        frames = hd.to_frames_normalized()
        for fmt in formats:
            if fmt not in Formats.NORMALIZED_FORMATS:
                raise ValueError(f"Unsupported fmt:{fmt}. HarmonizedExporter.export_wide supports {Formats.NORMALIZED_FORMATS}")

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
