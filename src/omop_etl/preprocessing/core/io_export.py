import logging
from pathlib import Path
from typing import Dict, Sequence
import polars as pl
from logging import getLogger

from logging.handlers import RotatingFileHandler
from omop_etl.infra.io.types import Layout
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.io_core import write_frame, write_manifest, WriterResult
from omop_etl.infra.io.path_planner import plan_paths, WriterContext
from omop_etl.infra.io.format_utils import NORMALIZED_FORMATS, expand_formats
from omop_etl.infra.logging.logging_setup import add_file_handler

log = getLogger(__name__)


class PreprocessExporter:
    def __init__(self, base_out: Path, layout: Layout = Layout.TRIAL_RUN):
        self.base_out = base_out
        self.layout = layout
        self._file_handler: RotatingFileHandler | None = None

    def export_wide(
        self,
        df: pl.DataFrame,
        meta,
        input_path: Path,
        formats: Sequence[str],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        opts = opts or WriterOptions()
        fmts = expand_formats(formats, allowed=NORMALIZED_FORMATS)
        out: Dict[str, WriterContext] = {}

        name_template = "{trial}_{run_id}_{started_at}_{mode}"
        for fmt in fmts:
            ctx = plan_paths(
                base_out=self.base_out,
                module=None,
                trial=meta.trial,
                run_id=meta.run_id,
                stem="preprocessed",
                fmt=fmt,
                layout=self.layout,
                started_at=meta.started_at,
                include_stem_dir=False,
                name_template=name_template,
            )
            ctx.base_dir.mkdir(parents=True, exist_ok=True)

            self._attach_logfile(ctx, meta, input_path, fmt)

            wopts = opts.csv if fmt == "csv" else (opts.tsv if fmt == "tsv" else opts.parquet)
            result: WriterResult = write_frame(df, ctx.data_path, fmt, wopts)

            manifest = build_manifest(
                trial=meta.trial,
                run_id=meta.run_id,
                started_at=meta.started_at,
                input_path=input_path,
                directory=ctx.base_dir,
                fmt=fmt,
                mode="preprocessed",
                result=result,
            )
            write_manifest(manifest, ctx.manifest_path)

            log.info(
                "preprocess.export_wide.done",
                extra={
                    "trial": meta.trial,
                    "run_id": meta.run_id,
                    "started_at": meta.started_at,
                    "fmt": fmt,
                    "rows": df.height,
                    "cols": df.width,
                    "data_path": str(ctx.data_path),
                    "manifest_path": str(ctx.manifest_path),
                    "log_path": str(ctx.log_path),
                },
            )
            out[fmt] = ctx
            self._detach_logfile()
        return out

    def _attach_logfile(self, ctx: WriterContext, meta, input_path: Path, fmt: str) -> None:
        self._file_handler = add_file_handler(ctx.log_path, json_format=True, level=logging.DEBUG)
        log.info(
            "preprocess.export_wide.start",
            extra={
                "trial": meta.trial,
                "run_id": meta.run_id,
                "started_at": meta.started_at,
                "fmt": fmt,
                "input": str(input_path),
                "output_dir": str(ctx.base_dir),
                "log_file": str(ctx.log_path),
                "log_format": "json",
            },
        )

    def _detach_logfile(self) -> None:
        if self._file_handler:
            try:
                root = logging.getLogger()
                root.removeHandler(self._file_handler)
                self._file_handler.close()
            finally:
                self._file_handler = None
