from pathlib import Path
from typing import Dict, Sequence
import logging
import polars as pl

from omop_etl.infra.io.types import Layout, WideFormat, WIDE_FORMATS
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.io_core import write_frame, write_manifest, WriterResult
from omop_etl.infra.io.path_planner import plan_single_file, WriterContext
from omop_etl.infra.logging.scoped import file_logging
from omop_etl.infra.logging.adapters import with_extra

log = logging.getLogger(__name__)


class PreprocessExporter:
    """
    Run-centric writer for preprocessing outputs (single wide file per format).

    Directory shape:
      <base>/runs/<started_at>_<run_id>/preprocessed/<trial>/preprocessed/<fmt>/
        <trial>_<run>_<ts>_preprocessed.<ext>
        <trial>_<run>_<ts>_preprocessed_manifest.json
        <trial>_<run>_<ts>_preprocessed.log
    """

    def __init__(self, base_out: Path, layout: Layout = Layout.TRIAL_RUN):
        self.base_out = base_out
        self.layout = layout

    def export_wide(
        self,
        df: pl.DataFrame,
        meta,
        input_path: Path,
        formats: Sequence[WideFormat],
        opts: WriterOptions | None = None,
    ) -> Dict[WideFormat, WriterContext]:
        opts = opts or WriterOptions()
        out: Dict[WideFormat, WriterContext] = {}

        # enforce only allowed tokens
        for fmt in formats:
            if fmt not in WIDE_FORMATS:
                raise ValueError(f"Unsupported wide fmt: {fmt}")

        for fmt in formats:
            # plan run-centric paths
            ctx = plan_single_file(
                base_out=self.base_out,
                meta=meta,
                module="preprocessed",
                trial=meta.trial,
                dataset="preprocessed",
                fmt=fmt,
                filename_base="{trial}_{run_id}_{started_at}_{mode}",
            )
            ctx.base_dir.mkdir(parents=True, exist_ok=True)

            # per-format scoped logfile
            with file_logging(ctx.log_path) as root_logger:
                run_log = with_extra(
                    root_logger,
                    {
                        "trial": meta.trial,
                        "run_id": meta.run_id,
                        "timestamp": meta.started_at,
                        "fmt": fmt,
                        "mode": "preprocessed",
                        "component": "preprocessed",
                    },
                )
                run_log.info(
                    "preprocess.export_wide.start",
                    extra={"input": str(input_path), "output_dir": str(ctx.base_dir)},
                )

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

                run_log.info(
                    "preprocess.export_wide.done",
                    extra={
                        "rows": df.height,
                        "cols": df.width,
                        "data_path": str(ctx.data_path),
                        "manifest_path": str(ctx.manifest_path),
                        "log_path": str(ctx.log_path),
                    },
                )

            out[fmt] = ctx

        return out
