from pathlib import Path
from typing import Dict, Sequence
import logging

from omop_etl.infra.io.types import Layout, WideFormat, WIDE_FORMATS
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.io_core import write_frame, write_manifest, write_json
from omop_etl.infra.io.path_planner import plan_single_file, WriterContext
from omop_etl.infra.logging.scoped import file_logging
from omop_etl.infra.logging.adapters import with_extra
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.semantic_mapping.core.models import BatchQueryResult

log = logging.getLogger(__name__)


class SemanticExporter:
    """
    Run-centric writer for semantic mapping outputs.

    Exports two files per format:
      - matches: queries with OMOP concept mappings
      - missing: queries that didn't match any concepts

    Directory shape:
      <base>/runs/<started_at>_<run_id>/semantic/<trial>/mapped/<fmt>/
        matches_{trial}_{run_id}_{ts}.{ext}
        missing_{trial}_{run_id}_{ts}.{ext}
        manifest.json
        run.log
    """

    def __init__(self, base_out: Path, layout: Layout = Layout.TRIAL_RUN):
        self.base_out = base_out
        self.layout = layout
        self.module_name = "semantic_mapped"

    def export(
        self,
        batch_result: BatchQueryResult,
        meta: RunMetadata,
        input_path: Path | None,
        formats: Sequence[WideFormat],
        opts: WriterOptions | None = None,
    ) -> Dict[str, WriterContext]:
        opts = opts or WriterOptions()
        out: Dict[str, WriterContext] = {}

        for fmt in formats:
            if fmt not in WIDE_FORMATS:
                raise ValueError(f"Unsupported fmt: {fmt}. Supported: {WIDE_FORMATS}")

        for fmt in formats:
            stem = f"{meta.trial}_{meta.run_id}_{meta.started_at}"
            ctx = plan_single_file(
                base_out=self.base_out,
                meta=meta,
                module=self.module_name,
                trial=meta.trial,
                mode="semantic_mapped",
                fmt=fmt,
                filename_base=stem,
            )
            ctx.base_dir.mkdir(parents=True, exist_ok=True)
            matches_path = ctx.base_dir / f"matches_{stem}.{fmt}"
            missing_path = ctx.base_dir / f"missing_{stem}.{fmt}"

            with file_logging(ctx.log_path) as root_logger:
                run_log = with_extra(
                    root_logger,
                    {
                        "trial": meta.trial,
                        "run_id": meta.run_id,
                        "timestamp": meta.started_at,
                        "fmt": fmt,
                        "mode": "mapped",
                        "component": self.module_name,
                    },
                )
                run_log.info(
                    "semantic.export.start",
                    extra={
                        "input": str(input_path) if input_path else None,
                        "output_dir": str(ctx.base_dir),
                    },
                )

                if fmt == "json":
                    matches_dict = batch_result.to_matches_dict()
                    missing_dict = batch_result.to_missing_dict()
                    matches_result = write_json(matches_dict, matches_path, opts.json)
                    write_json(missing_dict, missing_path, opts.json)
                    matches_count = len(matches_dict)
                    missing_count = len(missing_dict)
                elif fmt in {"csv", "tsv"}:
                    matches_df = batch_result.to_matches_df()
                    missing_df = batch_result.to_missing_df()
                    format_opts = opts.csv if fmt == "csv" else opts.tsv
                    matches_result = write_frame(matches_df, matches_path, fmt, format_opts)
                    write_frame(missing_df, missing_path, fmt, format_opts)
                    matches_count = matches_df.height
                    missing_count = missing_df.height
                elif fmt == "parquet":
                    matches_df = batch_result.to_matches_df()
                    missing_df = batch_result.to_missing_df()
                    matches_result = write_frame(matches_df, matches_path, fmt, opts.parquet)
                    write_frame(missing_df, missing_path, fmt, opts.parquet)
                    matches_count = matches_df.height
                    missing_count = missing_df.height
                else:
                    raise AssertionError(f"unhandled fmt: {fmt}")

                coverage_stats = batch_result.coverage_by_field_path()
                manifest = build_manifest(
                    trial=meta.trial,
                    run_id=meta.run_id,
                    started_at=meta.started_at,
                    input_path=input_path or Path("."),
                    directory=ctx.base_dir,
                    fmt=fmt,
                    mode="mapped",
                    result=matches_result,
                    options={
                        "matches_file": str(matches_path),
                        "matches_rows": matches_count,
                        "missing_file": str(missing_path),
                        "missing_rows": missing_count,
                        "total_queries": len(batch_result.results),
                        "coverage_by_field": coverage_stats,
                    },
                )
                write_manifest(manifest, ctx.manifest_path)

                run_log.info(
                    "semantic.export.done",
                    extra={
                        "matches_rows": matches_count,
                        "missing_rows": missing_count,
                        "matches_path": str(matches_path),
                        "missing_path": str(missing_path),
                        "manifest_path": str(ctx.manifest_path),
                        "log_path": str(ctx.log_path),
                    },
                )

            out[fmt] = ctx

        return out
