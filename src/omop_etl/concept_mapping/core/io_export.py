from pathlib import Path
from typing import Dict, Sequence
from dataclasses import asdict
import logging

from omop_etl.infra.io.types import Layout, WideFormat, WIDE_FORMATS
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.manifest_builder import build_manifest
from omop_etl.infra.io.io_core import write_frame, write_manifest, write_json
from omop_etl.infra.io.path_planner import plan_single_file, WriterContext
from omop_etl.infra.logging.scoped import file_logging
from omop_etl.infra.logging.adapters import with_extra
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.concept_mapping.core.models import (
    LookupResult,
    MissedLookup,
    FieldCoverage,
)

import polars as pl

log = logging.getLogger(__name__)


class ConceptLookupExporter:
    """
    Run-centric writer for concept lookup outputs.

    Exports:
      - missed: lookups that didn't find a mapping
      - coverage stats per value_set

    Directory shape:
      <base>/runs/<started_at>_<run_id>/concept_lookup/<trial>/<fmt>/
        missed_{trial}_{run_id}_{ts}.{ext}
        manifest.json
        run.log
    """

    def __init__(self, base_out: Path, layout: Layout = Layout.TRIAL_RUN):
        self.base_out = base_out
        self.layout = layout
        self.module_name = "concept_lookup"

    def export(
        self,
        lookup_result: LookupResult,
        meta: RunMetadata,
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
                mode="concept_lookup",
                fmt=fmt,
                filename_base=stem,
            )
            ctx.base_dir.mkdir(parents=True, exist_ok=True)
            missed_path = ctx.base_dir / f"missed_{stem}.{fmt}"

            with file_logging(ctx.log_path) as root_logger:
                run_log = with_extra(
                    root_logger,
                    {
                        "trial": meta.trial,
                        "run_id": meta.run_id,
                        "timestamp": meta.started_at,
                        "fmt": fmt,
                        "mode": "concept_lookup",
                        "component": self.module_name,
                    },
                )
                run_log.info(
                    "concept_lookup.export.start",
                    extra={"output_dir": str(ctx.base_dir)},
                )

                # build missed data
                missed_list = lookup_result.missed_list()
                coverage_stats = self._coverage_to_dict(lookup_result.all_coverage())

                if fmt == "json":
                    missed_dict = [asdict(m) for m in missed_list]
                    missed_result = write_json(missed_dict, missed_path, opts.json)
                    missed_count = len(missed_dict)
                elif fmt in {"csv", "tsv"}:
                    missed_df = self._missed_to_df(missed_list)
                    format_opts = opts.csv if fmt == "csv" else opts.tsv
                    missed_result = write_frame(missed_df, missed_path, fmt, format_opts)
                    missed_count = missed_df.height
                elif fmt == "parquet":
                    missed_df = self._missed_to_df(missed_list)
                    missed_result = write_frame(missed_df, missed_path, fmt, opts.parquet)
                    missed_count = missed_df.height
                else:
                    raise AssertionError(f"unhandled fmt: {fmt}")

                manifest = build_manifest(
                    trial=meta.trial,
                    run_id=meta.run_id,
                    started_at=meta.started_at,
                    input_path=Path(".."),
                    directory=ctx.base_dir,
                    fmt=fmt,
                    mode="concept_lookup",
                    result=missed_result,
                    options={
                        "missed_file": str(missed_path),
                        "missed_count": missed_count,
                        "coverage_by_field": coverage_stats,
                    },
                )
                write_manifest(manifest, ctx.manifest_path)

                run_log.info(
                    "concept_lookup.export.done",
                    extra={
                        "missed_count": missed_count,
                        "missed_path": str(missed_path),
                        "manifest_path": str(ctx.manifest_path),
                        "log_path": str(ctx.log_path),
                    },
                )

            out[fmt] = ctx

        return out

    @staticmethod
    def _missed_to_df(missed: list[MissedLookup]) -> pl.DataFrame:
        rows = [
            {
                "lookup_type": m.lookup_type,
                "value_set": m.value_set,
                "local_value": m.local_value,
            }
            for m in missed
        ]
        return pl.DataFrame(rows) if rows else pl.DataFrame(schema={"lookup_type": str, "value_set": str, "local_value": str})

    @staticmethod
    def _coverage_to_dict(coverage: Dict[str, FieldCoverage]) -> Dict[str, Dict]:
        return {
            vs: {
                "lookup_type": fc.lookup_type,
                "matched": fc.matched,
                "missed": fc.missed,
                "total": fc.total,
                "coverage_fraction": fc.coverage_fraction,
            }
            for vs, fc in coverage.items()
        }
