from pathlib import Path
from typing import Optional, Callable, Sequence, List, cast
import polars as pl

from .models import (
    EcrfConfig,
    PreprocessingRunOptions,
    PreprocessResult,
    OutputPath,
)
from .io_load import InputResolver
from .combine import combine
from ...infra.utils.run_context import RunMetadata
from .dispatch import resolve_processor
from omop_etl.preprocessing.core.io_export import PreprocessExporter
from omop_etl.infra.io.types import Layout, TabularFormat, NORMALIZED_FORMATS
from omop_etl.infra.io.options import WriterOptions
from omop_etl.infra.io.format_utils import expand_formats

Processor = Callable[[pl.DataFrame, EcrfConfig, PreprocessingRunOptions], pl.DataFrame]


class PreprocessingPipeline:
    def __init__(
        self,
        trial: str,
        ecrf_config: EcrfConfig,
        output_manager: Optional[PreprocessExporter] = None,
        processor: Optional[Processor] = None,
    ):
        self.trial = trial
        self.ecrf_config = ecrf_config
        self.exporter = output_manager or PreprocessExporter(base_out=Path(".data"), layout=Layout.TRIAL_RUN)
        self._processor = processor

    def run(
        self,
        input_path: Path,
        options: Optional[PreprocessingRunOptions] = None,
        formats: Sequence[TabularFormat] | TabularFormat | None = None,
        combine_key: str = PreprocessingRunOptions.combine_key,
    ) -> PreprocessResult:
        # meta
        run_meta = RunMetadata.create(self.trial)

        # resolve processor
        processor = self._processor or resolve_processor(self.trial)

        # load inputs
        resolver = InputResolver()
        ecrf = resolver.resolve(input_path, self.ecrf_config)
        ecrf.trial = self.trial

        df_combined = combine(ecrf, on=combine_key)
        df_out = processor(df_combined, ecrf, options or PreprocessingRunOptions())

        # normalize formats to concrete tabular formats
        fmts: List[TabularFormat] = cast(
            List[TabularFormat],
            expand_formats(formats if formats is not None else "csv", allowed=NORMALIZED_FORMATS, allow_all=True),
        )

        ctx_by_fmt = self.exporter.export_wide(
            df_out,
            meta=run_meta,
            input_path=input_path,
            formats=fmts,
            opts=WriterOptions(),
        )

        primary_fmt: TabularFormat = fmts[0]
        ctx = ctx_by_fmt[primary_fmt]

        out_path = OutputPath(
            data_file=ctx.data_path,
            manifest_file=ctx.manifest_path,
            log_file=ctx.log_path,
            directory=ctx.base_dir,
            format=primary_fmt,
        )

        return PreprocessResult(
            output_path=out_path,
            rows=df_out.height,
            columns=df_out.width,
            context=run_meta,
        )
