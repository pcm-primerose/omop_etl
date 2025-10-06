from pathlib import Path
from typing import Optional, Callable, Sequence
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
from omop_etl.infra.io.types import Layout, TabularFormat
from omop_etl.infra.io.options import WriterOptions

Processor = Callable[[pl.DataFrame, EcrfConfig, PreprocessingRunOptions], pl.DataFrame]


class PreprocessingPipeline:
    def __init__(
        self,
        trial: str,
        ecrf_config: EcrfConfig,
        meta: RunMetadata,
        output_manager: Optional[PreprocessExporter] = None,
        processor: Optional[Processor] = None,
    ):
        self.trial = trial
        self.ecrf_config = ecrf_config
        self.exporter = output_manager or PreprocessExporter(base_out=Path(".data"), layout=Layout.TRIAL_RUN)
        self._processor = processor
        self.meta = meta

    def run(
        self,
        input_path: Path,
        run_options: PreprocessingRunOptions,
        formats: Sequence[TabularFormat],
        resolve: Callable[[str], type] = resolve_processor,
    ) -> PreprocessResult:
        # resolve processor
        processor = resolve(self.trial)

        # load inputs
        resolver = InputResolver()
        ecrf = resolver.resolve(input_path, self.ecrf_config)
        ecrf.trial = self.trial

        df_combined = combine(ecrf, on=run_options.combine_key)
        df_out = processor(df_combined, ecrf, run_options)

        ctx_by_fmt = self.exporter.export_wide(
            df_out,
            meta=self.meta,
            input_path=input_path,
            formats=formats,
            opts=WriterOptions(),
        )

        # TODO: fix primary format later
        #  (need better upstream organization)
        primary_fmt: TabularFormat = formats[0]
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
            context=self.meta,
        )
