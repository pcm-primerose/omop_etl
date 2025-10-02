from __future__ import annotations
from pathlib import Path
from typing import Optional
from logging import getLogger

from .models import EcrfConfig, PreprocessingRunOptions, PreprocessResult
from .io_load import InputResolver
from .combine import combine
from .io_export import OutputManager
from ...infra.utils.run_context import RunMetadata
from .dispatch import resolve_processor

log = getLogger(__name__)


class PreprocessingPipeline:
    def __init__(
        self,
        trial: str,
        ecrf_config: EcrfConfig,
        output_manager: Optional[OutputManager] = None,
        processor: Optional[resolve_processor] = None,
    ):
        self.trial = trial
        self.ecrf_config = ecrf_config
        self.output_manager = output_manager or OutputManager()
        self._processor = processor

    def run(
        self,
        input_path: Path,
        options: Optional[PreprocessingRunOptions] = None,
        output: Optional[Path] = None,
        format: Optional[str] = None,
        combine_key: str = "SubjectId",
    ) -> PreprocessResult:
        """
        Executes preprocessing pipeline.

        Args:
            input_path: Input Excel file or CSV dir
            options: Optional run options (RunOptions)
            output: Optional output path override
            format: Optional format override
            combine_key: Key to join sheets on

        Returns:
            PreprocessResult with output details
        """
        # create run context
        ctx = RunMetadata.create(self.trial)

        # get trial from registry
        processor = self._get_processor()

        log.info("Pipeline started", extra={**ctx.as_dict(), "input": str(input_path)})

        # load data
        resolver = InputResolver()
        ecrf = resolver.resolve(input_path, self.ecrf_config)
        ecrf.trial = self.trial

        # combine sheets
        df = combine(ecrf, on=combine_key)

        # apply trial-specific processing
        df = processor(df, ecrf, options or PreprocessingRunOptions())

        # write output
        output_path = self.output_manager.write(
            df=df,
            ctx=ctx,
            input_path=input_path,
            output=output,
            fmt=format,
            options=vars(options) if options else None,
        )

        log.info(
            "Pipeline completed",
            extra={
                **ctx.as_dict(),
                "output": str(output_path.data_file),
                "rows": df.height,
                "columns": df.width,
            },
        )

        return PreprocessResult(output_path=output_path, rows=df.height, columns=df.width, context=ctx)

    def _get_processor(self) -> resolve_processor:
        return self._processor or resolve_processor(self.trial)
