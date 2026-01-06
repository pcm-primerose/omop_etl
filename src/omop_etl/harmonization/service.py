from pathlib import Path
from typing import Sequence, Optional, List
from omop_etl.infra.io.types import (
    Layout,
    AnyFormatToken,
    WideFormat,
    TabularFormat,
    WIDE_FORMATS,
    TABULAR_FORMATS,
)
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.format_utils import expand_formats
from omop_etl.harmonization.core.dispatch import resolve_harmonizer
from omop_etl.harmonization.models import HarmonizedData
from omop_etl.harmonization.core.pipeline import (
    HarmonizationPipeline,
    HarmonizerResolver,
)


class HarmonizationService:
    def __init__(
        self,
        outdir: Path,
        layout: Layout = Layout.TRIAL_RUN,
        harmonizer_resolver: Optional[HarmonizerResolver] = None,
    ):
        self.outdir = outdir
        self.layout = layout
        self._resolver = harmonizer_resolver or resolve_harmonizer

    def run(
        self,
        trial: str,
        input_path: Path,
        meta: RunMetadata,
        formats: AnyFormatToken | Sequence[AnyFormatToken] = "csv",
        write_wide: bool = True,
        write_normalized: bool = True,
    ) -> HarmonizedData:
        # normalize formats
        wide_fmts: List[WideFormat] = expand_formats(formats, allowed=WIDE_FORMATS)
        tab_fmts: List[TabularFormat] = expand_formats(formats, allowed=TABULAR_FORMATS)

        pipeline = HarmonizationPipeline(
            trial=trial,
            meta=meta,
            outdir=self.outdir,
            layout=self.layout,
            resolver=self._resolver,
        )
        return pipeline.run(
            input_path=input_path,
            wide_formats=wide_fmts,
            tabular_formats=tab_fmts,
            write_wide=write_wide,
            write_normalized=write_normalized,
        )
