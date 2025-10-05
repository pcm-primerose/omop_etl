from pathlib import Path
from typing import Sequence
from omop_etl.infra.io.types import Format, Layout
from omop_etl.infra.utils.run_context import RunMetadata
from .datamodels import HarmonizedData
from .core.pipeline import run_harmonization


class HarmonizationService:
    def __init__(self, outdir: Path, layout: Layout = Layout.TRIAL_RUN):
        self.outdir = outdir
        self.layout = layout

    def run(
        self,
        trial: str,
        input_path: Path,
        meta=RunMetadata,
        formats: Format | Sequence[Format] = "csv",
        write_wide: bool = True,
        write_normalized: bool = True,
        # filter_pred: Optional[Callable[[Patient], bool]] = None,  # add later
    ) -> HarmonizedData:
        return run_harmonization(
            trial=trial,
            input_path=input_path,
            outdir=self.outdir,
            layout=self.layout,
            formats=formats,
            write_wide=write_wide,
            write_normalized=write_normalized,
            meta=meta,
            # filter_pred=filter_pred,
        )
