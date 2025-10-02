from __future__ import annotations
from pathlib import Path
import polars as pl
from typing import Optional, Sequence

from .datamodels import HarmonizedData
from .harmonizers.impress import ImpressHarmonizer
from ..infra.path_planner import Layout
from ..infra.run_context import RunMetadata
from .core.io_export import HarmonizedExporter
from ..infra.types import Format


class HarmonizationService:
    def __init__(self, outdir: Path, layout: Layout = Layout.TRIAL_RUN, out_manager: Optional[HarmonizedExporter] = None):
        self.out = out_manager or HarmonizedExporter(base_out=outdir, layout=layout)

    def run(
        self,
        *,
        trial: str,
        input_path: Path,
        formats: Sequence[Format] = ("csv",),
        write_wide: bool = True,
        write_normalized: bool = True,
        meta: Optional[RunMetadata] = None,
    ) -> HarmonizedData:
        meta = meta or RunMetadata.create(trial)
        if trial.upper() != "IMPRESS":
            raise KeyError(f"No harmonizer for trial '{trial}'")

        df = pl.read_csv(input_path)
        hd = ImpressHarmonizer(df, trial_id=trial.upper()).process()

        if write_wide:
            self.out.export_wide(hd, meta=meta, input_path=input_path, formats=formats)
        if write_normalized:
            self.out.export_normalized(hd, meta=meta, input_path=input_path, formats=formats)

        return hd
