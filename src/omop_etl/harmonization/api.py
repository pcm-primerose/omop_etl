from __future__ import annotations
from pathlib import Path
import polars as pl
from typing import Optional

from .datamodels import HarmonizedData
from .harmonizers.impress import ImpressHarmonizer
from ..infra.run_context import RunContext
from .core.io_export import HarmonizedOutputManager


class HarmonizationService:
    def __init__(self, out_manager: Optional[HarmonizedOutputManager] = None):
        self.out = out_manager or HarmonizedOutputManager()

    def run(
        self,
        *,
        trial: str,
        input_path: Path,
        output_format: str = "csv",
        output_dir: Optional[Path] = None,
        write_wide: bool = True,
        write_normalized: bool = True,
        ctx: Optional[RunContext] = None,
    ) -> HarmonizedData:
        ctx = ctx or RunContext.create(trial)

        # TODO: replace with factory later
        if trial.upper() != "IMPRESS":
            raise KeyError(f"No harmonizer for trial '{trial}'")

        df = pl.read_csv(input_path)
        hd = ImpressHarmonizer(df, trial_id=trial.upper()).process()

        if write_wide:
            self.out.write_wide(hd, ctx=ctx, input_path=input_path, fmt=output_format, output=output_dir)
        if write_normalized:
            self.out.write_normalized_dir(hd, ctx=ctx, input_path=input_path, fmt=output_format, output=output_dir)

        return hd
