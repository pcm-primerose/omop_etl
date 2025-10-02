from pathlib import Path
import polars as pl
from typing import Optional, Sequence, List

from omop_etl.infra.io.types import Formats
from omop_etl.infra.io.path_planner import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from .datamodels import HarmonizedData
from .harmonizers.impress import ImpressHarmonizer
from .core.io_export import HarmonizedExporter
from omop_etl.infra.utils.registry import get_registered_trials


class HarmonizationService:
    def __init__(self, outdir: Path, layout: Layout = Layout.TRIAL_RUN, out_manager: Optional[HarmonizedExporter] = None):
        self.out = out_manager or HarmonizedExporter(base_out=outdir, layout=layout)

    def run(
        self,
        trial: str,
        input_path: Path,
        formats: Sequence[Formats.Format] = ("csv",),
        write_wide: bool = True,
        write_normalized: bool = True,
        meta: Optional[RunMetadata] = None,
    ) -> HarmonizedData:
        meta = meta or RunMetadata.create(trial)
        if trial.upper() != get_registered_trials():
            raise KeyError(f"No harmonizer for trial '{trial}'")

        df = pl.read_csv(input_path)
        hd = ImpressHarmonizer(df, trial_id=trial.upper()).process()

        fmts = _expand_formats(formats)
        wide_fmts = _formats_for_mode(fmts, mode="wide")
        norm_fmts = _formats_for_mode(fmts, mode="normalized")

        if write_wide and wide_fmts:
            self.out.export_wide(hd, meta=meta, input_path=input_path, formats=wide_fmts)
        if write_normalized and norm_fmts:
            self.out.export_normalized(hd, meta=meta, input_path=input_path, formats=norm_fmts)

        return hd


def _expand_formats(formats: Formats.Format | Sequence[Formats.Format]) -> List[Formats.Format]:
    if isinstance(formats, str):
        return [formats]
    return list(formats)


def _formats_for_mode(formats: Sequence[Formats.Format], mode: str) -> List[Formats.Format]:
    allowed = Formats.WIDE_FORMATS if mode == "wide" else Formats.NORMALIZED_FORMATS
    out: List[Formats.Format] = []
    for f in formats:
        if f == "all":
            out.extend(allowed)
        elif f in allowed:
            out.append(f)
        else:
            raise ValueError(f"Format {f} not in supported formats: {Formats.Format}")

    # order without duplicates
    seen = set()
    ordered = []
    for f in allowed:
        if f in out and f not in seen:
            ordered.append(f)
            seen.add(f)
    return ordered
