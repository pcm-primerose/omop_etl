from pathlib import Path
from typing import Optional, Sequence, List
import polars as pl

from ..datamodels import HarmonizedData
from ..core.dispatch import resolve_harmonizer
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import Format, Layout, WIDE_FORMATS, NORMALIZED_FORMATS
from omop_etl.infra.io.format_utils import expand_formats
from ..core.io_export import HarmonizedExporter


def run_harmonization(
    trial: str,
    input_path: Path,
    outdir: Path,
    layout: Layout = Layout.TRIAL_RUN,
    formats: Format | Sequence[Format] = "csv",
    write_wide: bool = True,
    write_normalized: bool = True,
    meta: Optional[RunMetadata] = None,
    # todo: add filtering later
    # filter_pred: Optional[Callable[[Patient], bool]] = None,
) -> HarmonizedData:
    meta = meta or RunMetadata.create(trial)

    df = pl.read_csv(input_path)
    harmonizer = resolve_harmonizer(trial)
    hd = harmonizer(df, trial_id=trial.upper()).process()

    # if filter_pred:
    #     hd = hd.filter(filter_pred)

    fmts: List[Format] = expand_formats(formats)
    wide_fmts = [f for f in fmts if f in WIDE_FORMATS]
    norm_fmts = [f for f in fmts if f in NORMALIZED_FORMATS]

    exporter = HarmonizedExporter(base_out=outdir, layout=layout)

    if write_wide and wide_fmts:
        exporter.export_wide(hd, meta=meta, input_path=input_path, formats=wide_fmts)
    if write_normalized and norm_fmts:
        exporter.export_normalized(hd, meta=meta, input_path=input_path, formats=norm_fmts)

    return hd
