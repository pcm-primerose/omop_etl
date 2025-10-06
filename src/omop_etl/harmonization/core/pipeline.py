from pathlib import Path
from typing import Sequence
import polars as pl

from ..datamodels import HarmonizedData
from ..core.dispatch import resolve_harmonizer
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import AnyFormatToken, Layout, WIDE_FORMATS, TABULAR_FORMATS, WideFormat, TabularFormat
from omop_etl.infra.io.format_utils import expand_formats
from ..core.io_export import HarmonizedExporter


def run_harmonization(
    trial: str,
    write_wide: bool,
    write_normalized: bool,
    input_path: Path,
    outdir: Path,
    formats: AnyFormatToken | Sequence[AnyFormatToken],
    layout: Layout = Layout.TRIAL_RUN,
    meta=RunMetadata,
    # add filtering later:
    # filter_pred: Optional[Callable[[Patient], bool]] = None,
) -> HarmonizedData:
    df = _read_input(input_path)
    harmonizer = resolve_harmonizer(trial)
    hd = harmonizer(df, trial_id=trial.upper()).process()

    wide_fmts: list[WideFormat] = expand_formats(formats, allowed=WIDE_FORMATS)
    tab_fmts: list[TabularFormat] = expand_formats(formats, allowed=TABULAR_FORMATS)

    exporter = HarmonizedExporter(base_out=outdir, layout=layout)

    if write_wide:
        exporter.export_wide(hd, meta=meta, input_path=input_path, formats=wide_fmts)
    if write_normalized:
        exporter.export_normalized(hd, meta=meta, input_path=input_path, formats=tab_fmts)

    return hd


def _read_input(path: Path) -> pl.DataFrame:
    suf = path.suffix.lower()
    if suf == ".parquet":
        return pl.read_parquet(path)
    if suf == ".csv":
        return pl.read_csv(path)
    if suf == ".tsv":
        return pl.read_csv(path, separator="\t")
    raise ValueError(f"Unsupported input file type: {suf} for harmonization.")
