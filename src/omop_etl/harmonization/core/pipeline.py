from pathlib import Path
from typing import Sequence, Callable, Optional
import polars as pl

from ..datamodels import HarmonizedData
from ..core.dispatch import resolve_harmonizer
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import Layout, WideFormat, TabularFormat
from ..core.io_export import HarmonizedExporter


HarmonizerResolver = Callable[[str], type]


class HarmonizationPipeline:
    def __init__(
        self,
        trial: str,
        meta: RunMetadata,
        exporter: Optional[HarmonizedExporter] = None,
        resolver: Optional[HarmonizerResolver] = None,
        layout: Layout = Layout.TRIAL_RUN,
        outdir: Optional[Path] = None,
    ):
        self.trial = trial
        self.meta = meta
        self._resolver = resolver or resolve_harmonizer
        self.exporter = exporter or HarmonizedExporter(
            base_out=(outdir if outdir is not None else Path(".data")),
            layout=layout,
        )

    def run(
        self,
        input_path: Path,
        wide_formats: Sequence[WideFormat],
        tabular_formats: Sequence[TabularFormat],
        write_wide: bool = True,
        write_normalized: bool = True,
    ) -> HarmonizedData:
        df = HarmonizationPipeline._read_input(input_path)
        harmonizer_cls = self._resolver(self.trial)
        harmonized_data: HarmonizedData = harmonizer_cls(
            df,
            trial_id=self.trial.upper(),
        ).process()

        if write_wide and wide_formats:
            self.exporter.export_wide(
                harmonized_data,
                meta=self.meta,
                input_path=input_path,
                formats=wide_formats,
            )

        if write_normalized and tabular_formats:
            self.exporter.export_normalized(
                harmonized_data,
                meta=self.meta,
                input_path=input_path,
                formats=tabular_formats,
            )

        return harmonized_data

    @staticmethod
    def _read_input(path: Path) -> pl.DataFrame:
        suf = path.suffix.lower()
        if suf == ".parquet":
            return pl.read_parquet(path)
        if suf == ".csv":
            return pl.read_csv(path)
        if suf == ".tsv":
            return pl.read_csv(path, separator="\t")
        raise ValueError(f"Unsupported input file type: {suf} for harmonization.")
