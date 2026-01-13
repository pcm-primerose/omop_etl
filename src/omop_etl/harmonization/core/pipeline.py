import json
from pathlib import Path
from typing import Sequence, Callable
import polars as pl
from logging import getLogger

from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.harmonization.core.dispatch import resolve_harmonizer
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.core.exporter import HarmonizedExporter
from omop_etl.infra.io.types import (
    Layout,
    WideFormat,
    TabularFormat,
    NAME_TO_POLARS_DTYPE,
)


log = getLogger(__name__)

HarmonizerResolver = Callable[[str], type]


class HarmonizationPipeline:
    def __init__(
        self,
        trial: str,
        meta: RunMetadata,
        exporter: HarmonizedExporter | None = None,
        resolver: HarmonizerResolver | None = None,
        layout: Layout = Layout.TRIAL_RUN,
        outdir: Path | None = None,
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
        schema = HarmonizationPipeline._get_preprocessed_schema(input_path)
        df = HarmonizationPipeline._read_input(input_path, schema)

        harmonizer = self._resolver(self.trial)

        harmonized_data: HarmonizedData = harmonizer(
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
    def _read_input(path: Path, schema: pl.Schema | None = None) -> pl.DataFrame:
        suf = path.suffix.lower()

        if suf == ".parquet":
            if schema is None:
                return pl.read_parquet(path)
            if schema is not None:
                return pl.read_parquet(path, schema=schema)
        if suf == ".csv":
            if schema is None:
                return pl.read_csv(path, infer_schema_length=None)
            if schema is not None:
                return pl.read_csv(path, schema=schema)
        if suf == ".tsv":
            if schema is None:
                return pl.read_csv(path, separator="\t", infer_schema_length=None)
            if schema is not None:
                return pl.read_csv(path, separator="\t", schema=schema)
        raise ValueError(f"Unsupported input file type: {suf} for harmonization.")

    @staticmethod
    def _get_preprocessed_schema(path: Path) -> pl.Schema | None:
        manifest_file = list(path.parent.glob(pattern="*_manifest*.json"))
        if len(manifest_file) != 1:
            log.warning(f"Could not find manifest file in pre-processing input dir {path.parent}. Will infer schema from entire dataset.")
            return None

        with open(str(manifest_file[0]), "r") as f:
            loaded = json.load(f)
            schema = loaded["tables"]["wide"]["schema"]

        return _schema_from_manifest(schema)


def _schema_from_manifest(manifest_schema: dict[str, str]) -> pl.Schema:
    """
    Convert {column_name: type_name} from JSON to pl.Schema.
    """
    out: dict[str, pl.DataType] = {}
    for col, type_name in manifest_schema.items():
        try:
            out[col] = NAME_TO_POLARS_DTYPE[type_name]  # todo: fix type warning
        except KeyError:
            raise ValueError(
                f"Unknown dtype name {type_name} for column {col} in manifest. Add it to NAME_TO_POLARS_DTYPE/POLARS_DTYPE_TO_NAME."
            )
    return pl.Schema(out)
