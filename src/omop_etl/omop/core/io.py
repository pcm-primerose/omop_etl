from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import ClassVar, get_origin, get_type_hints, Any, cast
import polars as pl

from omop_etl.infra.io.path_planner import run_root
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.omop.models.tables import OmopTables


class OmopTableExporter:
    """
    Run-centric writer for the built OMOP tables:
    one CSV per populated table, under <base>/runs/<started_at>_<run_id>/omop/<TABLE_NAME>.csv,
    the files the DB loader's COPY step reads.

    NULLs are written as bare, unquoted empty fields (polars' default) and all upstream models
    are fully type-safe and nothing produces ""-fields, and format csv is used on load.
    """

    def __init__(self, base_out: Path):
        self.base_out = base_out

    def output_dir(self, meta: RunMetadata) -> Path:
        out_dir = run_root(self.base_out, meta) / "omop"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def write(self, tables: OmopTables, meta: RunMetadata) -> dict[str, Path]:
        out_dir = self.output_dir(meta)
        written: dict[str, Path] = {}
        for table_name in sorted(OmopTables.values()):
            rows = tables.get(table_name)
            if not rows:
                continue
            path = out_dir / f"{table_name.upper()}.csv"
            _rows_to_dataframe(rows).write_csv(path)
            written[table_name] = path
        return written


def _rows_to_dataframe(rows: list[object]) -> pl.DataFrame:
    columns = _row_columns(rows[0])
    return pl.DataFrame({col: [getattr(row, col) for row in rows] for col in columns})


def _row_columns(row: object) -> tuple[str, ...]:
    """Dataclass field names, excluding ClassVar and private fields."""
    if not is_dataclass(row) or isinstance(row, type):
        raise TypeError(f"Expected a dataclass instance, got: {type(row)}")

    hints = get_type_hints(type(row), include_extras=True)

    return tuple(f.name for f in fields(cast(Any, row)) if not f.name.startswith("_") and get_origin(hints.get(f.name)) is not ClassVar)
