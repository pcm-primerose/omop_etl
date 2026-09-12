from dataclasses import dataclass
from pathlib import Path
import polars as pl

from omop_etl.infra.utils.mapping_io import MAPPING_CONCEPT_COLUMNS


@dataclass(frozen=True, slots=True)
class MappingRow:
    """
    One mapping-file row (static/structural/semantic shape) for tests. Only the
    fields a test cares about need to be set, the rest default to a plausible
    standard/valid mapping. `value_set`/`source_value` are the file-specific identity
    columns, structural files have no `source_value` column, semantic files have no
    `value_set` column, so `write_*_mapping_csv` below selects only the columns each
    real file shape actually has.
    """

    concept_id: int
    concept_code: str = "93655004"
    concept_name: str = "Malignant melanoma"
    concept_class_id: str = "Clinical Finding"
    standard_concept: str = "Standard"
    validity: str = "Valid"
    domain_id: str = "Condition"
    vocabulary_id: str = "SNOMED"
    value_set: str = ""
    source_value: str = ""

    def as_row(self) -> dict[str, str]:
        row = {col: str(getattr(self, col)) for col in MAPPING_CONCEPT_COLUMNS}
        row["value_set"] = self.value_set
        row["source_value"] = self.source_value
        return row


def _write(path: Path, rows: tuple[MappingRow, ...], columns: list[str]) -> Path:
    pl.DataFrame([r.as_row() for r in rows]).select(columns).write_csv(path)
    return path


def write_static_mapping_csv(path: Path, *rows: MappingRow) -> Path:
    return _write(path, rows, ["value_set", "source_value", *MAPPING_CONCEPT_COLUMNS])


def write_structural_mapping_csv(path: Path, *rows: MappingRow) -> Path:
    return _write(path, rows, ["value_set", *MAPPING_CONCEPT_COLUMNS])


def write_semantic_mapping_csv(path: Path, *rows: MappingRow) -> Path:
    return _write(path, rows, ["source_value", *MAPPING_CONCEPT_COLUMNS])
