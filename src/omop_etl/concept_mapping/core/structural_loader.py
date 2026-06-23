import csv
from pathlib import Path
from logging import getLogger

from omop_etl.concept_mapping.core.models import (
    StructuralConcept,
    MappedConcept,
)

log = getLogger(__name__)


class StructuralMapLoader:
    def __init__(self, path: Path):
        self.path = path

    def as_rows(self) -> list[StructuralConcept]:
        rows: list[StructuralConcept] = []
        with open(self.path, "r", newline="") as f:
            for line_no, row in enumerate(csv.DictReader(f), start=2):
                none_cols = [k for k, v in row.items() if v is None]
                if none_cols:
                    log.warning(
                        "Malformed row in %s line %d: missing columns %s (row: %s)",
                        self.path,
                        line_no,
                        none_cols,
                        dict(row),
                    )
                rows.append(StructuralConcept.from_csv_row(row))
        return rows

    def as_index(self) -> dict[str, MappedConcept]:
        # value-less constants: keyed by value_set alone, lowercased and stripped to
        # match normalized values from from_csv_row
        idx: dict[str, MappedConcept] = {}
        for r in self.as_rows():
            key = r.value_set.lower().strip()
            idx[key] = r.to_mapped()
        return idx
