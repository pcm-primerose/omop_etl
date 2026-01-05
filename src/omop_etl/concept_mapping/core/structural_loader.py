import csv
from pathlib import Path

from omop_etl.concept_mapping.core.models import StructuralConcept


class StructuralMapLoader:
    def __init__(self, path: Path):
        self.path = path

    def as_rows(self) -> list[StructuralConcept]:
        rows: list[StructuralConcept] = []
        with open(self.path, "r", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(StructuralConcept.from_csv_row(row))
        return rows

    def as_index(self) -> dict[str, StructuralConcept]:
        idx: dict[str, StructuralConcept] = {}
        for r in self.as_rows():
            key = r.value_set
            idx[key] = r
        return idx
