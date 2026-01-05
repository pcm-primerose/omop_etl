import csv
from pathlib import Path

from omop_etl.concept_mapping.core.models import StaticConcept


class StaticMapLoader:
    def __init__(self, path: Path):
        self.path = path

    def as_rows(self) -> list[StaticConcept]:
        rows: list[StaticConcept] = []
        with open(self.path, "r", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(StaticConcept.from_csv_row(row))
        return rows

    def as_index(self) -> dict[tuple[str, str], StaticConcept]:
        idx: dict[tuple[str, str], StaticConcept] = {}
        for r in self.as_rows():
            key = (r.value_set, str(r.local_value))
            idx[key] = r
        return idx
