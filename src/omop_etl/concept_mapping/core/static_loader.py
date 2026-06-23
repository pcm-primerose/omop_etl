import csv
from pathlib import Path
from logging import getLogger

from omop_etl.concept_mapping.core.models import (
    StaticConcept,
    MappedConcept,
)

log = getLogger(__name__)


class StaticMapLoader:
    def __init__(self, path: Path):
        self.path = path

    def as_rows(self) -> list[StaticConcept]:
        rows: list[StaticConcept] = []
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
                rows.append(StaticConcept.from_csv_row(row))
        return rows

    def as_index(self) -> dict[tuple[str, str], MappedConcept]:
        idx: dict[tuple[str, str], MappedConcept] = {}
        for r in self.as_rows():
            key = (r.value_set.lower().strip(), str(r.local_value).lower().strip())
            existing = idx.get(key)
            if existing is not None and existing.concept_id != r.concept_id:
                raise ValueError(
                    f"Duplicate static mapping for {key}: maps to both concept_id "
                    f"{existing.concept_id} and {r.concept_id}. A curated source value "
                    f"must map to exactly one concept: fix {self.path}."
                )
            idx[key] = r.to_mapped()
        return idx
