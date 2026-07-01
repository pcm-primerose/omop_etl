import csv
from collections.abc import Iterable, Mapping, Sequence
from logging import getLogger
from pathlib import Path
from types import MappingProxyType
from typing import Self

from omop_etl.concept_mapping.core.models import MappedConcept

log = getLogger(__name__)


CONCEPT_COLUMNS = (
    "concept_id",
    "concept_name",
    "domain_id",
    "vocabulary_id",
    "concept_class_id",
    "standard_concept",
    "concept_code",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
)

_REQUIRED_COLUMNS = frozenset(CONCEPT_COLUMNS)


class Vocabulary:
    """
    Store mappings of concept_id to `MappedConcept`.

    Mapping files store only `concept_id`, this layer hydrates name/domain/vocabulary/
    validity from the subset generated from Athena concepts, so vocabulary metadata is loaed
    from concept database instead of hardcoded.
    """

    def __init__(self, concepts: Mapping[int, MappedConcept]) -> None:
        self._concepts: Mapping[int, MappedConcept] = MappingProxyType(dict(concepts))

    def hydrate(self, concept_id: int) -> MappedConcept | None:
        """Return the concept attributes, or None if the concept is absent."""
        return self._concepts.get(concept_id)

    def __contains__(self, concept_id: object) -> bool:
        return concept_id in self._concepts

    def __len__(self) -> int:
        return len(self._concepts)

    @classmethod
    def from_concept_rows(cls, rows: Iterable[Mapping[str, str | None]]) -> Self:
        concepts: dict[int, MappedConcept] = {}

        for row in rows:
            mapped = _concept_row_to_mapped(row)

            existing = concepts.get(mapped.concept_id)
            if existing is None:
                concepts[mapped.concept_id] = mapped
                continue

            if existing != mapped:
                log.warning("Duplicate concept_id %s with differing attributes: %s existing vs %s", mapped.concept_id, existing, mapped)

            log.debug("Ignoring duplicate identical concept_id %s", mapped.concept_id)

        return cls(concepts)

    @classmethod
    def from_csv(cls, path: Path) -> Self:
        """Load a tab-delimited OMOP CONCEPT subset file."""
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            _validate_header(reader.fieldnames, path)
            return cls.from_concept_rows(reader)


def _validate_header(fieldnames: Sequence[str] | None, path: Path) -> None:
    if fieldnames is None:
        raise ValueError(f"Empty CONCEPT file: {path}")

    columns = set(fieldnames)
    missing = _REQUIRED_COLUMNS - columns
    if missing:
        raise ValueError(f"CONCEPT file {path} is missing required columns: {sorted(missing)}")


def _concept_row_to_mapped(row: Mapping[str, str | None]) -> MappedConcept:
    invalid_reason = _cell(row, "invalid_reason")

    return MappedConcept(
        concept_id=int(_cell(row, "concept_id")),
        concept_code=_cell(row, "concept_code"),
        concept_name=_cell(row, "concept_name"),
        domain_id=_cell(row, "domain_id"),
        vocabulary_id=_cell(row, "vocabulary_id"),
        validity="valid" if invalid_reason == "" else "invalid",
    )


def _cell(row: Mapping[str, str | None], column: str) -> str:
    return (row.get(column) or "").strip()
