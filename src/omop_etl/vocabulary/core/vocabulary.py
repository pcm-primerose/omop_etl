from collections.abc import Iterable, Mapping
from logging import getLogger
from types import MappingProxyType
from typing import Self

from omop_etl.vocabulary.core.helpers import validity_from_invalid_reason
from omop_etl.vocabulary.core.models import AthenaConcept

log = getLogger(__name__)

ConceptsById = Mapping[int, AthenaConcept]


class Vocabulary:
    """
    Store mappings of mapping files' concept_id to `AthenaConcept` from the Atena bundle.

    Mapping files store only `concept_id`, this layer hydrates name/domain/vocabulary/
    validity from the subset generated from Athena concepts, so vocabulary metadata is
    loaded from the concept database instead of hardcoded.
    """

    def __init__(self, concepts: ConceptsById) -> None:
        # MappingProxyType over a copy: callers can't mutate this Vocabulary's
        # concepts later by mutating the dict/mapping they originally passed in
        self._concepts: ConceptsById = MappingProxyType(dict(concepts))

    def hydrate(self, concept_id: int) -> AthenaConcept | None:
        """Return the concept attributes, or None if the concept is absent."""
        return self._concepts.get(concept_id)

    def __contains__(self, concept_id: object) -> bool:
        return concept_id in self._concepts

    def __len__(self) -> int:
        return len(self._concepts)

    @classmethod
    def from_concept_rows(cls, rows: Iterable[Mapping[str, str | None]]) -> Self:
        concepts: dict[int, AthenaConcept] = {}

        for row in rows:
            mapped = cls._concept_row_to_mapped(row)

            existing = concepts.get(mapped.concept_id)
            if existing is None:
                concepts[mapped.concept_id] = mapped
                continue

            if existing != mapped:
                log.warning("Duplicate concept_id %s with differing attributes: %s existing vs %s", mapped.concept_id, existing, mapped)

            log.debug("Ignoring duplicate identical concept_id %s", mapped.concept_id)

        return cls(concepts)

    @staticmethod
    def _concept_row_to_mapped(row: Mapping[str, str | None]) -> AthenaConcept:
        invalid_reason = Vocabulary._cell(row, "invalid_reason")

        return AthenaConcept(
            concept_id=int(Vocabulary._cell(row, "concept_id")),
            concept_code=Vocabulary._cell(row, "concept_code"),
            concept_name=Vocabulary._cell(row, "concept_name"),
            domain_id=Vocabulary._cell(row, "domain_id"),
            vocabulary_id=Vocabulary._cell(row, "vocabulary_id"),
            validity=validity_from_invalid_reason(invalid_reason),
        )

    @staticmethod
    def _cell(row: Mapping[str, str | None], column: str) -> str:
        return (row.get(column) or "").strip()
