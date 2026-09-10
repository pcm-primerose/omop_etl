from dataclasses import dataclass
from pathlib import Path
from typing import Literal


ValidationIssueKind = Literal[
    "malformed",
    "missing",
    "non_standard",
    "invalid",
    "drift",
    "duplicate_key",
]


@dataclass(frozen=True, slots=True)
class AthenaConcept:
    """
    A concept's attributes as Athena defines them, keyed by concept_id.
    """

    concept_id: int
    concept_code: str
    concept_name: str
    domain_id: str
    vocabulary_id: str
    validity: str


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    file: Path
    concept_id: int | None  # None for "malformed" (no row to point at)
    kind: ValidationIssueKind
    column: str | None
    detail: str


@dataclass(frozen=True, slots=True)
class ValidationReport:
    errors: tuple[ValidationIssue, ...]
    athena_version: str

    @property
    def is_clean(self) -> bool:
        return not self.errors


@dataclass(frozen=True, slots=True)
class FlaggedConcept:
    concept_id: int
    reason: str  # non-standard or invalid


@dataclass(frozen=True, slots=True)
class ConceptSubsetReport:
    written_count: int
    missing_concept_ids: frozenset[int]
    flagged_concepts: tuple[FlaggedConcept, ...]
