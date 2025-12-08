from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple, List, Collection


@dataclass(frozen=True, slots=True)
class SemanticRow:
    term_id: str
    source_col: str
    source_term: str
    frequency: int
    omop_concept_id: str
    omop_concept_code: str
    omop_name: str
    omop_class: str
    omop_concept: str  # todo: rename in source
    omop_validity: str
    omop_domain: str
    omop_vocab: str

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "SemanticRow":
        return cls(
            term_id=row["term_id"].lower().strip(),
            source_col=row["source_col"].lower().strip(),
            source_term=row["source_term"].lower().strip(),
            frequency=int(row["frequency"]),
            omop_concept_id=row["omop_concept_id"].lower().strip(),
            omop_concept_code=row["omop_concept_code"].lower().strip(),
            omop_name=row["omop_name"].lower().strip(),
            omop_class=row["omop_class"].lower().strip(),
            omop_concept=row["omop_concept"].lower().strip(),
            omop_validity=row["omop_validity"].lower().strip(),
            omop_domain=row["omop_domain"].lower().strip(),
            omop_vocab=row["omop_vocab"].lower().strip(),
        )


class OmopDomain(str, Enum):
    CONDITION = "condition"
    DRUG = "drug"
    MEASUREMENTS = "measurement"
    PROCEDURE = "procedure"
    OBSERVATIONS = "observations"
    DEVICE = "device"


@dataclass(frozen=True, slots=True)
class QueryTarget:
    domains: Collection[OmopDomain] | None = None
    vocabs: Collection[str] | None = None
    concept_classes: Collection[str] | None = None
    standard_flags: Collection[str] | None = None
    validity: Collection[str] | None = None

    def __post_init__(self) -> None:
        def _fs(x):
            if x is None:
                return None
            if isinstance(x, frozenset):
                return x
            return frozenset(x)

        object.__setattr__(self, "domains", _fs(self.domains))
        object.__setattr__(self, "vocabs", _fs(self.vocabs))
        object.__setattr__(self, "concept_classes", _fs(self.concept_classes))
        object.__setattr__(self, "standard_flags", _fs(self.standard_flags))
        object.__setattr__(self, "validity", _fs(self.validity))


@dataclass(frozen=True, slots=True)
class FieldConfig:
    """What field used to constuct query"""

    # note: overkill for already mapped data, but reuse for lexical/vector search
    name: str
    field_path: tuple[str, ...]
    tags: Collection[str] = field(default_factory=frozenset)
    target: QueryTarget | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.tags, frozenset):
            object.__setattr__(self, "tags", frozenset(self.tags))


@dataclass(frozen=True, slots=True)
class Query:
    patient_id: str
    id: str
    query: str
    field_path: tuple[str, ...]
    raw_value: str
    leaf_index: None | int = None
    target: None | QueryTarget = None


@dataclass(frozen=True, slots=True)
class QueryResult:
    patient_id: str
    query: Query
    results: List[SemanticRow]


@dataclass(frozen=True, slots=True)
class BatchQueryResult:
    results: Tuple[QueryResult, ...]

    @property
    def matches(self) -> tuple[QueryResult, ...]:
        """Returns results for matched queries"""
        results = tuple(m for m in self.results if m.results)
        return results

    @property
    def missing(self) -> tuple[Query, ...]:
        """Returns constructed queries for non-matched results"""
        return tuple(m.query for m in self.results if not m.results)
