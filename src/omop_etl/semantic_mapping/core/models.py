from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
from typing import Tuple, List, Collection, Dict
import polars as pl

from omop_etl.infra.io.path_planner import WriterContext
from omop_etl.infra.utils.run_context import RunMetadata


@dataclass(frozen=True)
class OutputPaths:
    """Resolved output paths for semantic mapping results."""

    matches_path: Path | None
    missing_path: Path | None
    manifest_path: Path | None
    log_path: Path | None
    directory: Path | None
    format: str | None


def _norm(v: str | None) -> str:
    """Lowercase and strip a csv value, defaulting None to empty string."""
    return (v or "").lower().strip()


@dataclass(frozen=True, slots=True)
class SemanticRow:
    """
    One entry of the pre-computed semantic dictionary: a source term mapped to
    a single OMOP concept candidate (loaded from the mapping CSV).
    """

    term_id: str
    source_col: str
    source_term: str
    frequency: int
    omop_concept_id: str
    omop_concept_code: str
    omop_concept_name: str
    omop_concept_class: str
    omop_standard_concept: str
    omop_validity: str
    omop_domain: str
    omop_vocab: str

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "SemanticRow":
        return cls(
            term_id=_norm(row["term_id"]),
            source_col=_norm(row["source_col"]),
            source_term=_norm(row["source_term"]),
            frequency=int(row.get("frequency") or 0),
            omop_concept_id=_norm(row["omop_concept_id"]),
            omop_concept_code=_norm(row["omop_concept_code"]),
            omop_concept_name=_norm(row["omop_concept_name"]),
            omop_concept_class=_norm(row["omop_concept_class"]),
            omop_standard_concept=_norm(row["omop_standard_concept"]),
            omop_validity=_norm(row["omop_validity"]),
            omop_domain=_norm(row["omop_domain"]),
            omop_vocab=_norm(row["omop_vocab"]),
        )


class OmopDomain(str, Enum):
    """OMOP domains a query can target and a SemanticRow can belong to."""

    CONDITION = "condition"
    DRUG = "drug"
    MEASUREMENTS = "measurement"
    PROCEDURE = "procedure"
    OBSERVATIONS = "observations"
    DEVICE = "device"
    MEAS_VALUE = "meas value"
    TYPE_CONCEPT = "type concept"


@dataclass(frozen=True, slots=True)
class QueryTarget:
    """
    Filter narrowing which SemanticRows count as a match for a query (by
    domain, vocab, concept class, standard flag, validity). None = no filter on
    that facet.
    """

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
    """
    Configures one Patient field to extract queries from: the field_path to
    walk and the QueryTarget filter applied to its matches.
    """

    name: str
    field_path: tuple[str, ...]
    tags: Collection[str] = field(default_factory=frozenset)
    target: QueryTarget | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.tags, frozenset):
            object.__setattr__(self, "tags", frozenset(self.tags))


@dataclass(frozen=True, slots=True)
class Query:
    """
    One value extracted from a Patient instance to map: its location
    (patient_id, field_path, leaf_index) plus the raw_value to look up.

    leaf_index is the instance's position in its NK-sorted (deterministic)
    Patient collection, it correlates this query with its result within a run
    and never reaches a persisted key (builders key on the NK, not leaf_index).
    Query (and QueryResult) lifetime is constrained to the semantic-mapping module.
    """

    patient_id: str
    id: str
    query: str
    field_path: tuple[str, ...]
    raw_value: str
    leaf_index: None | int = None
    target: None | QueryTarget = None


@dataclass(frozen=True, slots=True)
class QueryResult:
    """
    A Query paired with the SemanticRows it matched (empty = unmapped), its
    location ties each match back to the exact Patient instance it came from.
    """

    patient_id: str
    query: Query
    results: List[SemanticRow]


@dataclass(frozen=True, slots=True)
class BatchQueryResult:
    """
    All QueryResults for a mapping run: partitions matched vs missing and
    serializes them (DataFrame, dict) with per-field_path coverage stats.
    """

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

    def to_matches_df(self) -> pl.DataFrame:
        """
        Convert matched results to a Polars DataFrame.

        Each row represents a single match (query + matched SemanticRow).
        Queries with multiple matches produce multiple rows.
        """
        rows = []
        for qr in self.matches:
            q = qr.query
            for sem_row in qr.results:
                rows.append(
                    {
                        "patient_id": q.patient_id,
                        "query_id": q.id,
                        "query": q.query,
                        "field_path": ".".join(q.field_path),
                        "raw_value": q.raw_value,
                        "leaf_index": q.leaf_index,
                        "term_id": sem_row.term_id,
                        "source_col": sem_row.source_col,
                        "source_term": sem_row.source_term,
                        "frequency": sem_row.frequency,
                        "omop_concept_id": sem_row.omop_concept_id,
                        "omop_concept_code": sem_row.omop_concept_code,
                        "omop_concept_name": sem_row.omop_concept_name,
                        "omop_concept_class": sem_row.omop_concept_class,
                        "omop_standard_concept": sem_row.omop_standard_concept,
                        "omop_validity": sem_row.omop_validity,
                        "omop_domain": sem_row.omop_domain,
                        "omop_vocab": sem_row.omop_vocab,
                    }
                )
        return pl.DataFrame(rows)

    def to_missing_df(self) -> pl.DataFrame:
        """Convert missing (unmatched) queries to a Polars DataFrame."""
        rows = []
        for q in self.missing:
            rows.append(
                {
                    "patient_id": q.patient_id,
                    "query_id": q.id,
                    "query": q.query,
                    "field_path": ".".join(q.field_path),
                    "raw_value": q.raw_value,
                    "leaf_index": q.leaf_index,
                }
            )
        return pl.DataFrame(rows)

    def to_matches_dict(self) -> List[Dict]:
        rows = []
        for qr in self.matches:
            q = qr.query
            for sem_row in qr.results:
                rows.append(
                    {
                        "patient_id": q.patient_id,
                        "query_id": q.id,
                        "query": q.query,
                        "field_path": ".".join(q.field_path),
                        "raw_value": q.raw_value,
                        "leaf_index": q.leaf_index,
                        "term_id": sem_row.term_id,
                        "source_col": sem_row.source_col,
                        "source_term": sem_row.source_term,
                        "frequency": sem_row.frequency,
                        "omop_concept_id": sem_row.omop_concept_id,
                        "omop_concept_code": sem_row.omop_concept_code,
                        "omop_concept_name": sem_row.omop_concept_name,
                        "omop_concept_class": sem_row.omop_concept_class,
                        "omop_standard_concept": sem_row.omop_standard_concept,
                        "omop_validity": sem_row.omop_validity,
                        "omop_domain": sem_row.omop_domain,
                        "omop_vocab": sem_row.omop_vocab,
                    }
                )
        return rows

    def to_missing_dict(self) -> List[Dict]:
        rows = []
        for q in self.missing:
            rows.append(
                {
                    "patient_id": q.patient_id,
                    "query_id": q.id,
                    "query": q.query,
                    "field_path": ".".join(q.field_path),
                    "raw_value": q.raw_value,
                    "leaf_index": q.leaf_index,
                }
            )
        return rows

    def coverage_by_field_path(self) -> Dict[str, Dict]:
        """
        Compute mapping coverage statistics per field_path.

        Returns a dict keyed by field_path (dot-joined) with stats:
          - matched: number of queries that found matches
          - missing: number of queries that didn't match
          - total: total queries for this field_path
          - coverage_fraction: fraction of queries that matched (0.0-1.0)
        """
        from collections import defaultdict

        counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"matched": 0, "missing": 0})

        for qr in self.matches:
            fp = ".".join(qr.query.field_path)
            counts[fp]["matched"] += 1

        for q in self.missing:
            fp = ".".join(q.field_path)
            counts[fp]["missing"] += 1

        result: Dict[str, Dict] = {}
        for fp, c in counts.items():
            total = c["matched"] + c["missing"]
            frac = round(c["matched"] / total, 5) if total > 0 else 0.0
            result[fp] = {
                "matched": c["matched"],
                "missing": c["missing"],
                "total": total,
                "coverage_fraction": frac,
            }

        return result


@dataclass(frozen=True)
class SemanticMappingResult:
    """Result of semantic mapping pipeline"""

    batch_result: BatchQueryResult
    meta: RunMetadata
    output_paths: Dict[str, WriterContext] | None = None

    @property
    def matches_count(self) -> int:
        return len(self.batch_result.matches)

    @property
    def missing_count(self) -> int:
        return len(self.batch_result.missing)

    @property
    def total_queries(self) -> int:
        return len(self.batch_result.results)
