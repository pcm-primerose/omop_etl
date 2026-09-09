from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from typing import Tuple, List, Dict
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
    return (v or "").casefold().strip()


@dataclass(frozen=True, slots=True)
class SemanticRow:
    """
    One entry of the pre-computed semantic dictionary: a source term mapped to
    a single OMOP concept candidate (loaded from the mapping CSV).
    """

    source_value: str
    concept_id: str
    concept_code: str
    concept_name: str
    concept_class_id: str
    standard_concept: str
    validity: str
    domain_id: str
    vocabulary_id: str

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "SemanticRow":
        return cls(
            source_value=_norm(row["source_value"]),
            concept_id=_norm(row["concept_id"]),
            concept_code=_norm(row["concept_code"]),
            concept_name=_norm(row["concept_name"]),
            concept_class_id=_norm(row["concept_class_id"]),
            standard_concept=_norm(row["standard_concept"]),
            validity=_norm(row["validity"]),
            domain_id=_norm(row["domain_id"]),
            vocabulary_id=_norm(row["vocabulary_id"]),
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
    EPISODE = "episode"
    REGIMEN = "regimen"
    GEOGRAPHY = "geography"


@dataclass(frozen=True, slots=True)
class FieldConfig:
    """
    Declares one Patient field to extract for semantic mapping, i.e. the
    allow-list of *semantic fields*. It carries only the field_path to walk:
    no domains (the consumer narrows by domain at lookup time, not here) and no
    match-time filter. `name` is a label used for run selection
    (`enable_names`) and coverage reporting.
    """

    name: str
    field_path: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Query:
    """
    One value extracted from a Patient instance to map: its location
    (patient_id, field_path) plus the raw_value to look up. Concepts are
    resolved by the normalized raw_value. Query (and QueryResult) lifetime is
    constrained to the semantic-mapping module.
    """

    patient_id: str
    id: str
    query: str
    field_path: tuple[str, ...]
    raw_value: str


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
                        "source_value": sem_row.source_value,
                        "concept_id": sem_row.concept_id,
                        "concept_code": sem_row.concept_code,
                        "concept_name": sem_row.concept_name,
                        "concept_class_id": sem_row.concept_class_id,
                        "standard_concept": sem_row.standard_concept,
                        "validity": sem_row.validity,
                        "domain_id": sem_row.domain_id,
                        "vocabulary_id": sem_row.vocabulary_id,
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
                        "source_value": sem_row.source_value,
                        "concept_id": sem_row.concept_id,
                        "concept_code": sem_row.concept_code,
                        "concept_name": sem_row.concept_name,
                        "concept_class_id": sem_row.concept_class_id,
                        "standard_concept": sem_row.standard_concept,
                        "validity": sem_row.validity,
                        "domain_id": sem_row.domain_id,
                        "vocabulary_id": sem_row.vocabulary_id,
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
