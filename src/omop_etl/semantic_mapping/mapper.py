import csv
import hashlib
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Sequence, Any, Set, Collection
from dataclasses import dataclass, field

from src.omop_etl.harmonization.datamodels import HarmonizedData, Patient

# todo last things
# [ ] add semantic mapping file to env (and package in resources?)
# [ ] create all default configs, if none provided, run with those

# todo later
# [ ] add semantic file to env, load from env if avail
# [ ] extract to separate files later; types, datamodels, etc, think about how to make it nice for extension later
# [ ] create polars backend for indexing and querying entire Athena database: OOM, lazy, vectorized
# [ ] either extend models or create new ones in separate retrieval modules (lexical, vector etc) passed to rerankers
#     - but need to finish basic impl & set up evals first
# [ ] BM25 & vector search needs to yield struct similar to SemanticRow


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
            term_id=row["term_id"],
            source_col=row["source_col"],
            source_term=row["source_term"],
            frequency=int(row["frequency"]),
            omop_concept_id=row["omop_concept_id"],
            omop_concept_code=row["omop_concept_code"],
            omop_name=row["omop_name"],
            omop_class=row["omop_class"],
            omop_concept=row["omop_concept"],
            omop_validity=row["omop_validity"],
            omop_domain=row["omop_domain"],
            omop_vocab=row["omop_vocab"],
        )


class LoadSemantics:
    def __init__(self, path: Path):
        self.path = path

    def as_rows(self) -> list[SemanticRow]:
        rows: list[SemanticRow] = []
        print(f"path in LoadSemantics: {self.path}")
        with open(self.path, "r", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(SemanticRow.from_csv_row(row))
        return rows

    def as_indexed(self) -> dict[str, list[SemanticRow]]:
        return self._index(self.as_rows())

    def as_lazyframe(self):
        raise NotImplementedError

    @staticmethod
    def _index(rows: list[SemanticRow]) -> dict[str, list[SemanticRow]]:
        idx: dict[str, list[SemanticRow]] = defaultdict(list)
        for row in rows:
            key = row.source_term.lower().strip()
            idx[key].append(row)
        return dict(idx)


class OmopDomain(str, Enum):
    CONDITION = "Condition"
    DRUG = "Drug"
    MEASUREMENTS = "Measurement"
    PROCEDURES = "Procedures"
    OBSERVATIONS = "Observations"
    DEVICE = "Device"


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


def extract_queries(patient: Patient, configs: List[FieldConfig]) -> List[Query]:
    queries: List[Query] = []

    for cfg in configs:
        head, *tail = cfg.field_path
        attr = getattr(patient, head, None)
        if attr is None:
            continue

        # collections
        if isinstance(attr, (list, tuple, set)):
            for idx, item in enumerate(attr):
                val = _get_nested_attr(item, tail) if tail else item
                if isinstance(val, str) and val:
                    query_id = _make_query_id(
                        patient_id=patient.patient_id,
                        field_path=cfg.field_path,
                        leaf_index=idx,
                        raw_value=val,
                    )
                    queries.append(
                        Query(
                            id=query_id,
                            patient_id=patient.patient_id,
                            query=val.lower().strip(),
                            field_path=cfg.field_path,
                            leaf_index=idx,
                            target=cfg.target,
                            raw_value=val,
                        )
                    )
            continue

        # singletons & scalars
        val = _get_nested_attr(attr, tail) if tail else attr
        if isinstance(val, str) and val:
            query_id = _make_query_id(
                patient_id=patient.patient_id,
                field_path=cfg.field_path,
                leaf_index=None,
                raw_value=val,
            )
            queries.append(
                Query(
                    id=query_id,
                    patient_id=patient.patient_id,
                    query=val.lower().strip(),
                    field_path=cfg.field_path,
                    leaf_index=None,
                    target=cfg.target,
                    raw_value=val,
                )
            )

    return queries


def _get_nested_attr(obj: object, path: Sequence[str]) -> Any:
    """
    Walk chain of attrs on single object,
    e.g.: _get_nested_attr(patient.tumor_type, ["main_tumor_type"])
    """
    current = obj
    for name in path:
        if current is None:
            return None
        current = getattr(current, name, None)
    return current


def _make_query_id(
    patient_id: str,
    field_path: tuple[str, ...],
    leaf_index: int | None,
    raw_value: str,
) -> str:
    key = "|".join(
        [
            patient_id,
            ".".join(field_path),
            "" if leaf_index is None else str(leaf_index),
            raw_value.strip().lower(),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class BatchQueryResult:
    results: Tuple[QueryResult, ...]

    @property
    def matches(self) -> tuple[QueryResult, ...]:
        """Returns results for matched queries"""
        return tuple(m for m in self.results if m.results)

    @property
    def missing(self) -> tuple[Query, ...]:
        """Returns constructed queries for non-matched results"""
        return tuple(m.query for m in self.results if not m.results)


# todo: when expanding to athena, make multi-index
# and construct with this before running query, for now doesn't matter
class DictSemanticIndex:
    def __init__(self, indexed_corpus: dict[str, List[SemanticRow]]):
        self.indexed_corpus = indexed_corpus

    def lookup_exact(self, queries: list[Query]) -> BatchQueryResult:
        results: list[QueryResult] = []

        for q in queries:
            candidates = self.indexed_corpus.get(q.query, [])
            candidates = self._filter_by_target(candidates, q.target)

            results.append(
                QueryResult(
                    patient_id=q.patient_id,
                    query=q,
                    results=list(candidates),
                )
            )

        return BatchQueryResult(results=tuple(results))

    @staticmethod
    def _filter_by_target(candidates: list[SemanticRow], target: QueryTarget | None) -> list[SemanticRow]:
        if target is None:
            return candidates

        doms = target.domains
        vocabs = target.vocabs
        classes = target.concept_classes
        validity = target.validity
        standard = target.standard_flags

        filtered: list[SemanticRow] = []
        for row in candidates:
            if doms is not None and OmopDomain(row.omop_domain) not in doms:
                continue
            if vocabs is not None and row.omop_vocab not in vocabs:
                continue
            if classes is not None and row.omop_class not in classes:
                continue
            if standard is not None and row.omop_concept not in standard:
                continue
            if validity is not None and row.omop_validity not in validity:
                continue
            filtered.append(row)

        return filtered


class SemanticLookupPipeline:
    def __init__(self, semantics_path: Path, field_configs: Sequence[FieldConfig]):
        self.semantics_path = semantics_path
        self.field_configs = field_configs
        loader = LoadSemantics(semantics_path)
        self._index = DictSemanticIndex(indexed_corpus=loader.as_indexed())

    def run_lookup(
        self,
        harmonized_data: HarmonizedData,
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
    ) -> BatchQueryResult:
        corpus = self._load_corpus()
        index = DictSemanticIndex(indexed_corpus=corpus)
        configs = self._build_configs(
            enable_names=enable_names,
            required_domains=required_domains,
            required_tags=required_tags,
        )

        all_queries: list[Query] = self._build_queries(harmonized_data=harmonized_data, configs=configs)

        results: BatchQueryResult = index.lookup_exact(queries=all_queries)
        return results

    def _load_corpus(self) -> dict[str, List[SemanticRow]]:
        loader = LoadSemantics(self.semantics_path)
        return loader.as_indexed()

    def _build_configs(
        self,
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
    ) -> list[FieldConfig]:
        configs = list(self.field_configs)

        if enable_names is not None:
            configs = [c for c in configs if c.name in enable_names]

        if required_domains is not None:
            configs = [c for c in configs if c.target and c.target.domains is not None and c.target.domains & required_domains]

        if required_tags is not None:
            configs = [c for c in configs if c.tags & required_tags]

        return configs

    @staticmethod
    def _build_queries(configs: list[FieldConfig], harmonized_data: HarmonizedData) -> list[Query]:
        all_queries: list[Query] = []
        for patient in harmonized_data.patients:
            all_queries.extend(extract_queries(patient=patient, configs=configs))
        return all_queries


class SemanticService:
    def __init__(self, semantic_path: Path, harmonized_data: HarmonizedData, output_path: Path | None = None):
        self.semantic_path = semantic_path
        self.harmonized_data = harmonized_data
        self.output_path = output_path

    # todo: create all configs
    def run(self):
        field_configs = [
            FieldConfig(
                name="tumor.main",
                field_path=("tumor_type", "main_tumor_type"),
                target=QueryTarget([OmopDomain.CONDITION, OmopDomain.OBSERVATIONS]),
                tags="tumor",
            ),
            FieldConfig(
                name="ae.term",
                field_path=("adverse_events", "term"),
                target=QueryTarget(domains=[OmopDomain.CONDITION]),
                tags="adverse_events",
            ),
            FieldConfig(name=""),
        ]

        pipeline = SemanticLookupPipeline(
            semantics_path=Path(self.semantic_path),
            field_configs=field_configs,
        )

        batch = pipeline.run_lookup(harmonized_data=self.harmonized_data)

        for q in batch.missing:
            print("NO MATCH:", q.patient_id, q.field_path, q.raw_value)

        for qr in batch.matches:
            print(f"MATCH: {qr.query.patient_id, qr.query.field_path, qr.query.raw_value} --- {qr.results}")

        return batch
