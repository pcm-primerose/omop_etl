import csv
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Sequence, Any
import polars as pl
from dataclasses import dataclass

from src.omop_etl.harmonization.datamodels import HarmonizedData, Patient


# todo
# [x] load semantic mapped data
# [x] just make it really dumb and working first
# [ ] return typed struct from json instead of using CsvReader (returns string for all fields)
# [ ] use composition in query results to store collection of rows matching a given query?
# [ ] make inverted index to get O(1) lookup
# [ ] make generic reflection from config to construct queries (config built in pipeline etc)
# [ ] make richer metadata in query result: struct contains leaf index if collection, field name, class name (or instance id?), etc
# [ ] also need to return null-results, probably in separate struct, since these will propegate to hybrid search pipeline
# [ ] make helper properties: missing, matched in results
# [ ] make config layer to match on all possible fields in CsvRow with typed values
# [ ] keep one central list of possible
# [ ] query id is not deterministic/idempotent (should have same id for class.field and query term?)
# [ ] write better docstrings

# [ ] separate models from types etc, think about how to make it nice for extension later
# [ ] return some mapped queries, do something with them in mail (e.g filter)

# todo later
# [ ] figure out how to abstract in query: just make it work first then add the wrapper/abstraction layer
# [ ] create polars backend for indexing and querying entire Athena database: OOM, lazy, vectorized
# [ ] either extend models or create new ones in separate retrieval modules (lexical, veector etc) passed to rerankers
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
    omop_concept: str
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
        with open(self.path, "r", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(SemanticRow.from_csv_row(row))
        return rows

    def as_indexed(self) -> dict[str, list[SemanticRow]]:
        return self._index(self.as_rows())

    @NotImplemented
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
    MEASUREMENTS = "Measurements"
    PROCEDURES = "Procedures"
    OBSERVATIONS = "Observations"
    DEVICE = "Device"


@dataclass(frozen=True, slots=True)
class QueryTarget:
    """OMOP target fields to query"""

    domains: frozenset[OmopDomain] | None = None
    vocabs: frozenset[str] | None = None
    concept_classes: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class FieldConfig:
    """What field used to constuct query"""

    # note: overkill for already mapped data, but reuse for lexical/vector search
    name: str
    field_path: tuple[str, ...]
    target: QueryTarget | None = None
    tags: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class Query:
    patient_id: str
    id: int
    query: str
    field_path: tuple[str, ...]
    raw_value: str
    leaf_index: None | int = None
    target: None | QueryTarget = None


def extract_queries(patient: Patient, configs: List[FieldConfig]) -> None | List[Query]:
    queries: List[Query] = []
    query_id = 0

    for cfg in configs:
        head, *tail = cfg.field_path
        attr = getattr(patient, head, None)
        if attr is None:
            continue

        # collections
        if isinstance(attr, (list, tuple, set, dict)):
            for idx, item in enumerate(attr):
                val = _get_nested_attr(item, tail) if tail else attr
                if isinstance(val, str) and val:
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
                    query_id += 1
            continue

        # singletons & scalars
        val = _get_nested_attr(attr, tail) if tail else attr
        if isinstance(attr, str) and val:
            if isinstance(val, str) and val:
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
                query_id += 1

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
        current = getattr(obj, name, None)
    return current


@dataclass(frozen=True, slots=True)
class QueryResult:
    patient_id: str
    query: Query
    results: List[SemanticRow]


@dataclass(frozen=True, slots=True)
class BatchQueryResult:
    results: Tuple[QueryResult]

    @property
    def matches(self) -> tuple[QueryResult, ...]:
        """Returns results for matched queries"""
        return tuple(m for m in self.results if m.results)

    @property
    def missing(self) -> tuple[Query, ...]:
        """Returns constructed queries for non-matched results"""
        return tuple(m.query for m in self.results if not m.results)


def lookup_exact(self, queries: list[Query]) -> BatchQueryResult:
    matches: tuple[QueryResult] = tuple()
    missing: tuple[Query] = tuple()

    for q in queries:
        candidates = self._idx.get(q.query)
        if not candidates:
            missing.append(q)
            continue

        for row in candidates:
            matches.append(
                QueryResult(
                    query_id=q.query_id,
                    result=row["omop_name"],
                    frequency=int(row["frequency"]),
                    omop_concept_id=row["omop_concept_id"],
                    omop_concept_code=row["omop_concept_code"],
                    omop_class=row["omop_class"],
                    omop_validity=row["omop_validity"],
                    omop_domain=row["omop_domain"],
                    omop_vocab=row["omop_vocab"],
                    omop_concept=row["omop_concept"],
                )
            )

    return BatchQueryResult(results=matches)


class Lookup:
    def __init__(self, semantic_data: List[dict] | pl.LazyFrame, harmonized_data: HarmonizedData):
        self.semantic_data = semantic_data
        self.harmonized_data = harmonized_data

    def exact_match(self):
        # 1. need to create a query for appropriate fields
        # 2. how to store fields that are in need of semantic mapping in a nice way?
        #   - either do a mapping and extract just what's in the mapping from Patient class,
        #   - ooor,
        # 3. run query on semantic mapped data
        # 4. return some struct that can be mapped back to instances and fields in HarmonizedData.patients
        #   - can just use patient id to get patient, for collection leaves can use the index field to map
        #   this is not needed obv for singletons and scalars

        # create queries from harmonized data
        queries: List[Query] = []
        for patient in self.harmonized_data.patients:
            # grab data we want to query with
            # and check that we don't have None
            # and normalize query
            # todo: make mapping of leaf classes to query from
            #   instead of hardcoding this

            # todo: but on the other hand, probs need to post-processes specific classes,
            #   e.g., want one representative tumor concept in the OMOP CDM, even though there are several matches..
            #   or can populate several entries per patient, perhaps just do that to begin with,
            #   exact matching can't score similarity anyways, so would need some onotlogy-aware ranking to get most specific tumor types

            # todo: maybe pass patient.tumor_type instead,
            #   a method then structures the queries,
            #   then repeat this pattern for all classes to query (configure this?)
            #   then run queries in one go, but then need to map back to the input class that provided the queries
            if patient.tumor_type.main_tumor_type is not None:
                queries.append(Query(patient_id=patient.patient_id, query=patient.tumor_type.main_tumor_type.lower().strip()))

            if patient.tumor_type.cohort_tumor_type is not None:
                queries.append(Query(patient_id=patient.patient_id, query=patient.tumor_type.cohort_tumor_type.lower().strip()))

            if patient.tumor_type.other_tumor_type is not None:
                queries.append(Query(patient_id=patient.patient_id, query=patient.tumor_type.other_tumor_type.lower().strip()))

            if patient.tumor_type.icd10_code is not None:
                queries.append(Query(patient_id=patient.patient_id, query=patient.tumor_type.icd10_code.lower().strip()))

            if patient.tumor_type.icd10_description is not None:
                queries.append(Query(patient_id=patient.patient_id, query=patient.tumor_type.icd10_description.lower().strip()))

        # query semantic data
        query_results: List[QueryResult] = []
        for query in queries:
            for row in self.semantic_data:
                if query.query == row["source_term"].lower():
                    query_results.append(
                        QueryResult(
                            patient_id=query.patient_id,
                            query=query.query,
                            result=row["omop_name"],
                            frequency=row["frequency"],
                            omop_concept_id=row["omop_concept_id"],
                            omop_concept_code=row["omop_concept_code"],
                            omop_class=row["omop_class"],
                            omop_validity=row["omop_validity"],
                            omop_domain=row["omop_domain"],
                            omop_vocab=row["omop_vocab"],
                            omop_concept=row["omop_concept"],
                        )
                    )

        # return some struct that maps to harmonized data
        print(f"query results: {query_results}")
        return query_results


# todo: implement in pipeline later
class ConstructQueries:
    def __init__(self, harmonized_data: HarmonizedData):
        pass

    pass


# todo: implement on trial level later
class SemanticPipeline:
    def __init__(self, semantic_data: Path, harmonized_data: HarmonizedData):
        pass

    def run(self):
        pass

    pass
