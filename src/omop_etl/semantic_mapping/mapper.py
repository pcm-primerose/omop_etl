import csv
from pathlib import Path
from typing import List
import polars as pl
from dataclasses import dataclass

from src.omop_etl.harmonization.datamodels import HarmonizedData

# semantics_file = Path(__file__).parents[3] / ".data" / "semantic_mapping" / "mapped" / "braf_non-v600_mapped.csv"

# todo
# [x] load semantic mapped data
# [ ] just make it really dumb and working first

# todo notes
# [ ] make generalized query layer; works for arbitrary instances with arbitrary fields of datamodel
#     and returns general result structs, then map to datamodel, structural mapper recieves this.
#     retrieval pipeline runs if needed for queries not found in direct lookup,
#     bm25 / vector search etc can be added later, returning same final result struct
#     since input to all retrieval will be the same Patient instances...
#
# [ ] log missing results from lookup, use these to run retrieval pipeline


class LoadSemanticFile:
    def __init__(self, path: Path):
        self.path = path

    def as_dict(self) -> List[dict]:
        data = []
        with open(self.path, "r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
        return data

    def as_lazyframe(self):
        return pl.scan_csv(self.path, infer_schema=0)


@dataclass(frozen=True, slots=True)
class Query:
    patient_id: str
    query: str
    leaf_index: int | None = None


@dataclass(frozen=True, slots=True)
class QueryResult:
    patient_id: str
    query: str
    result: str
    frequency: int
    omop_concept_id: str
    omop_concept_code: str
    omop_class: str
    omop_validity: str
    omop_domain: str
    omop_vocab: str
    omop_concept: str  # todo: rename
    is_standard: bool | None = None  # and use this later


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
            # 1.1 grab data we want to query with
            # and check that we don't have None
            # and normalize query
            # todo: make mapping of leaf classes to query from
            #   instead of hardcoding this

            # todo: but on the other hand, probs need to post-processes specific classes,
            #   e.g., want one representative tumor concept in the OMOP CDM, even though there are several matches..
            #   or can populate several entries per patient, perhaps just do that to begin with,
            #   exact matching can't score similarity anyways, so would need some onotlogy-aware ranking to get most specific tumor types
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


# implement later
class SemanticPipeline:
    def __init__(self, semantic_data: Path, harmonized_data: HarmonizedData):
        pass

    def run(self):
        pass

    pass
