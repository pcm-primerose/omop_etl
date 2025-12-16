from typing import Mapping, Sequence
from .models import MappedConcept, StaticConcept, StructuralConcept
from omop_etl.semantic_mapping.semantic_index import SemanticIndex

# todo: add struct and consume query results properly


class ConceptMappingService:
    """
    Facade over:
    - static mapping index
    - structural mapping index
    - semantic mapping index
    """

    def __init__(
        self,
        static_index: Mapping[tuple[str, str], StaticConcept],
        structural_index: Mapping[str, Sequence[StructuralConcept]] | None = None,
        semantic_index: SemanticIndex | None = None,
    ):
        self._static_index = static_index
        self._structural_index = structural_index or {}
        self._semantic_index = semantic_index

    def lookup_static(self, value_set: str, local_value: str) -> MappedConcept | None:
        c = self._static_index.get((value_set, str(local_value)))
        if c is None:
            return None
        return MappedConcept(
            concept_id=c.concept_id,
            concept_code=c.concept_code,
            concept_name=c.concept_name,
            domain_id=c.domain_id,
            vocabulary_id=c.vocabulary_id,
            standard_flag=c.valid_flag,
        )

    # struct
    def row_concepts_for_value_set(self, value_set: str) -> tuple[MappedConcept, ...]:
        rows = self._structural_index.get(value_set, ())
        return tuple(
            MappedConcept(
                concept_id=r.concept_id,
                concept_code=r.concept_code,
                concept_name=r.concept_name,
                domain_id=r.domain_id,
                vocabulary_id=r.vocabulary_id,
                standard_flag=r.valid_flag,
            )
            for r in rows
        )

    # semantic
    def lookup_semantic_for_location(
        self,
        patient_id: str,
        field_path: tuple[str, ...],
        leaf_index: int | None,
    ) -> tuple[MappedConcept, ...]:
        if self._semantic_index is None:
            return ()
        qresults = self._semantic_index.lookup_exact(patient_id, field_path, leaf_index)
        mapped: list[MappedConcept] = []
        for qr in qresults:
            for row in qr.results:
                mapped.append(
                    MappedConcept(
                        concept_id=int(row.omop_concept_id),
                        concept_code=row.omop_concept_code,
                        concept_name=row.omop_name,
                        domain_id=row.omop_domain,
                        vocabulary_id=row.omop_vocab,
                        standard_flag=row.omop_validity,
                    )
                )
        return tuple(mapped)
