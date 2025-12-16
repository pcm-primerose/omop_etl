from typing import Mapping, Sequence
from .models import MappedConcept, StaticConcept, StructuralConcept
from .semantic_loader import SemanticResultIndex


# todo: add struct and consume query results properly
#   so currently I need to the


class ConceptMappingService:
    """
    Facade over:
    - static mapping index
    - structural mapping index
    - semantic mapping results index
    """

    def __init__(
        self,
        static_index: Mapping[tuple[str, str], StaticConcept],
        structural_index: Mapping[str, Sequence[StructuralConcept]] | None = None,
        semantic_index: SemanticResultIndex | None = None,
    ):
        self._static_index = static_index
        self._structural_index = structural_index or {}
        self._semantic_index = semantic_index

    # static
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

    # structural
    def row_concepts_for_value_set(self, value_set: str) -> MappedConcept | None:
        rows: StructuralConcept = self._structural_index.get(value_set, ())
        if rows is None:
            return None

        return MappedConcept(
            concept_id=rows.concept_id,
            concept_code=rows.concept_code,
            concept_name=rows.concept_name,
            domain_id=rows.domain_id,
            vocabulary_id=rows.vocabulary_id,
            standard_flag=rows.valid_flag,
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

        qr = self._semantic_index.lookup(
            patient_id=patient_id,
            field_path=field_path,
            leaf_index=leaf_index,
        )
        if qr is None or not qr.results:
            return ()

        mapped: list[MappedConcept] = []
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
