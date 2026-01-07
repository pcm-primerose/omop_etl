from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from omop_etl.harmonization.models import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.core.id_generator import sha1_bigint

T = TypeVar("T")


class OmopBuilder(ABC, Generic[T]):
    """Abstract base class for OMOP table builders.

    Each builder is responsible for constructing rows for a single OMOP table,
    potentially from multiple patient fields.

    Class Variables:
        table_name: The OMOP table name (e.g., "person", "condition_occurrence")
        id_namespace: Namespace for ID generation. Defaults to table_name if not set.
    """

    table_name: ClassVar[str]
    id_namespace: ClassVar[str | None] = None  # defaults to table_name

    def __init__(self, concepts: ConceptLookupService):
        self._concepts = concepts

    @abstractmethod
    def build(self, patient: Patient, person_id: int) -> list[T]:
        """Build zero or more rows from a patient.

        Args:
            patient: The patient data to build rows from.
            person_id: The generated person_id for this patient.

        Returns:
            A list of rows (may be empty if patient data is insufficient).
        """
        ...

    def generate_row_id(self, *key_parts: str) -> int:
        """Generate a deterministic row ID from key parts.

        Uses SHA1 hashing with the builder's namespace to create a reproducible
        63-bit integer ID. The namespace defaults to the table_name.

        Args:
            *key_parts: String components that uniquely identify this row.
                        For simple tables: just patient_id
                        For collection tables: patient_id + index or sequence_id

        Returns:
            A deterministic 63-bit integer suitable for use as a primary key.

        Example:
            # Simple case (one row per patient)
            self.generate_row_id(patient.patient_id)

            # Collection case (multiple rows per patient)
            self.generate_row_id(patient.patient_id, str(index))
        """
        namespace = self.id_namespace or self.table_name
        composite_key = ":".join(key_parts)
        return sha1_bigint(namespace, composite_key)
