import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Generic, TypeVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.core.id_generator import sha1_bigint

T = TypeVar("T")


@dataclass
class BuildContext:
    """Per-patient runtime state passed to builders.

    Contains only per-patient data and cross-builder state (e.g. visit occurrence foreign keys).
    """

    patient: Patient
    person_id: int
    visit_id_by_date: dict[dt.date, int] = field(default_factory=dict)


class OmopBuilder(ABC, Generic[T]):
    """
    Abstract base class for OMOP table builders.

    Class vars:
        table_name: The OMOP table name (matching CDM spec)
        id_namespace: Namespace for ID generation
    """

    table_name: ClassVar[str]
    id_namespace: ClassVar[str | None] = None

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    @abstractmethod
    def build(self, ctx: BuildContext) -> list[T]:
        """
        Build rows from a patient.

        Args:
            ctx: Build context containing patient data, person_id, concept service,
                 and cross-builder state (e.g. visit_id_by_date).

        Returns:
            A list of rows (may be empty if patient data is insufficient).
        """
        ...

    def generate_row_id(self, *key_parts: str | None) -> int:
        """
        Deterministic row ID from key parts, using SHA1 hashing with builder's
        namespace to create a reproducible 63-bit integer ID.
        Namespace defaults to the table_name. None parts are filtered out.

        Example:
            self.generate_row_id(patient.patient_id)
            self.generate_row_id(patient.patient_id, str(cycle_number))
        """
        namespace = self.id_namespace or self.table_name
        filtered = [str(p) for p in key_parts if p is not None]
        composite_key = ":".join(filtered)
        return sha1_bigint(namespace, composite_key)
