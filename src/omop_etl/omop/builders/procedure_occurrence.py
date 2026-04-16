from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ProcedureOcurrenceRow
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class ProcedureOccurrenceBuilder(OmopBuilder[ProcedureOcurrenceRow]):
    """Builds procedure_occurrence rows from previous treatments and medical histories."""

    table_name: ClassVar[str] = "procedure_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[ProcedureOcurrenceRow]:
        rows: list[ProcedureOcurrenceRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        procedure_type_concept_id = ecrf.concept_id if ecrf else 0

        for idx, prev in enumerate(patient.previous_treatments):
            row = self._build_previous_treatment_row(patient, person_id, prev, idx, procedure_type_concept_id)
            if row is not None:
                rows.append(row)

        # medical history terms that mapped to Procedure domain (past surgeries, etc.)
        for idx, mh in enumerate(patient.medical_histories):
            row = self._build_medical_history_row(patient, person_id, mh, idx, procedure_type_concept_id)
            if row is not None:
                rows.append(row)

        return rows

    def _build_previous_treatment_row(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatments,
        index: int,
        procedure_type_concept_id: int,
    ) -> ProcedureOcurrenceRow | None:
        if prev.start_date is None:
            log.warning("Skipping previous treatment %d for %s: missing start_date", index, patient.patient_id)
            return None

        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
            index,
            domains={OmopDomain.PROCEDURE},
        )
        if not mapped:
            return None
        procedure_concept_id = int(mapped[0].concept_id)

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.PREVIOUS_TREATMENTS,
            str(prev.treatment_sequence_number),
        )

        return ProcedureOcurrenceRow(
            procedure_occurrence_id=row_id,
            person_id=person_id,
            procedure_concept_id=procedure_concept_id,
            procedure_date=prev.start_date,
            procedure_end_date=prev.end_date,
            procedure_type_concept_id=procedure_type_concept_id,
            procedure_source_value=prev.treatment,
        )

    def _build_medical_history_row(
        self,
        patient: Patient,
        person_id: int,
        mh: MedicalHistory,
        index: int,
        procedure_type_concept_id: int,
    ) -> ProcedureOcurrenceRow | None:
        if mh.start_date is None:
            return None

        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            index,
            domains={OmopDomain.PROCEDURE},
        )
        if not mapped:
            return None
        procedure_concept_id = int(mapped[0].concept_id)

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.MEDICAL_HISTORIES,
            str(mh.sequence_id),
        )

        return ProcedureOcurrenceRow(
            procedure_occurrence_id=row_id,
            person_id=person_id,
            procedure_concept_id=procedure_concept_id,
            procedure_date=mh.start_date,
            procedure_end_date=mh.end_date,
            procedure_type_concept_id=procedure_type_concept_id,
            procedure_source_value=mh.term,
        )
