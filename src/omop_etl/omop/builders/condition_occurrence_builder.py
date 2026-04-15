from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ConditionOccurrenceRow

log = getLogger(__name__)


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    """Builds condition_occurrence rows from tumor type, medical histories, and adverse events."""

    table_name: ClassVar[str] = "condition_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[ConditionOccurrenceRow]:
        rows: list[ConditionOccurrenceRow] = []
        ecrf = self.concepts.lookup_structural("ecrf")
        condition_type_concept_id = ecrf.concept_id if ecrf else 0

        # tumor type singleton: one condition row for the cancer diagnosis
        if patient.tumor_type is not None:
            row = self._build_tumor_type_row(patient, person_id, patient.tumor_type, condition_type_concept_id)
            if row is not None:
                rows.append(row)

        for idx, mh in enumerate(patient.medical_histories):
            row = self._build_medical_history_row(patient, person_id, mh, idx, condition_type_concept_id)
            if row is not None:
                rows.append(row)

        for idx, ae in enumerate(patient.adverse_events):
            row = self._build_adverse_event_row(patient, person_id, ae, idx, condition_type_concept_id)
            if row is not None:
                rows.append(row)

        return rows

    def _build_tumor_type_row(
        self,
        patient: Patient,
        person_id: int,
        tumor: TumorType,
        condition_type_concept_id: int,
    ) -> ConditionOccurrenceRow | None:
        # prefer icd10_code (most clinically specific), fall back to main_tumor_type
        if tumor.icd10_code:
            mapped = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                None,
            )
            source_value = tumor.icd10_code
        elif tumor.main_tumor_type:
            mapped = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                None,
            )
            source_value = tumor.main_tumor_type
        else:
            log.warning("Skipping tumor type for %s: no icd10_code or main_tumor_type", patient.patient_id)
            return None

        condition_concept_id = int(mapped[0].concept_id) if mapped else 0

        date = tumor.date or patient.treatment_start_date
        if date is None:
            log.warning("Skipping tumor type for %s: no usable date", patient.patient_id)
            return None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.TUMOR_TYPE,
        )

        return ConditionOccurrenceRow(
            condition_occurrence_id=row_id,
            person_id=person_id,
            condition_concept_id=condition_concept_id,
            condition_start_date=date,
            condition_type_concept_id=condition_type_concept_id,
            condition_source_value=source_value,
        )

    def _build_medical_history_row(
        self,
        patient: Patient,
        person_id: int,
        mh: MedicalHistory,
        index: int,
        condition_type_concept_id: int,
    ) -> ConditionOccurrenceRow | None:
        if mh.start_date is None:
            log.warning("Skipping medical history %d for %s: missing start_date", index, patient.patient_id)
            return None

        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            index,
        )
        condition_concept_id = int(mapped[0].concept_id) if mapped else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.MEDICAL_HISTORIES,
            str(mh.sequence_id),
        )

        return ConditionOccurrenceRow(
            condition_occurrence_id=row_id,
            person_id=person_id,
            condition_concept_id=condition_concept_id,
            condition_start_date=mh.start_date,
            condition_end_date=mh.end_date,
            condition_type_concept_id=condition_type_concept_id,
            condition_source_value=mh.term,
        )

    def _build_adverse_event_row(
        self,
        patient: Patient,
        person_id: int,
        ae: AdverseEvent,
        index: int,
        condition_type_concept_id: int,
    ) -> ConditionOccurrenceRow | None:
        if ae.start_date is None:
            log.warning("Skipping adverse event %d for %s: missing start_date", index, patient.patient_id)
            return None

        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
            index,
        )
        condition_concept_id = int(mapped[0].concept_id) if mapped else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.ADVERSE_EVENTS,
            str(ae.term),
            str(ae.start_date),
        )

        return ConditionOccurrenceRow(
            condition_occurrence_id=row_id,
            person_id=person_id,
            condition_concept_id=condition_concept_id,
            condition_start_date=ae.start_date,
            condition_end_date=ae.end_date,
            condition_type_concept_id=condition_type_concept_id,
            condition_source_value=ae.term[0:50],
        )
