from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.omop.builders.base import OmopBuilder, BuildContext
from omop_etl.omop.models.rows import ConditionOccurrenceRow
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    """Builds condition_occurrence rows from tumor type, medical histories, and adverse events.

    CDM policy: only records whose source values map to concepts with a domain of
    "Condition" should go in this table (CDM 5.4 condition_occurrence ETL conventions).
    No rows emitted for unmapped source values.
    """

    table_name: ClassVar[str] = "condition_occurrence"

    def build(self, ctx: BuildContext) -> list[ConditionOccurrenceRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[ConditionOccurrenceRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        condition_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        if patient.tumor_type is not None:
            rows.extend(self._build_tumor_type_rows(patient, person_id, patient.tumor_type, condition_type_concept_id))

        for idx, mh in enumerate(patient.medical_histories):
            rows.extend(self._build_medical_history_rows(patient, person_id, mh, idx, condition_type_concept_id))

        for idx, ae in enumerate(patient.adverse_events):
            rows.extend(self._build_adverse_event_rows(patient, person_id, ae, idx, condition_type_concept_id))

        return rows

    def _build_tumor_type_rows(
        self,
        patient: Patient,
        person_id: int,
        tumor: TumorType | None,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        if tumor is None:
            return []

        if tumor.icd10_code:
            matches = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                None,
                domains={OmopDomain.CONDITION},
            )
            source_value = tumor.icd10_code
        elif tumor.main_tumor_type:
            matches = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                None,
                domains={OmopDomain.CONDITION},
            )
            source_value = tumor.main_tumor_type
        else:
            log.warning("Skipping tumor type for %s: no icd10_code or main_tumor_type", patient.patient_id)
            return []

        if not matches:
            return []

        date = tumor.date or patient.treatment_start_date
        if date is None:
            log.warning("Skipping tumor type for %s: no usable date", patient.patient_id)
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.TUMOR_TYPE,
                    str(concept.concept_id),
                ),
                person_id=person_id,
                condition_concept_id=int(concept.concept_id),
                condition_start_date=date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=source_value,
            )
            for concept in matches
        ]

    def _build_medical_history_rows(
        self,
        patient: Patient,
        person_id: int,
        mh: MedicalHistory,
        index: int,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        start_date = mh.start_date
        if start_date is None:
            log.warning("Skipping medical history %d for %s: missing start_date", index, patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            index,
            domains={OmopDomain.CONDITION},
        )
        if not matches:
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.MEDICAL_HISTORIES,
                    str(mh.sequence_id),
                    str(concept.concept_id),
                ),
                person_id=person_id,
                condition_concept_id=int(concept.concept_id),
                condition_start_date=start_date,
                condition_end_date=mh.end_date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=mh.term,
            )
            for concept in matches
        ]

    def _build_adverse_event_rows(
        self,
        patient: Patient,
        person_id: int,
        ae: AdverseEvent,
        index: int,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        start_date = ae.start_date
        term = ae.term
        if start_date is None:
            log.warning("Skipping adverse event %d for %s: missing start_date", index, patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
            index,
            domains={OmopDomain.CONDITION},
        )
        if not matches:
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    term,
                    start_date.strftime(format="%Y%m%d"),
                    str(concept.concept_id),
                ),
                person_id=person_id,
                condition_concept_id=int(concept.concept_id),
                condition_start_date=start_date,
                condition_end_date=ae.end_date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=term[:50] if term is not None else None,
            )
            for concept in matches
        ]
