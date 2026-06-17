from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.omop.models.rows import ProcedureOccurrenceRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain
from omop_etl.omop.builders.base import (
    BuildContext,
    BuildResult,
    OmopBuilder,
)

log = getLogger(__name__)


class ProcedureOccurrenceBuilder(OmopBuilder[ProcedureOccurrenceRow]):
    """Builds procedure_occurrence rows from previous treatments and medical histories.

    CDM policy: only records whose source values map to standard concepts with a domain
    of "Procedure" should go in this table (CDM 5.4 procedure_occurrence ETL conventions).
    No rows emitted for unmapped source values.
    """

    table_name: ClassVar[str] = OmopTables.PROCEDURE_OCCURRENCE

    def build(self, ctx: BuildContext) -> BuildResult[ProcedureOccurrenceRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[ProcedureOccurrenceRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"type concept"})
        procedure_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        for idx, prev in enumerate(patient.previous_treatments):
            if prev.start_date is None:
                log.warning("Skipping previous treatment %d for %s: missing start_date", idx, patient.patient_id)
                continue
            if prev.treatment:
                rows.extend(self._build_previous_treatment_main_rows(patient, person_id, prev, procedure_type_concept_id))
            if prev.additional_treatment:
                rows.extend(self._build_previous_treatment_additional_rows(patient, person_id, prev, procedure_type_concept_id))

        for mh in patient.medical_histories:
            rows.extend(self._build_medical_history_rows(patient, person_id, mh, procedure_type_concept_id))

        return BuildResult(rows=tuple(rows))

    def _build_previous_treatment_main_rows(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatment,
        procedure_type_concept_id: int,
    ) -> list[ProcedureOccurrenceRow]:
        start_date = prev.start_date
        if start_date is None:
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.TREATMENT),
            prev.treatment,
            domains={OmopDomain.PROCEDURE},
        )
        if not matches:
            return []

        return [
            ProcedureOccurrenceRow(
                procedure_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.PREVIOUS_TREATMENTS,
                    PreviousTreatment.Fields.TREATMENT,
                    *prev.natural_key(),
                    concept.concept_id,
                ),
                person_id=person_id,
                procedure_concept_id=concept.concept_id,
                procedure_date=start_date,
                procedure_end_date=prev.end_date,
                procedure_type_concept_id=procedure_type_concept_id,
                procedure_source_value=prev.treatment,
            )
            for concept in matches
        ]

    def _build_previous_treatment_additional_rows(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatment,
        procedure_type_concept_id: int,
    ) -> list[ProcedureOccurrenceRow]:
        start_date = prev.start_date
        if start_date is None:
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.ADDITIONAL_TREATMENT),
            prev.additional_treatment,
            domains={OmopDomain.PROCEDURE},
        )
        if not matches:
            return []

        return [
            ProcedureOccurrenceRow(
                procedure_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.PREVIOUS_TREATMENTS,
                    *prev.natural_key(),
                    PreviousTreatment.Fields.ADDITIONAL_TREATMENT,
                    concept.concept_id,
                ),
                person_id=person_id,
                procedure_concept_id=concept.concept_id,
                procedure_date=start_date,
                procedure_end_date=prev.end_date,
                procedure_type_concept_id=procedure_type_concept_id,
                procedure_source_value=prev.additional_treatment,
            )
            for concept in matches
        ]

    def _build_medical_history_rows(
        self,
        patient: Patient,
        person_id: int,
        mh: MedicalHistory,
        procedure_type_concept_id: int,
    ) -> list[ProcedureOccurrenceRow]:
        start_date = mh.start_date
        if start_date is None:
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            mh.term,
            domains={OmopDomain.PROCEDURE},
        )
        if not matches:
            return []

        return [
            ProcedureOccurrenceRow(
                procedure_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.MEDICAL_HISTORIES,
                    *mh.natural_key(),
                    concept.concept_id,
                ),
                person_id=person_id,
                procedure_concept_id=concept.concept_id,
                procedure_date=start_date,
                procedure_end_date=mh.end_date,
                procedure_type_concept_id=procedure_type_concept_id,
                procedure_source_value=mh.term,
            )
            for concept in matches
        ]
