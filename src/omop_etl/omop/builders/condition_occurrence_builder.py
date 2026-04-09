from typing import ClassVar
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ConditionOccurrenceRow


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    table_name: ClassVar[str] = "condition_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[ConditionOccurrenceRow]:
        outpatient = self._concepts.lookup_structural("outpatient_visit")
        ecrf = self._concepts.lookup_structural("ecrf")
        print(f"ecrf; {ecrf}")
        condition_ocurrence_id = self.generate_row_id(patient.patient_id)
        tumor_type_concepts = self._concepts.lookup_semantic(patient.patient_id, ("tumor_type", "ICD10"), None)
        tumor_type_concept_id = tumor_type_concepts[0].concept_id if tumor_type_concepts else 0

        row = ConditionOccurrenceRow(
            condition_occurrence_id=condition_ocurrence_id,
            person_id=person_id,
            condition_concept_id=tumor_type_concept_id,
            condition_start_date=dt.date,
            condition_type_concept_id=32809,  # Case Report Form
            condition_start_datetime=None,
            condition_end_date=None,
            condition_end_datetime=None,
            condition_status_concept_id=None,
            stop_reason=None,
            provider_id=None,
            visit_occurrence_id=None,
            visit_detail_id=None,
            condition_source_value=None,
            condition_source_concept_id=None,
            condition_status_source_value=None,
        )
        return [row]
