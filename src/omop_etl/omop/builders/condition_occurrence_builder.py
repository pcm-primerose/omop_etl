from typing import ClassVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ConditionOccurrenceRow


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    table_name: ClassVar[str] = "condition_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[ConditionOccurrenceRow]:
        outpatient = self._concepts.lookup_structural("outpatient_visit")
        ecrf = self._concepts.lookup_structural("ecrf")


        """
        rows: list[ConditionOccurrenceRow] = []
        pass

    pass
        """

        
        row = ConditionOccurrenceRow(
            condition_occurrence_id = int
            person_id = int
            condition_concept_id = int
            condition_start_date = dt.date
            condition_type_concept_id = int
            condition_start_datetime = dt.datetime | None = None
            condition_end_date = dt.date | None = None
            condition_end_datetime = dt.datetime | None = None
            condition_status_concept_id = int | None = None
            stop_reason = str | None = pd_field(None, max_length=20)
            provider_id = int | None = None
            visit_occurrence_id = int | None = None
            visit_detail_id = int | None = None
            condition_source_value = str | None = pd_field(None, max_length=50)
            condition_source_concept_id = int | None = None
            condition_status_source_value = str | None = pd_field(None, max_length=50)	
		)
		return [row]