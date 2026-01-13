from typing import ClassVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ConditionOccurrenceRow


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    table_name: ClassVar[str] = "condition_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[ConditionOccurrenceRow]:
        outpatient = self._concepts.lookup_structural("outpatient_visit")
        ecrf = self._concepts.lookup_structural("ecrf")

        rows: list[ConditionOccurrenceRow] = []
        pass

    pass
