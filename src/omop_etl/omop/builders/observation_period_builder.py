from typing import ClassVar

from omop_etl.harmonization.models import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import ObservationPeriodRow


class ObservationPeriodBuilder(OmopBuilder[ObservationPeriodRow]):
    """Builds ObservationPeriod table rows from patient treatment dates."""

    table_name: ClassVar[str] = "observation_period"

    def build(self, patient: Patient, person_id: int) -> list[ObservationPeriodRow]:
        treatment_start = patient.treatment_start_date
        if treatment_start is None:
            return []

        treatment_end = patient.treatment_end_date
        observation_type = self._concepts.lookup_structural("ecrf")
        period_id = self.generate_row_id(patient.patient_id)

        row = ObservationPeriodRow(
            observation_period_id=period_id,
            person_id=person_id,
            observation_period_start_date=treatment_start,
            observation_period_end_date=treatment_end,
            period_type_concept_id=observation_type.concept_id,
        )
        return [row]
