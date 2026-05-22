from typing import ClassVar

from omop_etl.omop.builders.base import OmopBuilder, BuildContext
from omop_etl.omop.models.rows import ObservationPeriodRow


class ObservationPeriodBuilder(OmopBuilder[ObservationPeriodRow]):
    """Builds ObservationPeriod table rows from patient treatment dates."""

    table_name: ClassVar[str] = "observation_period"

    def build(self, ctx: BuildContext) -> list[ObservationPeriodRow]:
        patient = ctx.patient
        treatment_start_date = patient.treatment_start_date
        eot = patient.end_of_treatment
        eot_date = eot.date if eot else None
        treatment_end_date = eot_date or patient.treatment_start_last_cycle
        if treatment_start_date is None:
            return []
        if treatment_end_date is None:
            return []

        observation_type = self.concepts.lookup_structural("observation_period")
        period_type_concept_id = int(observation_type.concept_id) if observation_type else 0
        period_id = self.generate_row_id(patient.patient_id)

        row = ObservationPeriodRow(
            observation_period_id=period_id,
            person_id=ctx.person_id,
            observation_period_start_date=treatment_start_date,
            observation_period_end_date=treatment_end_date,
            period_type_concept_id=period_type_concept_id,
        )

        return [row]
