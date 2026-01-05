from omop_etl.harmonization.datamodels import Patient
from omop_etl.omop.id_generator import sha1_bigint
from omop_etl.omop.models.rows import ObservationPeriodRow
from omop_etl.concept_mapping.api import ConceptLookupService


class ObservationPeriodBuilder:
    def __init__(self, concepts: ConceptLookupService):
        self._concepts = concepts

    def build(self, patient: Patient, person_id: int) -> ObservationPeriodRow | None:
        treatment_start = patient.treatment_start_date
        if treatment_start is None:
            return None

        treatment_end = patient.treatment_end_date
        observation_type = self._concepts.row_concepts_for_value_set("ecrf")
        period_id = sha1_bigint("observation_period", patient.patient_id)

        return ObservationPeriodRow(
            observation_period_id=period_id,
            person_id=person_id,
            observation_period_start_date=treatment_start,
            observation_period_end_date=treatment_end,
            period_type_concept_id=observation_type.concept_id,
        )
