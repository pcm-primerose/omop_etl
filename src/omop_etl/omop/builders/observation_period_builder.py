from omop_etl.harmonization.datamodels import Patient
from omop_etl.omop.models.rows import ObservationPeriodRow
from omop_etl.mapping.concept_service import ConceptMappingService


class ObservationPeriodBuilder:
    def __init__(self, concept_service: ConceptMappingService):
        self._concepts = concept_service

    def build(self, patient: Patient, person_id: int) -> ObservationPeriodRow:
        treatment_start = patient.treatment_start_date
        # if treatment_start is None:
        # log etc later

        treatment_end = patient.treatment_end_date
        observation_type = self._concepts.row_concepts_for_value_set("ecrf")
        # todo: donn't like candidates and tuple unpacking in impl
        #   but have to do this since ill query by shared indices in the structural mapping file
        #   and I can't name each one, so "need" to unpack tuples or look over instances
        #   which is ugly but ok for now
        print(f"observation type: {observation_type[0]}")

        return ObservationPeriodRow(
            observation_period_id=person_id,  # fixme
            person_id=person_id,
            observation_start_date=treatment_start,
            observation_end_date=treatment_end,
            period_type_concept_id=observation_type[0].concept_id,
        )

    def _generate_observation_period_id(
        self,
    ):
        # fixme: better to handle this in row creator instead
        pass
