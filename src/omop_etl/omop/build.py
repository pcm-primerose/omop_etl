from omop_etl.harmonization.models import HarmonizedData
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.cdm_source_builder import CdmSourceBuilder
from omop_etl.omop.builders.observation_period_builder import ObservationPeriodBuilder
from omop_etl.omop.id_generator import sha1_bigint
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.builders.person_builder import PersonRowBuilder
from omop_etl.omop.models.rows import PersonRow, ObservationPeriodRow


class BuildOmopRows:
    def __init__(
        self,
        harmonized_data: HarmonizedData,
        concepts: ConceptLookupService,
    ):
        self._hd = harmonized_data
        self._concepts = concepts

        self._person_builder = PersonRowBuilder(self._concepts)
        self._observation_period_builder = ObservationPeriodBuilder(self._concepts)
        self._cdm_source_builder = CdmSourceBuilder(self._concepts)

    def build_all_rows(self) -> OmopTables:
        pid_map = self._generate_person_ids()
        person_rows: list[PersonRow] = []
        observation_period_rows: list[ObservationPeriodRow] = []
        cdm_source = self._cdm_source_builder.build_cdm_source()

        for p in self._hd.patients:
            pid = pid_map[p.patient_id]
            # fixme: make this better later, each builder should be responsible for
            #   returning valid data
            person = self._person_builder.build(patient=p, person_id=pid)
            if person is None:
                continue
            person_rows.append(person)

            op = self._observation_period_builder.build(patient=p, person_id=pid)
            if op is None:
                continue
            observation_period_rows.append(op)

        return OmopTables(
            person=person_rows,
            observation_period=observation_period_rows,
            cdm_source=cdm_source,
        )

    def _generate_person_ids(self) -> dict[str, int]:
        return {p.patient_id: sha1_bigint("person", p.patient_id) for p in self._hd.patients}
