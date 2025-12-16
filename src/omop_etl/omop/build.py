from pathlib import Path
from itertools import count

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.mapping.semantic_loader import SemanticResultIndex
from omop_etl.mapping.static_loader import StaticMapLoader
from omop_etl.mapping.concept_service import ConceptMappingService
from omop_etl.mapping.structural_loader import StructuralMapLoader
from omop_etl.omop.builders.observation_period_builder import ObservationPeriodBuilder
from omop_etl.omop.models.tables import OmopTables

from omop_etl.omop.builders.person_builder import PersonRowBuilder
from omop_etl.omop.models.rows import PersonRow, ObservationPeriodRow
from omop_etl.semantic_mapping.models import BatchQueryResult


class BuildOmopRows:
    def __init__(
        self,
        harmonized_data: HarmonizedData,
        static_mapping_path: Path,
        semantic_batch: BatchQueryResult | None = None,
        structural_mapping_path: Path | None = None,
    ):
        self._hd = harmonized_data

        # static index
        static_index = StaticMapLoader(static_mapping_path).as_index()

        # structural index
        if structural_mapping_path is not None:
            structural_index = StructuralMapLoader(structural_mapping_path).as_index()
        else:
            structural_index = {}

        # semantic results index
        semantic_index = SemanticResultIndex.from_batch(semantic_batch) if semantic_batch is not None else None

        self._concepts = ConceptMappingService(
            static_index=static_index,
            structural_index=structural_index,
            semantic_index=semantic_index,
        )

        # todo: make this functional

        self._person_builder = PersonRowBuilder(self._concepts)
        self._observation_period_builder = ObservationPeriodBuilder(self._concepts)
        # self._measurement_builder = ...

    def build_all_rows(self) -> OmopTables:
        pid_map = self._generate_person_ids()
        person_rows: list[PersonRow] = []
        observation_period_rows: list[ObservationPeriodRow] = []

        for p in self._hd.patients:
            pid = pid_map[p.patient_id]
            person_rows.append(self._person_builder.build(patient=p, person_id=pid))
            observation_period_rows.append(self._observation_period_builder.build(patient=p, person_id=pid))

        return OmopTables(person=person_rows, observation_period=observation_period_rows)

    def _generate_person_ids(self) -> dict[str, int]:
        # todo: make sha1-hex instead of sorting
        mapping: dict[str, int] = {}
        gen = count(start=1)
        for p in sorted(self._hd.patients, key=lambda x: x.patient_id):
            mapping[p.patient_id] = next(gen)
        return mapping
