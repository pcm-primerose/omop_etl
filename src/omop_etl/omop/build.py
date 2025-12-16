from pathlib import Path
from itertools import count
from typing import Mapping

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.mapping.static_loader import StaticMapLoader
from omop_etl.mapping.concept_service import ConceptMappingService
from omop_etl.semantic_mapping.api import SemanticService

from omop_etl.omop.builders.person_builder import PersonRowBuilder
from omop_etl.omop.models.person import PersonRow


class BuildOmopRows:
    def __init__(
        self,
        harmonized_data: HarmonizedData,
        static_mapping_path: Path,
        semantic_path: Path | None = None,
    ):
        self._hd = harmonized_data

        static_index = StaticMapLoader(static_mapping_path).as_index()
        structural_index: Mapping[str, list] = {}
        semantic_index = None
        if semantic_path is not None:
            sem_service = SemanticService(
                harmonized_data=harmonized_data,
                semantic_path=semantic_path,
            )
            batch = sem_service.run()
            # todo: expand:
            # semantic_index = SemanticIndex.lookup_exact(batch)

        self._concepts = ConceptMappingService(
            static_index=static_index,
            structural_index=structural_index,
            semantic_index=semantic_index,
        )

        self._person_builder = PersonRowBuilder(self._concepts)
        # self._measurement_builder = ...

    def _generate_person_ids(self) -> dict[str, int]:
        mapping: dict[str, int] = {}
        gen = count(start=1)
        for p in sorted(self._hd.patients, key=lambda x: x.patient_id):
            mapping[p.patient_id] = next(gen)
        return mapping

    def build_all_rows(self) -> dict[str, list[object]]:
        pid_map = self._generate_person_ids()
        person_rows: list[PersonRow] = []

        for p in self._hd.patients:
            pid = pid_map[p.patient_id]
            person_rows.append(self._person_builder.build(patient=p, person_id=pid))

        # todo: return struct instead of dict
        return {
            "person": person_rows,
            # "measurement": measurement_rows,
        }
