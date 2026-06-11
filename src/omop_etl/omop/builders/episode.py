from typing import ClassVar
from collections import defaultdict
from logging import getLogger
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.omop.builders.base import BuildContext, BuildResult, OmopBuilder
from omop_etl.omop.core.linkage import RowPublication
from omop_etl.omop.models.rows import EpisodeRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)

# todo: map study drugs to regimen concepts


class EpisodeBuilder(OmopBuilder[EpisodeRow]):
    """ """

    table_name: ClassVar[str] = OmopTables.EPISODE

    def build(self, ctx: BuildContext) -> BuildResult[EpisodeRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        # todo: use treatment_regimen or treatment_cycle? think: regimen
        regimen = self.concepts.lookup_structural("treatment_regimen", domains=OmopDomain.EPISODE)
        if regimen is None:
            log.warning("No treatment_egiment Episode concept, skipping episodes for %s", patient.patient_id)
            return BuildResult(rows=(), publications=())
        regiment_concept = regimen.concept_id

        built_rows: list[EpisodeRow] = []
        publications: list[RowPublication] = []

        treatment_episode_rows = self._build_treatment_episodes(
            cycles=patient.treatment_cycles,
            patient_id=patient.patient_id,
            person_id=person_id,
        )
        if treatment_episode_rows:
            publications.append(
                self._publish_treatment_episodes(
                    patient_id=patient.patient_id,
                    cycles=patient.treatment_cycles,
                    treatment_episode_rows=treatment_episode_rows,
                )
            )

        # if rows, publish and extend built rows
        # todo: this kind of breaks the design from other builders as we iterate
        # over all the patient's data, so it makes more sense to publish from inside the builder?

        return BuildResult(rows=tuple(built_rows), publications=tuple(publications))

    # just iterate over grouped dict,
    # return rows for EpisodeRow for each valid cycle per treatment,
    # and generate FK from treatment cycle component NK fields,
    # if we can avoid passing Patient to this and just use the dict it'd be nicer
    # then if we get rows, we publish them from build() using separate method (cleaner)
    def _build_treatment_episodes(
        self,
        cycles: tuple[TreatmentCycleComponent, ...],
        patient_id: str,
        person_id: int,
        regimen_concept: int,
    ) -> list[EpisodeRow]:
        grouped_by_treatment = self._group_by_treatment_number(cycles)
        if not grouped_by_treatment:
            return []

        built_rows: list[EpisodeRow] = []

        for treatment_number in grouped_by_treatment:
            cycles = grouped_by_treatment[treatment_number]
            start_date = min(c.start_date for c in cycles)
            start_datetime = dt.datetime(start_date.year, start_date.month, start_date.day)
            end_date = max((c.end_date or c.start_date) for c in cycles)
            end_datetime = dt.datetime(end_date.year, end_date.month, end_date.day)
            episode_id = self.generate_row_id(
                patient_id,
                Patient.Collections.TREATMENT_CYCLES,
                treatment_number,
            )

            built_rows.append(
                EpisodeRow(
                    episode_id=episode_id,
                    person_id=person_id,
                    episode_concept_id=regimen_concept,
                    episode_start_date=start_date,
                    episode_start_datetime=start_datetime,
                    episode_end_date=end_date,
                    episode_end_datetime=end_datetime,
                    episode_number=treatment_number,
                    episode_object_concept_id=0,  # todo: use semantic mapping of Study Drugs (Regimen concepts)
                    episode_type_concept_id=0,  # episode_type_concept_id,
                    episode_source_value="",  # source_value[:50] if source_value else None,
                )
            )

        return built_rows

    @staticmethod
    def _publish_treatment_episodes(
        patient_id: str,
        cycles: tuple[TreatmentCycleComponent, ...],
        treatment_episode_rows: list[EpisodeRow],
    ):
        pass

    @staticmethod
    def _group_by_treatment_number(treatment_cycles: tuple[TreatmentCycleComponent, ...]) -> dict[int, list[TreatmentCycleComponent]]:
        lines = defaultdict(list)
        for line in treatment_cycles:
            start_date = line.start_date
            treatment_number = line.treatment_number
            if start_date is None or treatment_number is None:
                continue
            lines[treatment_number].append(line)

        return lines
