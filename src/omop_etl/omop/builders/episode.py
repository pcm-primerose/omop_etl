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
        # fixme
        regimen = self.concepts.lookup_structural("treatment_regimen", domains={OmopDomain.EPISODE})
        if regimen is None:
            log.warning("No treatment_regiment Episode concept, skipping episodes for %s", patient.patient_id)
            return BuildResult(rows=(), publications=())
        regiment_concept = regimen.concept_id

        ecrf = self.concepts.lookup_structural("ecrf", domains={OmopDomain.TYPE_CONCEPT})
        if ecrf is None:
            log.warning("No eCRF concept found in structural file, setting to 0")
        ecrf_concept = ecrf.concept_id if ecrf else 0

        built_rows: list[EpisodeRow] = []
        publications: list[RowPublication] = []

        treatment_episode_rows = self._build_treatment_episodes(
            cycles=patient.treatment_cycles, patient_id=patient.patient_id, person_id=person_id, ecrf_concept=ecrf_concept, regimen_concept=regiment_concept
        )
        # todo: need to pre-processes for publication method
        # if treatment_episode_rows:
        # publications.append(
        #     )
        # )

        print(f"treatment episodes: {treatment_episode_rows}")

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
        ecrf_concept: int,
    ) -> list[EpisodeRow]:
        grouped_by_treatment = self._group_by_treatment_number(cycles)
        if not grouped_by_treatment:
            return []

        built_rows: list[EpisodeRow] = []

        # todo: map study drugs
        # 1. expand dict helper method to key by tuple(treatment_number, study_drug)?
        # 2. get study drgus tuple and match to ... no
        # 3. check most common source treatment name ... no
        # 4.

        # problem is that the source_treatment_name is in each cycle,
        # but now we index by treatment name,
        # and we then just map each treatment instead of each cycle,
        # so we need to get the "global" treatment name for a combined cycle collection for each treatment number,
        # can either extract this from

        for treatment_line in grouped_by_treatment:
            cycles = grouped_by_treatment[treatment_line]

            # problem is: across every instance of TreatmentCycleComponent *with the same treatment number*,
            # the source_treatment_name should be the same, we just trust the Patient model
            treatment_name = set(cycle.source_treatment_name for cycle in cycles)
            if len(treatment_name) != 1:
                # this should never happen
                raise RuntimeError("TreatmentCycleComponent has several source_treatment_names across instances withtin one treatment_number!!!!")

            # todo: refactor semantic mapping to be invariant to Patient indices
            drug_regimen = self.concepts.lookup_semantic(
                patient_id=patient_id,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                leaf_index=None,
                domains={OmopDomain.REGIMEN},
            )

            print(f"drug regimen: {drug_regimen}")
            drug_regimen_concept = drug_regimen[0].concept_id if drug_regimen else 0

            starts: list[dt.date] = []
            ends: list[dt.date] = []
            for c in cycles:
                start = c.start_date
                if start is None:
                    continue
                starts.append(start)
                ends.append(c.end_date or start)

            start_date, end_date = min(starts), max(ends)

            start_datetime, end_datetime = (
                dt.datetime(start_date.year, start_date.month, start_date.day),
                dt.datetime(end_date.year, end_date.month, end_date.day),
            )

            episode_id = self.generate_row_id(
                patient_id,
                Patient.Collections.TREATMENT_CYCLES,
                treatment_line,
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
                    episode_number=treatment_line,
                    episode_object_concept_id=drug_regimen_concept,
                    episode_type_concept_id=ecrf_concept,
                    episode_source_value=",".join(sorted(treatment_name)),
                )
            )

        return built_rows

    @staticmethod
    def _publish_treatment_episodes(
        patient_id: str,
        treatment_episode_rows: list[EpisodeRow],
        grouped_by_treatment: dict[int, tuple[TreatmentCycleComponent]],
        regimen_concept: int,
        episode_id: int,
    ) -> list[RowPublication]:
        publications: list[RowPublication] = []

        # loop over treatment numbers in grouped_by_treatment

        for treatment_number in grouped_by_treatment.items():
            print(f"treatment_number: {treatment_number}")

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
