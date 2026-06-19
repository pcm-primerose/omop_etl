from typing import ClassVar
from collections import defaultdict, Counter
from logging import getLogger
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.omop.builders.base import BuildContext, BuildResult, OmopBuilder
from omop_etl.omop.core.linkage import RowPublication, SourceReference, OmopRowReference
from omop_etl.omop.models.rows import EpisodeRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)

# todo: map study drugs to regimen concepts


class EpisodeBuilder(OmopBuilder[EpisodeRow]):
    """
    Treatment-regimen episodes are an abstraction over the drug_exposure rows of a
    line of therapy, one episode record per line of therapy
    (TreatmentCycleComponent.treatment_number), spanning that line's cycles
    (min start to max end).

    Each episode is published under SourceReference(patient, TREATMENT_CYCLES,
    (treatment_number,)) so EpisodeEventBuilder can link the line's
    drug_exposure rows to it via EPISODE_EVENT.

    episode_concept_id is the 'treatment_regimen' Episode concept,
    episodes are not emitted when it is missing, since a 0 here is meaningless.
    """

    table_name: ClassVar[str] = OmopTables.EPISODE

    def build(self, ctx: BuildContext) -> BuildResult[EpisodeRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        # todo: use treatment_regimen or treatment_cycle concept?
        regimen = self.concepts.lookup_structural("treatment_regimen", domains={OmopDomain.EPISODE})
        if regimen is None:
            log.warning("No treatment_regimen Episode concept, skipping episodes for %s", patient.patient_id)
            return BuildResult(rows=(), publications=())
        regimen_concept = regimen.concept_id

        ecrf = self.concepts.lookup_structural("ecrf", domains={OmopDomain.TYPE_CONCEPT})
        if ecrf is None:
            log.warning("No eCRF concept found in structural file, setting to 0")
        ecrf_concept = ecrf.concept_id if ecrf else 0

        built_rows: list[EpisodeRow] = []
        publications: list[RowPublication] = []

        treatment_episode_rows = self._build_treatment_episodes(
            cycles=patient.treatment_cycles,
            patient_id=patient.patient_id,
            person_id=person_id,
            ecrf_concept=ecrf_concept,
            regimen_concept=regimen_concept,
        )
        built_rows.extend(treatment_episode_rows)

        treatment_episode_publications = self._publish_treatment_episodes(patient_id=patient.patient_id, rows=treatment_episode_rows)
        publications.extend(treatment_episode_publications)

        return BuildResult(rows=tuple(built_rows), publications=tuple(publications))

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

        for treatment_line in grouped_by_treatment:
            cycles = grouped_by_treatment[treatment_line]

            counts = Counter(cycle.source_treatment_name for cycle in cycles)
            treatment_names = [s for s, count in counts.most_common() if s is not None]
            if len(treatment_names) != 1:
                log.warning(
                    "Recieved treatment TreatmentCycleComponent instances of same line with different drugs for %s",
                    patient_id,
                )

            treatment_name = treatment_names[0] if treatment_names else None

            drug_regimen = self.concepts.lookup_semantic(
                patient_id,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                value=treatment_name,
                domains={OmopDomain.REGIMEN},
            )

            if len(drug_regimen) > 1:
                log.warning(
                    "One source_treatment_name entry maps to several concepts in Episode builder, taking first for %s",
                    patient_id,
                )
            if len(drug_regimen) == 0:
                log.warning(
                    "Unmapped source_treatment_name, setting drug regimen concept id to 0, for %s",
                    patient_id,
                )
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
                    episode_source_value=treatment_name,
                )
            )

        return built_rows

    @staticmethod
    def _publish_treatment_episodes(
        patient_id: str,
        rows: list[EpisodeRow],
    ) -> list[RowPublication]:
        publications: list[RowPublication] = []

        for row in rows:
            publications.append(
                RowPublication(
                    target_table=OmopTables.EPISODE,
                    source_ref=SourceReference(patient_id, Patient.Collections.TREATMENT_CYCLES, (row.episode_number,)),
                    rows=(
                        OmopRowReference(
                            table=OmopTables.EPISODE,
                            row_id=row.episode_id,
                            primary_concept_id=row.episode_concept_id,
                        ),
                    ),
                )
            )

        return publications

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
