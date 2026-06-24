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


class EpisodeBuilder(OmopBuilder[EpisodeRow]):
    """
    Oncology Episode rows over a line of therapy and its cycles:

    - Treatment Regimen (treatment_regimen, 32531): one per line of therapy
      (TreatmentCycleComponent.treatment_number), spanning that line's cycles
      (min start to max end). Published under SourceReference(patient,
      TREATMENT_CYCLES, (treatment_number,)) so EpisodeEventBuilder can link the
      line's drug_exposure rows to it via EPISODE_EVENT.
    - Treatment Cycle (treatment_cycle, 32532): one per (treatment_number,
      cycle_number), spanning that cycle's components. Combination drugs split
      into several components sharing one (treatment_number, cycle_number)
      collapse into a single cycle episode; the per-ingredient drug_exposure rows
      attach to it via EPISODE_EVENT. Each cycle's episode_parent_id is its line's
      Treatment Regimen and it reuses the regimen's object concept.

    Each kind resolves its own Episode concept: a missing treatment_regimen
    concept skips all episodes (a 0 here is meaningless), a missing treatment_cycle
    concept skips just the cycles.
    """

    table_name: ClassVar[str] = OmopTables.EPISODE

    def build(self, ctx: BuildContext) -> BuildResult[EpisodeRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        ecrf = self.concepts.resolve("ecrf", domains={OmopDomain.TYPE_CONCEPT})
        if not ecrf:
            log.warning("No eCRF concept found in structural file, setting to 0")
        ecrf_concept = ecrf[0].concept_id if ecrf else 0

        built_rows: list[EpisodeRow] = []
        publications: list[RowPublication] = []

        regimen_rows = self._build_treatment_regimens(
            cycles=patient.treatment_cycles,
            patient_id=patient.patient_id,
            person_id=person_id,
            ecrf_concept=ecrf_concept,
        )
        built_rows.extend(regimen_rows)

        # cycle inherits its line's regimen as episode_parent_id
        regimen_by_line = {row.episode_number: row for row in regimen_rows if row.episode_number is not None}

        cycle_rows = self._build_treatment_cycle_episodes(
            cycles=patient.treatment_cycles,
            patient_id=patient.patient_id,
            person_id=person_id,
            ecrf_concept=ecrf_concept,
            regimen_by_line=regimen_by_line,
        )
        built_rows.extend(cycle_rows)

        publications.extend(self._publish_episodes(patient_id=patient.patient_id, rows=regimen_rows))
        publications.extend(self._publish_episodes(patient_id=patient.patient_id, rows=cycle_rows))

        return BuildResult(rows=tuple(built_rows), publications=tuple(publications))

    def _build_treatment_regimens(
        self,
        cycles: tuple[TreatmentCycleComponent, ...],
        patient_id: str,
        person_id: int,
        ecrf_concept: int,
    ) -> list[EpisodeRow]:
        grouped_by_treatment = self._group_by_treatment_number(cycles)
        if not grouped_by_treatment:
            return []

        built_rows: list[EpisodeRow] = []

        regimen = self.concepts.resolve("treatment_regimen", domains={OmopDomain.EPISODE})
        if not regimen:
            log.warning("No treatment_regimen Episode concept, skipping episodes for %s", patient_id)
            return []
        regimen_concept = regimen[0].concept_id

        for treatment_line in sorted(grouped_by_treatment):
            line_cycles = grouped_by_treatment[treatment_line]

            counts = Counter(cycle.source_treatment_name for cycle in line_cycles)
            treatment_names = [s for s, count in counts.most_common() if s is not None]
            if len(treatment_names) != 1:
                log.warning(
                    "Received TreatmentCycleComponent instances of same treatment with different drugs for %s",
                    patient_id,
                )

            treatment_name = treatment_names[0] if treatment_names else None

            drug_regimen = self.concepts.resolve(
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                treatment_name,
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

            start_date, end_date = self._date_spans(line_cycles)

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

    def _build_treatment_cycle_episodes(
        self,
        cycles: tuple[TreatmentCycleComponent, ...],
        patient_id: str,
        person_id: int,
        ecrf_concept: int,
        regimen_by_line: dict[int, EpisodeRow],
    ) -> list[EpisodeRow]:
        """
        One Treatment Cycle episode per (treatment_number, cycle_number), spanning
        that cycle's components. Each cycle inherits its line's regimen as
        episode_parent_id and reuses the regimen's object concept (None / 0 if the
        line produced no regimen).
        """

        cycle = self.concepts.resolve("treatment_cycle", domains={OmopDomain.EPISODE})
        if not cycle:
            log.warning("No treatment_cycle Episode concept, skipping treatment cycle episodes for %s", patient_id)
            return []
        cycle_concept = cycle[0].concept_id

        grouped = self._group_by_treatment_cycle_numbers(cycles)

        built_rows: list[EpisodeRow] = []

        for treatment_line, cycle_number in sorted(grouped):
            components = grouped[(treatment_line, cycle_number)]
            start_date, end_date = self._date_spans(components)
            start_datetime, end_datetime = (
                dt.datetime(start_date.year, start_date.month, start_date.day),
                dt.datetime(end_date.year, end_date.month, end_date.day),
            )

            counts = Counter(c.source_treatment_name for c in components)
            treatment_names = [s for s, count in counts.most_common() if s is not None]
            treatment_name = treatment_names[0] if treatment_names else None

            parent_regimen = regimen_by_line.get(treatment_line)
            parent_id = parent_regimen.episode_id if parent_regimen else None
            object_concept_id = parent_regimen.episode_object_concept_id if parent_regimen else 0

            episode_id = self.generate_row_id(
                patient_id,
                Patient.Collections.TREATMENT_CYCLES,
                treatment_line,
                cycle_number,
            )

            built_rows.append(
                EpisodeRow(
                    episode_id=episode_id,
                    person_id=person_id,
                    episode_concept_id=cycle_concept,
                    episode_start_date=start_date,
                    episode_start_datetime=start_datetime,
                    episode_end_date=end_date,
                    episode_end_datetime=end_datetime,
                    episode_parent_id=parent_id,
                    episode_number=cycle_number,
                    episode_object_concept_id=object_concept_id,
                    episode_type_concept_id=ecrf_concept,
                    episode_source_value=treatment_name,
                )
            )

        return built_rows

    @staticmethod
    def _publish_episodes(
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

    @staticmethod
    def _group_by_treatment_cycle_numbers(cycles: tuple[TreatmentCycleComponent, ...]) -> dict[tuple[int, int], list[TreatmentCycleComponent]]:
        lines = defaultdict(list)
        for line in cycles:
            start_date = line.start_date
            treatment_number = line.treatment_number
            cycle_number = line.cycle_number
            if start_date is None or treatment_number is None or cycle_number is None:
                continue
            lines[(treatment_number, cycle_number)].append(line)

        return lines

    @staticmethod
    def _date_spans(cycles: list[TreatmentCycleComponent]) -> tuple[dt.date, dt.date]:
        starts: list[dt.date] = []
        ends: list[dt.date] = []
        for c in cycles:
            start = c.start_date
            if start is None:
                continue
            starts.append(start)
            ends.append(c.end_date or start)

        start_date, end_date = min(starts), max(ends)
        return start_date, end_date
