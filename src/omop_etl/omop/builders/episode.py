from typing import ClassVar, NamedTuple
from dataclasses import dataclass
from collections import defaultdict, Counter
from itertools import groupby
from logging import getLogger
import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.omop.models.rows import EpisodeRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain
from omop_etl.omop.builders.base import (
    BuildContext,
    BuildResult,
    OmopBuilder,
)
from omop_etl.omop.core.linkage import (
    RowPublication,
    SourceReference,
    OmopRowReference,
)

log = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _DynamicRun:
    """
    A maximal run of consecutive same-status tumor assessments to one Disease
    Dynamic Episode. Carries the run's assessments so EpisodeEventBuilder can link
    their measurement rows.
    """

    status_concept_id: int
    start_date: dt.date
    end_date: dt.date | None
    assessments: tuple[TumorAssessment, ...]


class _StatusedAssessment(NamedTuple):
    """
    An evaluable assessment tagged with its dynamic status concept and date,
    for run-length grouping inside dynamic_runs.
    """

    status_concept_id: int
    assessment_date: dt.date
    assessment: TumorAssessment


class EpisodeBuilder(OmopBuilder[EpisodeRow]):
    """
    Oncology Episode rows for one patient's cancer and its treatment.

    - Disease Episode: one per patient, the span of the primary cancer.
      object_concept_id = the tumor's condition concept, start = the cancer
      condition's start date, end = date_of_death (or NULL). Scoped to
      the primary-cancer condition_occurrence row published by
      ConditionOccurrenceBuilder, and the parent of the regimens below.
    - Treatment Regimen: one per line of therapy
      (TreatmentCycleComponent.treatment_number), spanning that line's cycles
      (min start to max end). episode_parent_id = the Disease Episode, the parent
      of its cycles via their episode_parent_id.
    - Treatment Cycle: one per (treatment_number, cycle_number), spanning that
      cycle's components. Combination drugs split into several components sharing
      one (treatment_number, cycle_number) collapse into a single cycle episode.
      Each cycle's episode_parent_id is its line's Treatment Regimen and it
      reuses the regimen's object concept. Published under
      SourceReference(patient, TREATMENT_CYCLES, (treatment_number, cycle_number))
      so EpisodeEventBuilder can attach the cycle's drug_exposure rows via
      EPISODE_EVENT.

    Each kind resolves its own Episode concept and degrades independently.
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

        disease_row = self._build_disease_episode(ctx, person_id, ecrf_concept)
        disease_episode_id: int | None = None
        tumor = patient.tumor_type
        if disease_row is not None and tumor is not None:
            built_rows.append(disease_row)
            disease_episode_id = disease_row.episode_id
            # publish using tumor type so EpisodeEventBuilder links to cancer type
            publications.append(self._publish_disease_episode(patient.patient_id, tumor, disease_row))

        regimen_rows = self._build_treatment_regimens(
            cycles=patient.treatment_cycles,
            patient_id=patient.patient_id,
            person_id=person_id,
            ecrf_concept=ecrf_concept,
            disease_episode_id=disease_episode_id,
        )
        built_rows.extend(regimen_rows)

        # cycle inherits its line's regimen as episode_parent_id
        regimen_by_line = {row.episode_number: row for row in regimen_rows if row.episode_number is not None}

        cycle = self.concepts.resolve("treatment_cycle", domains={OmopDomain.EPISODE})
        if not cycle:
            log.warning("No treatment_cycle Episode concept, skipping treatment cycle episodes for %s", patient.patient_id)
        else:
            cycle_concept = cycle[0].concept_id
            grouped = self._group_by_treatment_cycle_numbers(patient.treatment_cycles)
            for treatment_line, cycle_number in sorted(grouped):
                cycle_row = self._build_cycle_episode(
                    components=grouped[(treatment_line, cycle_number)],
                    treatment_line=treatment_line,
                    cycle_number=cycle_number,
                    cycle_concept=cycle_concept,
                    ecrf_concept=ecrf_concept,
                    person_id=person_id,
                    patient_id=patient.patient_id,
                    regimen_by_line=regimen_by_line,
                )
                built_rows.append(cycle_row)
                publications.append(
                    self._publish_cycle_episode(
                        patient.patient_id,
                        (treatment_line, cycle_number),
                        (cycle_row,),
                    )
                )

        for run in self.dynamic_runs(self.concepts, ctx):
            dynamic_row = self._build_dynamic_episode(run, person_id, ecrf_concept, disease_episode_id, patient.patient_id)
            built_rows.append(dynamic_row)
            publications.append(self._publish_dynamic_episode(patient.patient_id, run.start_date, dynamic_row))

        return BuildResult(rows=tuple(built_rows), publications=tuple(publications))

    def _build_disease_episode(
        self,
        ctx: BuildContext,
        person_id: int,
        ecrf_concept: int,
    ) -> EpisodeRow | None:
        """
        One Disease Episode spanning the primary cancer, scoped to the tumor's
        published condition_occurrence row(s): the object concept and start date
        come from there, so no published condition means no cancer to scope,
        and no episode date (returns None).
        """
        patient = ctx.patient
        tumor = patient.tumor_type
        if tumor is None:
            return None

        disease = self.concepts.resolve("disease_episode", domains={OmopDomain.EPISODE})
        if not disease:
            log.warning("No disease_episode Episode concept, skipping Disease Episode for %s", patient.patient_id)
            return None
        disease_concept = disease[0].concept_id

        condition_refs = ctx.resolve_rows(
            OmopTables.CONDITION_OCCURRENCE,
            SourceReference(patient.patient_id, Patient.Singletons.TUMOR_TYPE, tumor.natural_key()),
        )
        if not condition_refs:
            log.warning("No primary-cancer condition published for %s, skipping Disease Episode", patient.patient_id)
            return None
        if len(condition_refs) > 1:
            log.warning(
                "Tumor type for %s mapped to %d Condition concepts, Disease Episode uses the first, review the mapping",
                patient.patient_id,
                len(condition_refs),
            )
        primary_condition = condition_refs[0]

        start_date = primary_condition.event_date
        if start_date is None:
            log.warning("No condition start date published for %s, skipping Disease Episode", patient.patient_id)
            return None
        end_date = patient.date_of_death

        episode_id = self.generate_row_id(patient.patient_id, Patient.Singletons.TUMOR_TYPE, *tumor.natural_key())
        main_tumor_type = tumor.main_tumor_type

        return EpisodeRow(
            episode_id=episode_id,
            person_id=person_id,
            episode_concept_id=disease_concept,
            episode_start_date=start_date,
            episode_start_datetime=dt.datetime(start_date.year, start_date.month, start_date.day),
            episode_end_date=end_date,
            episode_end_datetime=dt.datetime(end_date.year, end_date.month, end_date.day) if end_date else None,
            episode_object_concept_id=primary_condition.primary_concept_id,
            episode_type_concept_id=ecrf_concept,
            episode_source_value=main_tumor_type[:50] if main_tumor_type else None,
        )

    def _build_treatment_regimens(
        self,
        cycles: tuple[TreatmentCycleComponent, ...],
        patient_id: str,
        person_id: int,
        ecrf_concept: int,
        disease_episode_id: int | None,
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
                    episode_parent_id=disease_episode_id,
                    episode_number=treatment_line,
                    episode_object_concept_id=drug_regimen_concept,
                    episode_type_concept_id=ecrf_concept,
                    episode_source_value=treatment_name,
                )
            )

        return built_rows

    def _build_cycle_episode(
        self,
        *,
        components: list[TreatmentCycleComponent],
        treatment_line: int,
        cycle_number: int,
        cycle_concept: int,
        ecrf_concept: int,
        person_id: int,
        patient_id: str,
        regimen_by_line: dict[int, EpisodeRow],
    ) -> EpisodeRow:
        """
        One Treatment Cycle episode from a single (treatment_number, cycle_number)
        group. Combination drugs split into several components share that group, so
        they collapse into this one episode. Inherits the line's regimen as
        episode_parent_id and reuses its object concept (None / 0 if the line
        produced no regimen).
        """
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

        return EpisodeRow(
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

    def _build_dynamic_episode(
        self,
        run: _DynamicRun,
        person_id: int,
        ecrf_concept: int,
        disease_episode_id: int | None,
        patient_id: str,
    ) -> EpisodeRow:
        """One Disease Dynamic Episode from a status run. Parent = the Disease Episode."""
        start, end = run.start_date, run.end_date
        return EpisodeRow(
            episode_id=self.generate_row_id(patient_id, Patient.Collections.TUMOR_ASSESSMENTS, start),
            person_id=person_id,
            episode_concept_id=run.status_concept_id,
            episode_start_date=start,
            episode_start_datetime=dt.datetime(start.year, start.month, start.day),
            episode_end_date=end,
            episode_end_datetime=dt.datetime(end.year, end.month, end.day) if end else None,
            episode_parent_id=disease_episode_id,
            episode_type_concept_id=ecrf_concept,
        )

    @staticmethod
    def dynamic_runs(concepts: ConceptLookupService, ctx: BuildContext) -> list[_DynamicRun]:
        """
        Disease Dynamic runs: the tumor-assessment response time-series run-length-encoded.
        Each assessment's effective response (iRECIST > RECIST > RANO, Not-Evaluable skipped)
        maps to a dynamic status concept, maximal stretches of consecutive same-status
        assessments form one run, spanning [first date, day before the next run's
        start) - open-ended on the last. NE assessments carry no status, so they drop
        out and let their same-status neighbours merge. Shared by EpisodeBuilder (the
        episode rows) and EpisodeEventBuilder (the measurement links), so the grouping
        is identical on both sides.
        """
        patient = ctx.patient

        # evaluable assessments in date order, each tagged with its dynamic status
        # concept (Not-Evaluable / unmapped assessments carry no status and drop out)
        statused: list[_StatusedAssessment] = []
        for ta in sorted(patient.tumor_assessments, key=lambda a: a.date or dt.date.min):
            date = ta.date
            if date is None:
                continue
            status = EpisodeBuilder._dynamic_status(concepts, ta, patient.patient_id)
            if status is not None:
                statused.append(_StatusedAssessment(status, date, ta))

        # one run per maximal stretch of consecutive same-status assessments
        runs_of_assessments: list[list[_StatusedAssessment]] = [list(group) for _, group in groupby(statused, key=lambda s: s.status_concept_id)]

        runs: list[_DynamicRun] = []
        for i, run in enumerate(runs_of_assessments):
            # a run ends the day before the next run starts, the last run stays open
            next_start = runs_of_assessments[i + 1][0].assessment_date if i + 1 < len(runs_of_assessments) else None
            end_date = next_start - dt.timedelta(days=1) if next_start is not None else None
            runs.append(
                _DynamicRun(
                    status_concept_id=run[0].status_concept_id,
                    start_date=run[0].assessment_date,
                    end_date=end_date,
                    assessments=tuple(s.assessment for s in run),
                )
            )
        return runs

    @staticmethod
    def _dynamic_status(concepts: ConceptLookupService, ta: TumorAssessment, patient_id: str) -> int | None:
        """
        Maps the disease-dynamic Episode concept for one assessmens from a tumor assessment's response scale
        (iRECIST > RECIST > RANO. Not-Evaluable & unmapped scales skipped).
        """
        for value_set, value in (
            ("response_irecist", ta.irecist_response),
            ("response_recist", ta.recist_response),
            ("response_rano", ta.rano_response),
        ):
            if value is None:
                continue
            response = concepts.resolve(value_set, value, domains={OmopDomain.MEASUREMENTS})
            if not response:
                continue  # not evaluable (Meas Value domain) or unmapped
            status = concepts.resolve("dynamic_status", str(response[0].concept_id), domains={OmopDomain.EPISODE})
            if status:
                return status[0].concept_id
            log.warning("No dynamic_status mapping for response concept %s (patient %s)", response[0].concept_id, patient_id)
        return None

    @staticmethod
    def _publish_dynamic_episode(patient_id: str, start_date: dt.date, row: EpisodeRow) -> RowPublication:
        """
        Publish a Dynamic Episode under its run-start SourceReference so
        EpisodeEventBuilder can attach the run's measurement rows.
        """
        return RowPublication(
            target_table=OmopTables.EPISODE,
            source_ref=SourceReference(patient_id, Patient.Collections.TUMOR_ASSESSMENTS, (start_date,)),
            rows=(
                OmopRowReference(
                    table=OmopTables.EPISODE,
                    row_id=row.episode_id,
                    primary_concept_id=row.episode_concept_id,
                ),
            ),
        )

    @staticmethod
    def _publish_cycle_episode(
        patient_id: str,
        natural_key: tuple[int, int],
        rows: tuple[EpisodeRow, ...],
    ) -> RowPublication:
        """Publish a cycle episode under its explicit (treatment_number,
        cycle_number) SourceReference so EpisodeEventBuilder can resolve it to
        attach the cycle's drug_exposure rows."""
        return RowPublication(
            target_table=OmopTables.EPISODE,
            source_ref=SourceReference(patient_id, Patient.Collections.TREATMENT_CYCLES, natural_key),
            rows=tuple(
                OmopRowReference(
                    table=OmopTables.EPISODE,
                    row_id=row.episode_id,
                    primary_concept_id=row.episode_concept_id,
                )
                for row in rows
            ),
        )

    @staticmethod
    def _publish_disease_episode(
        patient_id: str,
        tumor: TumorType,
        row: EpisodeRow,
    ) -> RowPublication:
        """
        Publish the Disease Episode under the tumor's SourceReference (the same
        anchor as its cancer condition_occurrence row) so EpisodeEventBuilder can
        link it to that condition via EPISODE_EVENT.
        """
        return RowPublication(
            target_table=OmopTables.EPISODE,
            source_ref=SourceReference(patient_id, Patient.Singletons.TUMOR_TYPE, tumor.natural_key()),
            rows=(
                OmopRowReference(
                    table=OmopTables.EPISODE,
                    row_id=row.episode_id,
                    primary_concept_id=row.episode_concept_id,
                ),
            ),
        )

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
