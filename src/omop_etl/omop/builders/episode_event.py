from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import BuildContext, BuildResult, OmopBuilder
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.core.linkage import SourceReference
from omop_etl.omop.models.rows import EpisodeEventRow
from omop_etl.omop.models.tables import OmopTables

log = getLogger(__name__)


class EpisodeEventBuilder(OmopBuilder[EpisodeEventRow]):
    """
    EPISODE_EVENT junction rows. Three kinds of links, all resolved via the base
    _resolve_link_targets helper (cdm54 episode_event):

    - Treatment Cycle to drug_exposure: each cycle episode (published under
      SourceReference(patient, TREATMENT_CYCLES, (treatment_number, cycle_number)))
      links to the drug_exposure rows of that cycle's components. A drug links to
      the most granular episode it belongs to, the cycle -> regimen -> disease
      hierarchy is carried by episode_parent_id, not duplicated here.
    - Disease Episode to condition_occurrence: the Disease Episode (published under
      SourceReference(patient, TUMOR_TYPE, tumor.natural_key())) links to the
      primary-cancer condition_occurrence row(s).
    - Disease Dynamic Episode to measurement: each dynamic episode (published under
      SourceReference(patient, TUMOR_ASSESSMENTS, (run_start_date,))) links to the
      measurement rows of its run's tumor assessments.
      Reuses EpisodeBuilder.dynamic_runs so the run grouping matches.

    Must run after EpisodeBuilder, DrugExposureBuilder, ConditionOccurrenceBuilder
    and MeasurementBuilder so all the publications it resolves are present in the
    BuildContext.
    """

    table_name: ClassVar[str] = OmopTables.EPISODE_EVENT

    def build(self, ctx: BuildContext) -> BuildResult[EpisodeEventRow]:
        rows: list[EpisodeEventRow] = []
        rows.extend(self._cycle_drug_links(ctx))
        rows.extend(self._disease_condition_links(ctx))
        rows.extend(self._dynamic_measurement_links(ctx))
        return BuildResult(rows=tuple(rows), publications=())

    def _cycle_drug_links(self, ctx: BuildContext) -> list[EpisodeEventRow]:
        patient = ctx.patient
        cycles_by_key = EpisodeBuilder._group_by_treatment_cycle_numbers(patient.treatment_cycles)  # noqa

        rows: list[EpisodeEventRow] = []
        for (treatment_line, cycle_number), components in cycles_by_key.items():
            episode_refs = ctx.resolve_rows(
                OmopTables.EPISODE,
                SourceReference(patient.patient_id, Patient.Collections.TREATMENT_CYCLES, (treatment_line, cycle_number)),
            )
            if not episode_refs:
                continue
            episode_id = episode_refs[0].row_id

            for component in components:
                targets = self._resolve_link_targets(
                    ctx,
                    target_table=OmopTables.DRUG_EXPOSURE,
                    source_ref=SourceReference(
                        patient.patient_id,
                        Patient.Collections.TREATMENT_CYCLES,
                        component.natural_key(),
                    ),
                )
                rows.extend(
                    EpisodeEventRow(
                        episode_id=episode_id,
                        event_id=target.event_id,
                        episode_event_field_concept_id=target.field_concept_id,
                    )
                    for target in targets
                )

        return rows

    def _disease_condition_links(self, ctx: BuildContext) -> list[EpisodeEventRow]:
        patient = ctx.patient
        tumor = patient.tumor_type
        if tumor is None:
            return []

        tumor_ref = SourceReference(patient.patient_id, Patient.Singletons.TUMOR_TYPE, tumor.natural_key())
        episode_refs = ctx.resolve_rows(OmopTables.EPISODE, tumor_ref)
        if not episode_refs:
            return []
        disease_episode_id = episode_refs[0].row_id

        targets = self._resolve_link_targets(
            ctx,
            target_table=OmopTables.CONDITION_OCCURRENCE,
            source_ref=tumor_ref,
        )
        return [
            EpisodeEventRow(
                episode_id=disease_episode_id,
                event_id=target.event_id,
                episode_event_field_concept_id=target.field_concept_id,
            )
            for target in targets
        ]

    def _dynamic_measurement_links(self, ctx: BuildContext) -> list[EpisodeEventRow]:
        patient = ctx.patient

        rows: list[EpisodeEventRow] = []
        for run in EpisodeBuilder.dynamic_runs(self.concepts, ctx):
            episode_refs = ctx.resolve_rows(
                OmopTables.EPISODE,
                SourceReference(patient.patient_id, Patient.Collections.TUMOR_ASSESSMENTS, (run.start_date,)),
            )
            if not episode_refs:
                continue
            episode_id = episode_refs[0].row_id

            for ta in run.assessments:
                targets = self._resolve_link_targets(
                    ctx,
                    target_table=OmopTables.MEASUREMENT,
                    source_ref=SourceReference(patient.patient_id, Patient.Collections.TUMOR_ASSESSMENTS, ta.natural_key()),
                )
                rows.extend(
                    EpisodeEventRow(
                        episode_id=episode_id,
                        event_id=target.event_id,
                        episode_event_field_concept_id=target.field_concept_id,
                    )
                    for target in targets
                )

        return rows
