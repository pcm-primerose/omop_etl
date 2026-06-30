import datetime as dt
from logging import getLogger
from typing import ClassVar, Any

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.end_of_treatment import TrialOutcomeStatus
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import (
    BuildResult,
    LinkTarget,
    SourceReference,
)
from omop_etl.omop.models.rows import ObservationRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class ObservationBuilder(OmopBuilder[ObservationRow]):
    """
    Builds observation rows from patient scalars, the lost-to-followup singleton,
    adverse-event-derived facts (outcome, was_serious, turned_serious_date,
    severity, relatedness, expectedness), and per-treatment-cycle metadata /
    deviation facts. All observation_concept_id domains must not be in Condition,
    Procedure, Drug, Specimen, Measurement, or Device.

    Four patterns:

    1. Unmapped scalar source attributes (evaluable_for_efficacy_analysis,
       has_clinical_benefit_at_week_*, end_of_treatment_reason):
       observation_concept_id=0, source field name in observation_source_value,
       value goes into value_as_concept_id / value_as_string / value_source_value.

    2. Mapped lost_to_followup singleton: observation_concept_id is the topic
       concept, value_as_concept_id is the answer concept.

    3. AE-derived modifier rows (outcome, was_serious, turned_serious, severity,
       relatedness, expectedness): same shape as 1/2, plus FK linkage to the
       AE's condition_occurrence row(s) via observation_event_id +
       obs_event_field_concept_id. Resolved via `_resolve_link_targets` over
       the AE's SourceReference: when an AE produces N condition_occurrence
       rows (multi-concept mapping), each modifier expands to N rows with the
       target row_id in the PK hash for distinctness.

       Emit-anyway policy: each AE modifier method emits one unlinked row when
       no linkage was published (the assessment is meaningful regardless of
       whether the AE produced a condition row).

    4. Treatment-cycle metadata / deviation modifier rows (days-tablet-not-taken,
       administered-to-spec, reasons, ...): same modifier shape, FK-linked to the
       cycle's drug_exposure row(s) (published by DrugExposureBuilder).
       Skip-when-no-link policy (unlike pattern 3): a cycle modifier only means
       something attached to the drug_exposure it modifies, so the row is dropped
       when no drug_exposure was published for the cycle.

    A row is only skipped when the source value or a required date is missing
    (or, for pattern 4, when no FK target was published). When a concept lookup
    misses, the row is still emitted with concept_id=0, and the raw literal is
    stored in value_source_value or observation_source_value.
    """

    table_name: ClassVar[str] = OmopTables.OBSERVATION

    def build(self, ctx: BuildContext) -> BuildResult[ObservationRow]:
        """
        Emit observation rows for the patient. Order: scalar attributes
        (evaluable, clinical_benefit, eot_reason), the lost_to_followup
        singleton, then per-AE rows (outcome, was_serious, turned_serious_date,
        severity, relatedness, expectedness). observation_type_concept_id is
        the ecrf Type Concept; raises if the structural entry is missing.
        """
        patient = ctx.patient
        person_id = ctx.person_id

        ecrf = self.concepts.resolve("ecrf", domains={"Type Concept"})
        if not ecrf:
            raise RuntimeError("Missing ecrf concept in structural mapping")

        observation_type_concept_id = ecrf[0].concept_id

        rows: list[ObservationRow] = []
        rows.extend(self._build_evaluable(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_clinical_benefit(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_end_of_treatment(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_lost_to_followup(patient, person_id, observation_type_concept_id))

        for idx, ae in enumerate(patient.adverse_events):
            rows.extend(self._build_ae_outcome(patient, person_id, observation_type_concept_id, ae, idx, ctx))
            rows.extend(self._build_ae_was_serious(patient, person_id, observation_type_concept_id, ae, idx, ctx))
            rows.extend(self._build_ae_turned_serious(patient, person_id, observation_type_concept_id, ae, ctx))
            rows.extend(self._build_ae_severity(patient, person_id, observation_type_concept_id, ae, idx, ctx))
            rows.extend(self._build_ae_relatedness(patient, person_id, observation_type_concept_id, ae, ctx))
            rows.extend(self._build_ae_expected(patient, person_id, observation_type_concept_id, ae, ctx))

        for cycle in patient.treatment_cycles:
            rows.extend(self._build_treatment_cycle_metadata(patient, person_id, observation_type_concept_id, cycle, ctx))

        return BuildResult(rows=tuple(rows))

    def _ae_link_targets(self, patient: Patient, ae: AdverseEvent, ctx: BuildContext) -> tuple[LinkTarget, ...]:
        """Resolve LinkTargets for the AE's published condition_occurrence
        rows. Returns () when nothing was published, callers decide whether
        to emit an unlinked row or skip."""
        return self._resolve_link_targets(
            ctx,
            target_table=OmopTables.CONDITION_OCCURRENCE,
            source_ref=SourceReference(
                patient.patient_id,
                Patient.Collections.ADVERSE_EVENTS,
                ae.natural_key(),
            ),
        )

    def _yes_no_concept_id(self, value: bool) -> int:
        """
        Resolve True to Yes and False to No via the structural Meas Value
        concepts. Returns 0 when the mapping is missing.
        """
        concept = self.concepts.resolve("yes" if value else "no", domains={OmopDomain.MEAS_VALUE})
        return concept[0].concept_id if concept else 0

    def _bool_observation(
        self,
        *,
        observation_id: int,
        person_id: int,
        field_name: str,
        value: bool,
        date: dt.date,
        observation_type_concept_id: int,
        observation_concept_id: int = 0,
        observation_event_id: int | None = None,
        obs_event_field_concept_id: int | None = None,
    ) -> ObservationRow:
        """
        Compose a boolean observation row. Standardizes the source-value
        encoding for all boolean fields (evaluable, clinical_benefit,
        lost_to_followup, AE was_serious) so the columns can't drift
        between callsites.
        """
        return ObservationRow(
            observation_id=observation_id,
            person_id=person_id,
            observation_concept_id=observation_concept_id,
            observation_date=date,
            observation_type_concept_id=observation_type_concept_id,
            value_as_concept_id=self._yes_no_concept_id(value),
            observation_source_value=field_name,
            observation_source_concept_id=0,
            value_source_value=str(value).casefold(),
            observation_event_id=observation_event_id,
            obs_event_field_concept_id=obs_event_field_concept_id,
        )

    def _build_evaluable(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        Unmapped source attribute: observation_concept_id = 0,
        observation_source_value = field name, value_as_concept_id = Yes/No.
        Dated to treatment_start_date (no clearer event date exists; the
        evaluability decision is informed by treatment activity since start).
        """
        value = patient.evaluable_for_efficacy_analysis
        date = patient.treatment_start_date
        if value is None:
            return []
        if date is None:
            log.warning(
                "Skipping evaluable_for_efficacy_analysis for %s: missing treatment_start_date",
                patient.patient_id,
            )
            return []

        return [
            self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Scalars.EVALUABLE_FOR_EFFICACY_ANALYSIS,
                ),
                person_id=person_id,
                field_name=Patient.Scalars.EVALUABLE_FOR_EFFICACY_ANALYSIS,
                value=value,
                date=date,
                observation_type_concept_id=observation_type_concept_id,
            )
        ]

    def _build_clinical_benefit(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        Clinical benefit at a source-specific timepoint. Read from the
        ClinicalBenefit singleton, date is authoritative (no fallback).
        observation_source_value encodes the week
        (e.g. "has_clinical_benefit_at_week_16") so downstream queries
        can filter by timepoint.
        """
        cb = patient.clinical_benefit
        if cb is None:
            return []
        has_benefit = cb.has_benefit
        date = cb.date
        week = cb.week
        if has_benefit is None:
            return []
        if date is None:
            log.warning(
                "Skipping clinical_benefit for %s: ClinicalBenefit singleton has no date",
                patient.patient_id,
            )
            return []
        if week is None:
            log.warning(
                "Skipping clinical_benefit for %s: ClinicalBenefit singleton has no week",
                patient.patient_id,
            )
            return []

        field_name = f"has_clinical_benefit_at_week_{week}"
        return [
            self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.CLINICAL_BENEFIT,
                    *cb.natural_key(),
                ),
                person_id=person_id,
                field_name=field_name,
                value=has_benefit,
                date=date,
                observation_type_concept_id=observation_type_concept_id,
            )
        ]

    def _build_end_of_treatment(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        EOT reason mappings branch on EndOfTreatment singleton status field.

        - COMPLETED: observation_concept_id = structural
          `trial_completion` to "Completion of clinical trial",
          `value_as_concept_id` is None.
        - WITHDRAWN: observation_concept_id = structural
          `patient_withdrawn` to "Patient withdrawn from trial",
          `value_as_concept_id` = mapped `eot_reason` reason concept
          (or 0 if unmapped).

        Both shapes preserve the raw reason in `value_as_string` +
        `value_source_value` and use the singleton's field name in
        `observation_source_value`. Both topic concepts fall back to 0
        when the structural lookup is missing.

        Skipped when the singleton is None, when status is None or when date is None.
        """
        eot = patient.end_of_treatment
        if eot is None:
            return []

        status = eot.status
        date = eot.date
        reason = eot.reason
        if status is None:
            return []
        if date is None:
            log.warning("Skipping end_of_treatment for %s: singleton has no date", patient.patient_id)
            return []

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.END_OF_TREATMENT,
            *eot.natural_key(),
        )
        value_as_string = reason[:60] if reason else None
        value_source_value = reason[:50] if reason else None

        if status is TrialOutcomeStatus.COMPLETED:
            topic = self.concepts.resolve("trial_completion")
            return [
                ObservationRow(
                    observation_id=row_id,
                    person_id=person_id,
                    observation_concept_id=topic[0].concept_id if topic else 0,
                    observation_date=date,
                    observation_type_concept_id=observation_type_concept_id,
                    value_as_concept_id=None,
                    value_as_string=value_as_string,
                    observation_source_value=Patient.Singletons.END_OF_TREATMENT,
                    observation_source_concept_id=0,
                    value_source_value=value_source_value,
                )
            ]

        # WITHDRAWN status
        topic = self.concepts.resolve("patient_withdrawn")
        withdrawal_reason = self.concepts.resolve("eot_reason", reason) if reason else None
        return [
            ObservationRow(
                observation_id=row_id,
                person_id=person_id,
                observation_concept_id=topic[0].concept_id if topic else 0,
                observation_date=date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=withdrawal_reason[0].concept_id if withdrawal_reason else 0,
                value_as_string=value_as_string,
                observation_source_value=Patient.Singletons.END_OF_TREATMENT,
                observation_source_concept_id=0,
                value_source_value=value_source_value,
            )
        ]

    def _build_lost_to_followup(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        Encoded per clinical-trials CDM guideline as a "Patient withdrawn
        from trial" observation with the withdrawal reason in
        value_as_concept_id:
        - observation_concept_id: structural `patient_withdrawn`: 4087907
          "Patient withdrawn from trial" (or 0 if missing).
        - value_as_concept_id: static `lost_to_followup,True`: 44811247
          "Lost to clinical trial follow-up" (or 0 if missing).
        - observation_date: `date_lost_to_followup`.
        - observation_source_value: field name "lost_to_followup".
        - value_source_value: "true".

        Emit policy: only when lost_to_followup is True. False means the
        patient was NOT lost to follow-up, no withdrawal event to record,
        so no row (absence of row = not withdrawn under closed-world
        semantics).
        """
        followup = patient.lost_to_followup
        if followup is None:
            return []

        value = followup.lost_to_followup
        if not value:
            # None or False: no withdrawal event to record
            return []

        date = followup.date_lost_to_followup
        if date is None:
            log.warning("Skipping lost_to_followup for %s: missing date_lost_to_followup", patient.patient_id)
            return []

        topic = self.concepts.resolve("patient_withdrawn")
        reason = self.concepts.resolve(FollowUp.Fields.LOST_TO_FOLLOWUP, str(value))

        return [
            ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.LOST_TO_FOLLOWUP,
                    *followup.natural_key(),
                ),
                person_id=person_id,
                observation_concept_id=topic[0].concept_id if topic else 0,
                observation_date=date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=reason[0].concept_id if reason else 0,
                observation_source_value=Patient.Singletons.LOST_TO_FOLLOWUP,
                observation_source_concept_id=0,
                value_source_value=str(value).lower(),
            )
        ]

    def _build_ae_outcome(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        Topic concept is the structural lookup for adverse_event_outcome,
        answer concept is the static lookup for adverse_event_outcome values.
        Both lookups fall back to 0 when missing and the row is still emitted as
        long as outcome and start_date are present, with the raw value
        preserved in value_source_value. Linked back to the AE's
        condition_occurrence row(s): multi-concept AE expands to one row per
        target. Emit-anyway: when no linkage, one unlinked row.
        """
        raw_outcome = ae.outcome
        date = ae.start_date
        if raw_outcome is None:
            return []
        if date is None:
            log.warning("Skipping AE %d outcome for %s: missing start_date", index, patient.patient_id)
            return []

        topic_concept = self.concepts.resolve("adverse_event_outcome")
        outcome_concept = self.concepts.resolve("adverse_event_outcome", raw_outcome)

        def row(
            event_id: int | None,
            field_concept_id: int | None,
            *,
            _date: dt.date = date,
        ) -> ObservationRow:
            return ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.OUTCOME,
                    event_id,
                ),
                person_id=person_id,
                observation_concept_id=topic_concept[0].concept_id if topic_concept else 0,
                observation_date=_date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=outcome_concept[0].concept_id if outcome_concept else 0,
                observation_source_value=AdverseEvent.Fields.OUTCOME,
                observation_source_concept_id=0,
                value_source_value=str(raw_outcome)[:50],
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )

        targets = self._ae_link_targets(patient, ae, ctx)
        if not targets:
            return [row(event_id=None, field_concept_id=None)]
        return [row(t.event_id, t.field_concept_id) for t in targets]

    def _build_ae_was_serious(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        Unmapped source attribute and AE FK: observation_concept_id = 0,
        observation_source_value = "was_serious", value_as_concept_id = Yes/No,
        observation_event_id and obs_event_field_concept_id point at the
        AE's condition_occurrence row(s). Emits for both True and False so the
        explicit assessment is preserved. Dated to AE.start_date. Multi-concept
        AE expands to one row per target. Emit-anyway when no linkage.
        """
        was_serious = ae.was_serious
        if was_serious is None:
            return []
        date = ae.start_date
        if date is None:
            log.warning("Skipping AE %d was_serious for %s: missing start_date", index, patient.patient_id)
            return []

        def row(
            event_id: int | None,
            field_concept_id: int | None,
            *,
            _date: dt.date = date,
            _was_serious: bool = was_serious,
        ) -> ObservationRow:
            return self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.WAS_SERIOUS,
                    event_id,
                ),
                person_id=person_id,
                field_name=AdverseEvent.Fields.WAS_SERIOUS,
                value=_was_serious,
                date=_date,
                observation_type_concept_id=observation_type_concept_id,
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )

        targets = self._ae_link_targets(patient, ae, ctx)
        if not targets:
            return [row(event_id=None, field_concept_id=None)]
        return [row(t.event_id, t.field_concept_id) for t in targets]

    def _build_ae_turned_serious(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        AE turned-serious flag. Encoded as a Yes observation on
        turned_serious_date, value_source_value carries the ISO date so
        consumers can reconstruct the event without re-querying.
        Not using _bool_observation because value_source_value differs
        (date string, not "true"). Multi-concept AE expands to one row per
        target. Emit-anyway when no linkage.
        """
        date = ae.turned_serious_date
        if date is None:
            return []

        def row(
            event_id: int | None,
            field_concept_id: int | None,
            *,
            _date: dt.date = date,
        ) -> ObservationRow:
            return ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.TURNED_SERIOUS_DATE,
                    event_id,
                ),
                person_id=person_id,
                observation_concept_id=0,
                observation_date=_date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=self._yes_no_concept_id(True),
                observation_source_value=AdverseEvent.Fields.TURNED_SERIOUS_DATE,
                observation_source_concept_id=0,
                value_source_value=_date.isoformat(),
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )

        targets = self._ae_link_targets(patient, ae, ctx)
        if not targets:
            return [row(event_id=None, field_concept_id=None)]
        return [row(t.event_id, t.field_concept_id) for t in targets]

    def _build_ae_severity(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        CTCAE severity modifier observation. The grade-N concepts in the
        `adverse_event_code` static lookup are precoordinated severity
        concepts (CTCAE grade 1 = Mild, ..., 5 = Death). They encode
        both topic and severity level, so use directly as
        `observation_concept_id`; `value_as_concept_id` stays None.

        FK-linked to the AE's condition_occurrence row(s): multi-concept AE
        expands to one row per target. Emit-anyway when no linkage.

        Skipped when grade or start_date is None.
        """
        grade = ae.grade
        if grade is None:
            return []
        date = ae.start_date
        if date is None:
            log.warning("Skipping AE %d severity for %s: missing start_date", index, patient.patient_id)
            return []

        topic = self.concepts.resolve("adverse_event_code", str(grade))

        def row(
            event_id: int | None,
            field_concept_id: int | None,
            *,
            _date: dt.date = date,
        ) -> ObservationRow:
            return ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.GRADE,
                    event_id,
                ),
                person_id=person_id,
                observation_concept_id=topic[0].concept_id if topic else 0,
                observation_date=_date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=None,
                observation_source_value="severity",
                observation_source_concept_id=0,
                value_source_value=str(grade),
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )

        targets = self._ae_link_targets(patient, ae, ctx)
        if not targets:
            return [row(event_id=None, field_concept_id=None)]
        return [row(t.event_id, t.field_concept_id) for t in targets]

    def _build_ae_relatedness(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        Per-treatment relatedness modifier observation. Emits 0-2 rows per AE
        per target condition_occurrence row, one per non-None
        `related_to_treatment_<N>_status`.

        observation_concept_id = 0 (no Standard topic concept for
        drug-causality assessment in current vocab). Field name
        ("related_to_treatment_1" / "_2") in `observation_source_value`,
        mapped relatedness concept (Related / Not related / Unknown)
        in `value_as_concept_id`, enum value in `value_source_value`,
        and the corresponding `treatment_<N>_name` drug string in
        `qualifier_source_value` for context.

        FK-linked to the AE's condition_occurrence row(s); cross-product of
        per-treatment facts × link targets. Emit-anyway: when no linkage,
        emit unlinked rows (one per non-None per-treatment fact). Whole
        method skipped when start_date is None; per-treatment row skipped
        when status is None.
        """
        date = ae.start_date
        if date is None:
            return []

        per_treatment = (
            (1, ae.related_to_treatment_1_status, ae.treatment_1_name),
            (2, ae.related_to_treatment_2_status, ae.treatment_2_name),
        )
        targets = self._ae_link_targets(patient, ae, ctx)
        rows: list[ObservationRow] = []

        for treatment_num, status, treatment_name in per_treatment:
            if status is None:
                continue
            status_value = status.value
            field_name = f"related_to_treatment_{treatment_num}"
            concept = self.concepts.resolve("relatedness", status_value)

            def row(
                event_id: int | None,
                field_concept_id: int | None,
                *,
                _date: dt.date = date,
            ) -> ObservationRow:
                return ObservationRow(
                    observation_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.ADVERSE_EVENTS,
                        *ae.natural_key(),
                        field_name,
                        event_id,
                    ),
                    person_id=person_id,
                    observation_concept_id=0,
                    observation_date=_date,
                    observation_type_concept_id=observation_type_concept_id,
                    value_as_concept_id=concept[0].concept_id if concept else 0,
                    observation_source_value=field_name,
                    observation_source_concept_id=0,
                    value_source_value=status_value,
                    qualifier_source_value=treatment_name[:50] if treatment_name else None,
                    observation_event_id=event_id,
                    obs_event_field_concept_id=field_concept_id,
                )

            if not targets:
                rows.append(row(event_id=None, field_concept_id=None))
            else:
                rows.extend(row(t.event_id, t.field_concept_id) for t in targets)
        return rows

    def _build_ae_expected(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        Per-treatment expectedness modifier observation. Emits 0-2 rows per
        AE per target condition_occurrence row, one per non-None
        `was_serious_grade_expected_treatment_<N>`. Uses the LOINC Expected
        (1472143) / Not Expected (1472269) Meas Value concepts via
        `expectedness` static lookup.

        observation_concept_id = 0. Field name in `observation_source_value`,
        mapped Expected/Not-Expected concept in `value_as_concept_id`,
        boolean literal in `value_source_value`, treatment drug name in
        `qualifier_source_value`. FK-linked to the AE's condition_occurrence
        row(s); cross-product of per-treatment facts × link targets.
        Emit-anyway when no linkage.
        """
        date = ae.start_date
        if date is None:
            return []

        per_treatment = (
            (1, ae.was_serious_grade_expected_treatment_1, ae.treatment_1_name),
            (2, ae.was_serious_grade_expected_treatment_2, ae.treatment_2_name),
        )
        targets = self._ae_link_targets(patient, ae, ctx)
        rows: list[ObservationRow] = []

        for treatment_num, expected, treatment_name in per_treatment:
            if expected is None:
                continue
            field_name = f"was_serious_grade_expected_treatment_{treatment_num}"
            value_literal = str(expected).casefold()
            concept = self.concepts.resolve("expectedness", value_literal)

            def row(
                event_id: int | None,
                field_concept_id: int | None,
                *,
                _date: dt.date = date,
            ) -> ObservationRow:
                return ObservationRow(
                    observation_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.ADVERSE_EVENTS,
                        *ae.natural_key(),
                        field_name,
                        event_id,
                    ),
                    person_id=person_id,
                    observation_concept_id=0,
                    observation_date=_date,
                    observation_type_concept_id=observation_type_concept_id,
                    value_as_concept_id=concept[0].concept_id if concept else 0,
                    observation_source_value=field_name,
                    observation_source_concept_id=0,
                    value_source_value=value_literal,
                    qualifier_source_value=treatment_name[:50] if treatment_name else None,
                    observation_event_id=event_id,
                    obs_event_field_concept_id=field_concept_id,
                )

            if not targets:
                rows.append(row(event_id=None, field_concept_id=None))
            else:
                rows.extend(row(t.event_id, t.field_concept_id) for t in targets)
        return rows

    _CYCLE_METADATA_FIELDS: ClassVar[tuple[tuple[str, Any], ...]] = (
        (TreatmentCycleComponent.Fields.RECEIVED_TREATMENT_THIS_CYCLE, bool),
        (TreatmentCycleComponent.Fields.WAS_TOTAL_DOSE_DELIVERED, bool),
        (TreatmentCycleComponent.Fields.WAS_DOSE_ADMINISTERED_TO_SPEC, bool),
        (TreatmentCycleComponent.Fields.REASON_NOT_ADMINISTERED_TO_SPEC, str),
        (TreatmentCycleComponent.Fields.NUMBER_OF_DAYS_TABLET_NOT_TAKEN, int),
        (TreatmentCycleComponent.Fields.REASON_TABLET_NOT_TAKEN, str),
        (TreatmentCycleComponent.Fields.WAS_TABLET_TAKEN_TO_PRESCRIPTION_IN_PREVIOUS_CYCLE, bool),
    )

    def _build_treatment_cycle_metadata(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        cycle: TreatmentCycleComponent,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        Per-cycle metadata and deviation modifier observations, one row per non-None
        field in `_CYCLE_METADATA_FIELDS` per linked drug_exposure row.

        observation_concept_id = 0 (no Standard topic concept for trial drug
        adherence / deviation in current vocab). Field name in
        observation_source_value; the value lands in value_as_concept_id (Yes/No
        for bools), value_as_number (ints), or value_as_string (free-text
        reasons), with the raw literal in value_source_value. Dated to
        cycle.start_date.

        Skip-when-no-link: a cycle modifier only means something attached to the
        drug_exposure it modifies, so the whole cycle's metadata is skipped when
        DrugExposureBuilder published no row for it (e.g. a cycle with no drug
        name). A cycle mapped to N Drug concepts links each metadata row to all N.
        """
        date = cycle.start_date
        if date is None:
            return []

        targets = self._resolve_link_targets(
            ctx,
            target_table=OmopTables.DRUG_EXPOSURE,
            source_ref=SourceReference(
                patient.patient_id,
                Patient.Collections.TREATMENT_CYCLES,
                cycle.natural_key(),
            ),
        )
        if not targets:
            return []

        rows: list[ObservationRow] = []
        for field_name, kind in self._CYCLE_METADATA_FIELDS:
            value = getattr(cycle, field_name)
            if value is None:
                continue

            value_as_number: float | None = None
            value_as_string: str | None = None
            value_as_concept_id: int | None = None
            if kind is bool:
                value_as_concept_id = self._yes_no_concept_id(value)
                value_source_value = str(value).lower()
            elif kind is int:
                value_as_number = float(value)
                value_source_value = str(value)
            else:
                value_as_string = value[:60]
                value_source_value = value[:50]

            def row(
                event_id: int,
                field_concept_id: int,
                *,
                _date: dt.date = date,
                _field: str = field_name,
                _van: float | None = value_as_number,
                _vas: str | None = value_as_string,
                _vac: int | None = value_as_concept_id,
                _vsv: str = value_source_value,
            ) -> ObservationRow:
                return ObservationRow(
                    observation_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.TREATMENT_CYCLES,
                        *cycle.natural_key(),
                        _field,
                        event_id,
                    ),
                    person_id=person_id,
                    observation_concept_id=0,
                    observation_date=_date,
                    observation_type_concept_id=observation_type_concept_id,
                    value_as_number=_van,
                    value_as_string=_vas,
                    value_as_concept_id=_vac,
                    observation_source_value=_field,
                    observation_source_concept_id=0,
                    value_source_value=_vsv,
                    observation_event_id=event_id,
                    obs_event_field_concept_id=field_concept_id,
                )

            rows.extend(row(t.event_id, t.field_concept_id) for t in targets)
        return rows
