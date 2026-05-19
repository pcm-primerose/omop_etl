import datetime as dt
from logging import getLogger
from typing import ClassVar

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import BuildContext, OmopBuilder
from omop_etl.omop.models.rows import ObservationRow
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)

_WEEK_16 = dt.timedelta(weeks=16)


class ObservationBuilder(OmopBuilder[ObservationRow]):
    """
    Builds observation rows from patient scalars, the lost-to-followup singleton,
    and adverse-event-derived facts (outcome, was_serious, turned_serious_date).
    All observation_concept_id domains must NOT be Condition, Procedure, Drug,
    Specimen, Measurement, or Device.

    Three row shapes:

    1. Unmapped source attribute (no topic concept available for the field):
       observation_concept_id = 0, observation_source_value = source field name,
       value_as_concept_id / value_as_string / value_source_value carry the
       normalized + raw source value. Used for evaluable_for_efficacy_analysis,
       has_clinical_benefit_at_week_16, end_of_treatment_reason.

    2. Mapped observation topic (a Standard concept names the topic):
       observation_concept_id = topic concept, observation_source_value =
       source field name, value_as_concept_id carries the answer/result.
       Used for lost_to_followup ("Lost to follow-up" topic).

    3. AE-derived (shapes 1 or 2, plus FK linkage):
       observation_event_id + obs_event_field_concept_id link back to the
       condition_occurrence row produced by ConditionOccurrenceBuilder via
       BuildContext.condition_id_by_ae_sequence_id. Used for AE outcome,
       AE was_serious, AE turned_serious_date.

    Emit policy: a row is only skipped when the source value or a required
    date is missing. When a concept lookup misses (topic OR value), the row
    is still emitted with concept_id=0 — CDM convention for "result present
    in source but unmapped" — and the raw literal is preserved in
    value_source_value / observation_source_value so the fact stays
    queryable. Yes/No is resolved via the `yes` / `no` structural Meas
    Value concepts.
    """

    table_name: ClassVar[str] = "observation"

    def build(self, ctx: BuildContext) -> list[ObservationRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        observation_type_concept_id = ecrf.concept_id if ecrf else 0

        rows: list[ObservationRow] = []
        rows.extend(self._build_evaluable(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_clinical_benefit(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_eot_reason(patient, person_id, observation_type_concept_id))
        rows.extend(self._build_lost_to_followup(patient, person_id, observation_type_concept_id))

        for idx, ae in enumerate(patient.adverse_events):
            rows.extend(self._build_ae_outcome(patient, person_id, observation_type_concept_id, ae, idx, ctx))
            rows.extend(self._build_ae_was_serious(patient, person_id, observation_type_concept_id, ae, idx, ctx))
            rows.extend(self._build_ae_turned_serious(patient, person_id, observation_type_concept_id, ae, idx, ctx))

        return rows

    def _yes_no_concept_id(self, value: bool) -> int:
        """
        Resolve True to Yes / False to No via the structural Meas Value
        concepts. Returns 0 when the mapping is missing.
        """
        concept = self.concepts.lookup_structural("yes" if value else "no", domains={OmopDomain.MEAS_VALUE})
        return concept.concept_id if concept else 0

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
        Compose a boolean observation row. Standardizes the source/value
        encoding for all boolean fields (evaluable, clinical_benefit,
        lost_to_followup, AE was_serious) so the columns can't drift
        between sites.
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
            value_source_value=str(value).lower(),
            observation_event_id=observation_event_id,
            obs_event_field_concept_id=obs_event_field_concept_id,
        )

    def _build_evaluable(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
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
        Clinical benefit at W16. Uses the dedicated
        `clinical_benefit_at_week_16_date` scalar if set, otherwise falls back
        to `treatment_start_date + 16 weeks`.
        todo: Switch to a ClinicalBenefit singleton when extending to other timepoints.
        """
        value = patient.has_clinical_benefit_at_week_16
        if value is None:
            return []

        date = patient.clinical_benefit_at_week_16_date
        if date is None:
            start = patient.treatment_start_date
            if start is None:
                log.warning(
                    "Skipping has_clinical_benefit_at_week_16 for %s: no clinical_benefit_at_week_16_date and no treatment_start_date",
                    patient.patient_id,
                )
                return []
            date = start + _WEEK_16

        return [
            self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Scalars.HAS_CLINICAL_BENEFIT_AT_WEEK_16,
                ),
                person_id=person_id,
                field_name=Patient.Scalars.HAS_CLINICAL_BENEFIT_AT_WEEK_16,
                value=value,
                date=date,
                observation_type_concept_id=observation_type_concept_id,
            )
        ]

    def _build_eot_reason(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        Shape 1 (unmapped source attribute): observation_concept_id = 0,
        observation_source_value = field name, value_as_concept_id = mapped
        reason (or 0 if unmapped), value_as_string + value_source_value
        preserve the raw reason text.
        """
        reason = patient.end_of_treatment_reason
        date = patient.end_of_treatment_date
        if reason is None:
            return []
        if date is None:
            log.warning("Skipping end_of_treatment_reason for %s: missing end_of_treatment_date", patient.patient_id)
            return []

        concept = self.concepts.lookup_static("eot_reason", reason)

        return [
            ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Scalars.END_OF_TREATMENT_REASON,
                ),
                person_id=person_id,
                observation_concept_id=0,
                observation_date=date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=concept.concept_id if concept else 0,
                value_as_string=reason[:60],
                observation_source_value=Patient.Scalars.END_OF_TREATMENT_REASON,
                observation_source_concept_id=0,
                value_source_value=reason[:50],
            )
        ]

    def _build_lost_to_followup(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        followup = patient.lost_to_followup
        if followup is None:
            return []

        value = followup.lost_to_followup
        date = followup.date_lost_to_followup
        if value is None:
            return []
        if date is None:
            log.warning("Skipping lost_to_followup for %s: missing date_lost_to_followup", patient.patient_id)
            return []

        concept = self.concepts.lookup_static(FollowUp.Fields.LOST_TO_FOLLOWUP, str(value))
        observation_concept_id = concept.concept_id if concept else 0

        return [
            self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.LOST_TO_FOLLOWUP,
                    *followup.natural_key(),
                ),
                person_id=person_id,
                field_name=Patient.Singletons.LOST_TO_FOLLOWUP,
                value=value,
                date=date,
                observation_type_concept_id=observation_type_concept_id,
                observation_concept_id=observation_concept_id,
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
        Shape 2 or 3: topic concept (structural `adverse_event_outcome`) +
        answer concept (static `adverse_event_outcome,<text>`). Both
        lookups fall back to 0 when missing — the row is still emitted as
        long as outcome and start_date are present, with the raw value
        preserved in value_source_value.
        """
        raw_outcome = ae.outcome
        date = ae.start_date
        if raw_outcome is None:
            return []
        if date is None:
            log.warning("Skipping AE %d outcome for %s: missing start_date", index, patient.patient_id)
            return []

        topic_concept = self.concepts.lookup_structural("adverse_event_outcome")
        outcome_concept = self.concepts.lookup_static("adverse_event_outcome", raw_outcome)

        event_id, field_concept_id = self._ae_fk(ae, patient, index, ctx)

        return [
            ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.OUTCOME,
                ),
                person_id=person_id,
                observation_concept_id=topic_concept.concept_id if topic_concept else 0,
                observation_date=date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=outcome_concept.concept_id if outcome_concept else 0,
                observation_source_value=AdverseEvent.Fields.OUTCOME,
                observation_source_concept_id=0,
                value_source_value=str(raw_outcome)[:50],
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )
        ]

    def _build_ae_was_serious(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        was_serious = ae.was_serious
        if was_serious is None:
            return []
        date = ae.start_date
        if date is None:
            log.warning("Skipping AE %d was_serious for %s: missing start_date", index, patient.patient_id)
            return []

        event_id, field_concept_id = self._ae_fk(ae, patient, index, ctx)

        return [
            self._bool_observation(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.WAS_SERIOUS,
                ),
                person_id=person_id,
                field_name=AdverseEvent.Fields.WAS_SERIOUS,
                value=was_serious,
                date=date,
                observation_type_concept_id=observation_type_concept_id,
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )
        ]

    def _build_ae_turned_serious(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[ObservationRow]:
        """
        AE turned-serious flag. Encoded as a Yes observation on
        `turned_serious_date`; value_source_value carries the ISO date so
        consumers can reconstruct the event without re-querying.
        Not using _bool_observation because value_source_value differs
        (date string, not "true").
        """
        date = ae.turned_serious_date
        if date is None:
            return []

        event_id, field_concept_id = self._ae_fk(ae, patient, index, ctx)

        return [
            ObservationRow(
                observation_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    AdverseEvent.Fields.TURNED_SERIOUS_DATE,
                ),
                person_id=person_id,
                observation_concept_id=0,
                observation_date=date,
                observation_type_concept_id=observation_type_concept_id,
                value_as_concept_id=self._yes_no_concept_id(True),
                observation_source_value=AdverseEvent.Fields.TURNED_SERIOUS_DATE,
                observation_source_concept_id=0,
                value_source_value=date.isoformat(),
                observation_event_id=event_id,
                obs_event_field_concept_id=field_concept_id,
            )
        ]

    def _ae_fk(
        self,
        ae: AdverseEvent,
        patient: Patient,
        index: int,
        ctx: BuildContext,
    ) -> tuple[int | None, int | None]:
        """
        Resolve (observation_event_id, obs_event_field_concept_id) for an
        AE-derived observation row. Returns (None, None) when the AE has no
        sequence_id or no published condition_occurrence row. Raises if the
        `cdm_field` static entry for condition_occurrence.condition_occurrence_id
        is missing — this is required infrastructure for AE-attributed
        observations.
        """
        sequence_id = ae.sequence_id
        if sequence_id is None:
            log.warning(
                "AE %d for %s missing sequence_id: cannot link observation to condition_occurrence",
                index,
                patient.patient_id,
            )
            return None, None

        event_id = ctx.condition_id_by_ae_sequence_id.get(sequence_id)
        if event_id is None:
            log.warning(
                "AE %d for %s missing event_id: cannot link observation to condition_occurrence",
                index,
                patient.patient_id,
            )
            return None, None

        field_concept = self.concepts.lookup_static(
            "cdm_field",
            "condition_occurrence.condition_occurrence_id",
            domains={"Metadata"},
        )
        if field_concept is None:
            raise RuntimeError("Missing cdm_field mapping for condition_occurrence.condition_occurrence_id")

        return event_id, field_concept.concept_id
