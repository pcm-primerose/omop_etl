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


class ObservationBuilder(OmopBuilder[ObservationRow]):
    """
    Builds observation rows from patient scalars, the lost-to-followup singleton,
    and adverse-event-derived facts (outcome, was_serious, turned_serious_date).
    All observation_concept_id domains must not be in Condition, Procedure, Drug,
    Specimen, Measurement, or Device.


    There are three patterns used:

    1. For evaluable_for_efficacy_analysis, has_clinical_benfit_at_week_*
    and end_of_treatment_reason there is no observation_concept_id,
    it's set to 0. The source field name is tracked in observation_source_value,
    and value_as_concept_id, value_as_string and value_source_value has
    the raw and normalized source values.

    2. For lost_to_followup the observation_concept_id is mapped,
    observation_source_value has the field name, and value_as_concept_id has
    the result (answer).

    3. For AE-derived fields, AE outcome, AE was_serious and AE turned_serious_date,
    the same occurs as the first two patterns, but they are linked back to the
    source AE record from ConditionOccurrenceBuilder,
    using FKs stored in observation_event_id and obs_event_field_concept_id,
    produced by BuildContext.condition_id_by_ae_sequence_id.

    A row is only skipped when the source value or a required date is missing.
    When a concept lookup misses, the row is still emitted with concept_id=0,
    and the raw literal is stored in value_source_value or observation_source_value.
    """

    table_name: ClassVar[str] = "observation"

    def build(self, ctx: BuildContext) -> list[ObservationRow]:
        """
        Emit observation rows for the patient. Order: scalar attributes
        (evaluable, clinical_benefit, eot_reason), the lost_to_followup
        singleton, then per-AE rows (outcome, was_serious, turned_serious_date).
        observation_type_concept_id is the ecrf Type Concept, raises if the
        structural entry is missing.
        """
        patient = ctx.patient
        person_id = ctx.person_id

        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        if ecrf is None:
            raise RuntimeError("Missing ecrf concept in structural mapping")

        observation_type_concept_id = ecrf.concept_id

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
        Resolve True to Yes and False to No via the structural Meas Value
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

    def _build_eot_reason(
        self,
        patient: Patient,
        person_id: int,
        observation_type_concept_id: int,
    ) -> list[ObservationRow]:
        """
        Unmapped source attribute: observation_concept_id = 0,
        observation_source_value = field name, value_as_concept_id = mapped
        reason (or 0 if unmapped), value_as_string and value_source_value
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
        """
        observation_concept_id is the "Lost to follow-up" concept from the
        lost_to_followup static value set, falls back to 0 when the mapping is missing.
        value_as_concept_id is the Yes/No concept, observation_source_value is the field name and
        value_source_value carries the boolean literal. Date is date_lost_to_followup.
        """
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
        Topic concept is the structural lookup for adverse_event_outcome,
        answer concept is the static lookup for adverse_event_outcome values.
        Both lookups fall back to 0 when missing and the row is still emitted as
        long as outcome and start_date are present, with the raw value
        preserved in value_source_value, linked to Condition AE record.
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
        """
        Unmapped source attribute and AE FK: observation_concept_id = 0,
        observation_source_value = "was_serious", value_as_concept_id = Yes/No concept,
        observation_event_id and obs_event_field_concept_id point at the
        AE's condition_occurrence row. Emits for both True and False so the
        explicit assessment is preserved. Dated is AE.start_date.
        """
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
        turned_serious_date, value_source_value carries the ISO date so
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
        is missing, this is required for AE-attributed observations.
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
