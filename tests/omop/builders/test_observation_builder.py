import datetime as dt
import logging
import pytest

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent, RelatedStatus
from omop_etl.harmonization.models.domain.clinical_benefit import ClinicalBenefit
from omop_etl.harmonization.models.domain.end_of_treatment import EndOfTreatment, TrialOutcomeStatus
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.observation import ObservationBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import BuildResult
from tests.omop.conftest import (
    _static,
    _structural,
    create_build_context,
    create_patient,
    publish_ae_condition,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)

YES_CID = 4188539
NO_CID = 4188540
CDM_FIELD_CID = 1147127
AE_OUTCOME_TOPIC_CID = 4231813


def _with_yes_no(structural_index: dict) -> dict:
    """Yes/No structural Meas Value concepts (OHDSI ETL convention for booleans)."""
    structural_index["yes"] = _structural("yes", YES_CID, "meas value")
    structural_index["no"] = _structural("no", NO_CID, "meas value")
    return structural_index


def _with_ae_outcome_topic(structural_index: dict) -> dict:
    structural_index["adverse_event_outcome"] = _structural("adverse_event_outcome", AE_OUTCOME_TOPIC_CID, "observation")
    return structural_index


class TestObservationBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert ObservationBuilder(concepts).table_name == "observation"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


class TestEvaluableForEfficacy:
    """
    Pattern 1 (unmapped source attribute):
    concept_id=0, source_value=field name, value_source_value=lowercase literal.
    """

    def test_true_emits_row_with_yes_value(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            evaluable_for_efficacy_analysis=True,
            treatment_start_date=dt.date(2023, 1, 10),
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 1, 10)
        assert row.observation_type_concept_id == 32809
        assert row.observation_source_value == "evaluable_for_efficacy_analysis"
        assert row.observation_source_concept_id == 0
        assert row.value_as_concept_id == YES_CID
        assert row.value_source_value == "true"

    def test_false_emits_row_with_no_value(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            evaluable_for_efficacy_analysis=False,
            treatment_start_date=dt.date(2023, 1, 10),
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == NO_CID
        assert result.rows[0].value_source_value == "false"

    def test_yes_no_missing_falls_back_to_zero(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            evaluable_for_efficacy_analysis=True,
            treatment_start_date=dt.date(2023, 1, 10),
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == 0

    def test_skipped_when_value_is_none(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 10))

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_skipped_when_treatment_start_date_missing(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, evaluable_for_efficacy_analysis=True)

        with caplog.at_level(logging.WARNING):
            result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
        assert any("treatment_start_date" in rec.message for rec in caplog.records)


class TestClinicalBenefit:
    """
    Pattern 1 (no topic concept): concept_id=0, observation_source_value
    encodes the week as `has_clinical_benefit_at_week_<N>`, value_as_concept_id
    is Yes/No concepts, observation_date is the singleton's date.
    """

    @staticmethod
    def _make_singleton(
        *,
        has_benefit: bool | None,
        week: int | None = 16,
        date: dt.date | None = dt.date(2023, 4, 20),
    ) -> ClinicalBenefit:
        cb = ClinicalBenefit(patient_id=PID)
        cb.week = week
        cb.has_benefit = has_benefit
        cb.date = date
        return cb

    def test_emits_row_with_yes_for_true(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(has_benefit=True)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 4, 20)
        assert row.observation_source_value == "has_clinical_benefit_at_week_16"
        assert row.observation_source_concept_id == 0
        assert row.value_as_concept_id == YES_CID
        assert row.value_source_value == "true"

    def test_emits_row_with_no_for_false(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(has_benefit=False)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == NO_CID
        assert result.rows[0].value_source_value == "false"

    def test_source_value_encodes_week_for_other_timepoints(self, static_index, structural_index):
        """Week 24 from another source produces: `has_clinical_benefit_at_week_24`."""
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(
            has_benefit=True,
            week=24,
            date=dt.date(2023, 6, 1),
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_source_value == "has_clinical_benefit_at_week_24"
        assert result.rows[0].observation_date == dt.date(2023, 6, 1)

    def test_singleton_absent_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_skipped_when_has_benefit_is_none(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(has_benefit=None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_skipped_when_date_is_none(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(has_benefit=True, date=None)

        with caplog.at_level(logging.WARNING):
            result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
        assert any("no date" in rec.message for rec in caplog.records)

    def test_skipped_when_week_is_none(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.clinical_benefit = self._make_singleton(has_benefit=True, week=None)

        with caplog.at_level(logging.WARNING):
            result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
        assert any("no week" in rec.message for rec in caplog.records)


TRIAL_COMPLETION_CID = 40482840


class TestEndOfTreatment:
    """
    Reads the EndOfTreatment singleton and branches on status:
    - COMPLETED: topic = trial_completion (40482840), no value_as_concept_id.
    - WITHDRAWN: topic = patient_withdrawn (4087907),
      value_as_concept_id = mapped eot_reason concept (or 0).
    """

    @staticmethod
    def _make_eot(
        *,
        status: TrialOutcomeStatus | None,
        reason: str | None = None,
        date: dt.date | None = dt.date(2023, 8, 1),
    ) -> EndOfTreatment:
        eot = EndOfTreatment(patient_id=PID)
        eot.status = status
        eot.reason = reason
        eot.date = date
        return eot

    @staticmethod
    def _with_trial_completion(structural_index: dict) -> dict:
        structural_index["trial_completion"] = _structural("trial_completion", TRIAL_COMPLETION_CID, "observation")
        return structural_index

    def test_completed_status_emits_completion_shape(self, static_index, structural_index):
        self._with_trial_completion(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.COMPLETED,
            reason="Normal completion according to cohort-specific manual",
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == TRIAL_COMPLETION_CID
        assert row.observation_date == dt.date(2023, 8, 1)
        assert row.value_as_concept_id is None
        assert row.observation_source_value == "end_of_treatment"
        assert row.observation_source_concept_id == 0
        assert row.value_as_string == "Normal completion according to cohort-specific manual"
        assert row.value_source_value == "Normal completion according to cohort-specific man"  # truncated to 50

    def test_completion_topic_missing_falls_back_to_zero(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.COMPLETED,
            reason="Normal completion",
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_concept_id == 0
        assert result.rows[0].value_as_concept_id is None

    def test_withdrawn_with_mapped_reason_emits_withdrawal_shape(self, static_index, structural_index):
        static_index[("eot_reason", "disease progression")] = _static("eot_reason", "disease progression", 1617595, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.WITHDRAWN,
            reason="Disease progression",
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == PATIENT_WITHDRAWN_CID
        assert row.observation_date == dt.date(2023, 8, 1)
        assert row.value_as_concept_id == 1617595
        assert row.observation_source_value == "end_of_treatment"
        assert row.value_as_string == "Disease progression"
        assert row.value_source_value == "Disease progression"

    def test_withdrawn_with_unmapped_reason_emits_value_zero(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.WITHDRAWN,
            reason="Some unmapped reason",
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_concept_id == PATIENT_WITHDRAWN_CID
        assert result.rows[0].value_as_concept_id == 0
        assert result.rows[0].value_as_string == "Some unmapped reason"

    def test_withdrawal_topic_missing_falls_back_to_zero(self, static_index, structural_index):
        # remove patient_withdrawn from the fixture
        structural_index.pop("patient_withdrawn", None)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.WITHDRAWN,
            reason="Other",
        )

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_concept_id == 0
        assert result.rows[0].value_as_concept_id == 0

    def test_singleton_absent_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_status_none_returns_empty(self, static_index, structural_index):
        """Singleton with date but no status (e.g. date inferred from treatment cycles only): no row."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(status=None, reason=None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_date_skips(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.end_of_treatment = self._make_eot(
            status=TrialOutcomeStatus.WITHDRAWN,
            reason="Other",
            date=None,
        )

        with caplog.at_level(logging.WARNING):
            result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
        assert any("no date" in rec.message for rec in caplog.records)


PATIENT_WITHDRAWN_CID = 4087907
LOST_TO_FU_REASON_CID = 44811247


class TestLostToFollowup:
    """
    Clinical-trials CDM shape (Topic 1 / item 5 of the delta):
    observation_concept_id is `patient_withdrawn`: "Patient withdrawn
    from trial" (4087907), value_as_concept_id is the withdrawal reason
    `lost_to_followup,True`: "Lost to clinical trial follow-up"
    (44811247). Only emitted when lost_to_followup is True.
    """

    @staticmethod
    def _make_followup(value: bool | None, date: dt.date | None = dt.date(2023, 12, 1)) -> FollowUp:
        followup = FollowUp(patient_id=PID)
        followup.lost_to_followup = value
        followup.date_lost_to_followup = date
        return followup

    def test_lost_to_followup_true_emits_withdrawal_row(self, static_index, structural_index):
        static_index[("lost_to_followup", "true")] = _static("lost_to_followup", "true", LOST_TO_FU_REASON_CID, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.lost_to_followup = self._make_followup(True)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == PATIENT_WITHDRAWN_CID
        assert row.observation_date == dt.date(2023, 12, 1)
        assert row.value_as_concept_id == LOST_TO_FU_REASON_CID
        assert row.observation_source_value == "lost_to_followup"
        assert row.observation_source_concept_id == 0
        assert row.value_source_value == "true"

    def test_topic_concept_missing_falls_back_to_zero(self, static_index, structural_index):
        """No patient_withdrawn structural: concept_id=0, row still emits with reason."""
        # remove the patient_withdrawn structural concept from the fixture
        structural_index.pop("patient_withdrawn", None)
        static_index[("lost_to_followup", "true")] = _static("lost_to_followup", "true", LOST_TO_FU_REASON_CID, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.lost_to_followup = self._make_followup(True)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_concept_id == 0
        assert result.rows[0].value_as_concept_id == LOST_TO_FU_REASON_CID

    def test_reason_concept_missing_falls_back_to_zero(self, static_index, structural_index):
        """No lost_to_followup static: value_as_concept_id=0, row still emits with topic."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.lost_to_followup = self._make_followup(True)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].observation_concept_id == PATIENT_WITHDRAWN_CID
        assert result.rows[0].value_as_concept_id == 0

    def test_lost_to_followup_false_emits_nothing(self, static_index, structural_index):
        """False means the patient's not lost to follow-up: no withdrawal event to record."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.lost_to_followup = self._make_followup(False)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_singleton_absent_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_date_skips(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.lost_to_followup = self._make_followup(True, date=None)

        with caplog.at_level(logging.WARNING):
            result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
        assert any("date_lost_to_followup" in rec.message for rec in caplog.records)


class TestAdverseEventOutcome:
    """Pattern 2 and 3: topic concept (structural adverse_event_outcome) and
    answer concept (static adverse_event_outcome,<text>). Either lookup
    can miss and the row still emits with concept_id=0 fallback, as long
    as outcome and start_date are present. FK-linked."""

    def _make_patient(self, outcome: str | None) -> Patient:  # noqa
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.outcome = outcome
        ae.sequence_id = 1
        patient.adverse_events = [ae]
        return patient

    def test_mapped_outcome_emits_row(self, static_index, structural_index):
        _with_ae_outcome_topic(structural_index)
        static_index[("adverse_event_outcome", "recovering/resolving")] = _static("adverse_event_outcome", "recovering/resolving", 1074213, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Recovering/resolving")
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 999)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == AE_OUTCOME_TOPIC_CID
        assert row.observation_date == dt.date(2023, 5, 1)
        assert row.value_as_concept_id == 1074213
        assert row.observation_source_value == "outcome"
        assert row.observation_source_concept_id == 0
        assert row.value_source_value == "Recovering/resolving"
        assert row.observation_event_id == 999
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_topic_structural_missing_falls_back_to_zero(self, static_index, structural_index):
        """
        No topic structural: concept_id=0 but row still emits with mapped
        value and raw outcome preserved in value_source_value.
        """
        static_index[("adverse_event_outcome", "recovering/resolving")] = _static("adverse_event_outcome", "recovering/resolving", 1074213, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Recovering/resolving")
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 999)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == 1074213
        assert row.value_source_value == "Recovering/resolving"

    def test_value_static_missing_falls_back_to_zero(self, static_index, structural_index):
        """
        No static mapping for the outcome text: value_as_concept_id=0,
        row still emits with topic concept and raw outcome preserved.
        """
        _with_ae_outcome_topic(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Some unmapped outcome")
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 999)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == AE_OUTCOME_TOPIC_CID
        assert row.value_as_concept_id == 0
        assert row.value_source_value == "Some unmapped outcome"

    def test_both_lookups_missing_emits_zero_row_with_raw_value(self, static_index, structural_index):
        """
        Worst case: no mappings at all, row still emits with both
        concept ids 0 and value_source_value preserving the raw text.
        """
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Some outcome")
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 999)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == 0
        assert row.value_source_value == "Some outcome"
        assert row.observation_source_value == "outcome"

    def test_outcome_none_emits_nothing(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


class TestAdverseEventWasSerious:
    """
    Pattern 3: concept_id=0 and FK linkage. Emits for both True and False
    (records the assessment either way).
    """

    def _make_patient(self, was_serious: bool | None) -> Patient:  # noqa
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.sequence_id = 42
        ae.was_serious = was_serious
        patient.adverse_events = [ae]
        return patient

    def test_was_serious_true_emits_row_with_fk(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 123456789)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 5, 1)
        assert row.value_as_concept_id == YES_CID
        assert row.observation_source_value == "was_serious"
        assert row.observation_source_concept_id == 0
        assert row.value_source_value == "true"
        assert row.observation_event_id == 123456789
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_was_serious_false_emits_row_with_no_value(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(False)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 123456789)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == NO_CID
        assert result.rows[0].value_source_value == "false"

    def test_was_serious_none_emits_nothing(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_fk_when_no_condition_row_published(self, static_index, structural_index):
        """
        AE with no published condition_occurrence row: observation still emits
        under emit-anyway policy, FK fields left as None.
        """
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        # nothing published for this AE under the condition_occurrence target

        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].observation_event_id is None
        assert result.rows[0].obs_event_field_concept_id is None

    def test_no_fk_when_ae_missing_sequence_id(self, static_index, structural_index):
        """
        AE without sequence_id: NK is still derivable (term + start_date), but
        with no upstream publication the observation emits unlinked.
        """
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.was_serious = True
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].observation_event_id is None
        assert result.rows[0].obs_event_field_concept_id is None

    def test_raises_when_cdm_field_missing_but_fk_resolvable(self, static_index, structural_index):
        """
        cdm_field is required: builder raises.
        """
        _with_yes_no(structural_index)
        # remove the cdm_field entry from the shared fixture to
        # hit the missing-mapping error path
        static_index.pop(("cdm_field", "condition_occurrence.condition_occurrence_id"), None)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 987654321)
        with pytest.raises(RuntimeError, match="cdm_field"):
            ObservationBuilder(concepts).build(ctx)


class TestAdverseEventTurnedSerious:
    def test_emits_row_on_turned_serious_date(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.sequence_id = 7
        ae.turned_serious_date = dt.date(2023, 5, 5)
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 5, 5)
        assert row.value_as_concept_id == YES_CID
        assert row.observation_source_value == "turned_serious_date"
        assert row.value_source_value == "2023-05-05"
        assert row.observation_event_id == 555
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_skipped_when_turned_serious_date_unset(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.sequence_id = 7
        patient.adverse_events = [ae]

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


# CTCAE grade-N precoordinated severity concepts (Standard, SNOMED).
GRADE_1_CID = 763790
GRADE_2_CID = 765927
GRADE_3_CID = 763791
GRADE_4_CID = 765765
GRADE_5_CID = 765766
# Relatedness Meas Value Answer concepts (LOINC).
RELATED_CID = 36032695
NOT_RELATED_CID = 1094365
UNKNOWN_REL_CID = 45877986
# Expectedness Meas Value Answer concepts (LOINC).
EXPECTED_CID = 1472143
NOT_EXPECTED_CID = 1472269


# todo: move to conftest later
def _with_ae_severity(static_index: dict) -> dict:
    """`adverse_event_code` static result: grade 1..5: precoordinated CTCAE concepts."""
    for grade, cid in ((1, GRADE_1_CID), (2, GRADE_2_CID), (3, GRADE_3_CID), (4, GRADE_4_CID), (5, GRADE_5_CID)):
        static_index[("adverse_event_code", str(grade))] = _static("adverse_event_code", str(grade), cid, "observation")
    return static_index


def _with_relatedness(static_index: dict) -> dict:
    static_index[("relatedness", "related")] = _static("relatedness", "related", RELATED_CID, "meas value")
    static_index[("relatedness", "not_related")] = _static("relatedness", "not_related", NOT_RELATED_CID, "meas value")
    static_index[("relatedness", "unknown")] = _static("relatedness", "unknown", UNKNOWN_REL_CID, "meas value")
    return static_index


def _with_expectedness(static_index: dict) -> dict:
    static_index[("expectedness", "true")] = _static("expectedness", "true", EXPECTED_CID, "meas value")
    static_index[("expectedness", "false")] = _static("expectedness", "false", NOT_EXPECTED_CID, "meas value")
    return static_index


class TestAdverseEventSeverity:
    """
    CTCAE severity modifier: observation_concept_id is the
    precoordinated grade-N concept (from `adverse_event_code` static
    lookup), value_as_concept_id is None, value_source_value is the
    grade integer as string. FK-linked to the AE's condition_occurrence row.
    """

    @staticmethod
    def _make_patient(grade: int | None, sequence_id: int | None = 42) -> Patient:
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.grade = grade
        ae.sequence_id = sequence_id
        patient.adverse_events = [ae]
        return patient

    def test_emits_row_for_grade(self, static_index, structural_index):
        _with_ae_severity(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(grade=3)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 777)
        result = ObservationBuilder(concepts).build(ctx)

        severity_result = [r for r in result.rows if r.observation_source_value == "severity"]
        assert len(severity_result) == 1
        row = severity_result[0]
        assert row.observation_concept_id == GRADE_3_CID
        assert row.observation_date == dt.date(2023, 5, 1)
        assert row.value_as_concept_id is None
        assert row.value_source_value == "3"
        assert row.observation_event_id == 777
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_skipped_when_grade_is_none(self, static_index, structural_index):
        _with_ae_severity(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(grade=None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert all(r.observation_source_value != "severity" for r in result.rows)

    def test_falls_back_to_zero_when_grade_unmapped(self, static_index, structural_index):
        """No adverse_event_code static entry: concept_id=0, row still emits."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(grade=3)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        severity_result = [r for r in result.rows if r.observation_source_value == "severity"]
        assert len(severity_result) == 1
        assert severity_result[0].observation_concept_id == 0
        assert severity_result[0].value_source_value == "3"


class TestAdverseEventRelatedness:
    """
    One observation row per non-None
    related_to_treatment_<N>_status. observation_concept_id=0,
    value_as_concept_id = mapped relatedness concept,
    qualifier_source_value = treatment_<N>_name. FK-linked.
    """

    @staticmethod
    def _make_patient(
        *,
        status_1: RelatedStatus | None = None,
        status_2: RelatedStatus | None = None,
        name_1: str | None = "Vemurafenib",
        name_2: str | None = None,
        sequence_id: int = 42,
    ) -> Patient:
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.related_to_treatment_1_status = status_1
        ae.related_to_treatment_2_status = status_2
        ae.treatment_1_name = name_1
        ae.treatment_2_name = name_2
        ae.sequence_id = sequence_id
        patient.adverse_events = [ae]
        return patient

    def test_emits_one_row_per_set_treatment_status(self, static_index, structural_index):
        _with_relatedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(
            status_1=RelatedStatus.RELATED,
            status_2=RelatedStatus.NOT_RELATED,
            name_1="Vemurafenib",
            name_2="Cobimetinib",
        )
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        rel_result = [r for r in result.rows if r.observation_source_value and r.observation_source_value.startswith("related_to_treatment_")]
        assert len(rel_result) == 2
        by_field = {r.observation_source_value: r for r in rel_result}
        assert by_field["related_to_treatment_1"].value_as_concept_id == RELATED_CID
        assert by_field["related_to_treatment_1"].value_source_value == "related"
        assert by_field["related_to_treatment_1"].qualifier_source_value == "Vemurafenib"
        assert by_field["related_to_treatment_1"].observation_event_id == 555
        assert by_field["related_to_treatment_1"].obs_event_field_concept_id == CDM_FIELD_CID
        assert by_field["related_to_treatment_2"].value_as_concept_id == NOT_RELATED_CID
        assert by_field["related_to_treatment_2"].value_source_value == "not_related"
        assert by_field["related_to_treatment_2"].qualifier_source_value == "Cobimetinib"

    def test_unknown_status_maps_to_unknown_concept(self, static_index, structural_index):
        _with_relatedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(status_1=RelatedStatus.UNKNOWN)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        rel_result = [r for r in result.rows if r.observation_source_value == "related_to_treatment_1"]
        assert len(rel_result) == 1
        assert rel_result[0].value_as_concept_id == UNKNOWN_REL_CID
        assert rel_result[0].value_source_value == "unknown"

    def test_none_status_emits_no_row(self, static_index, structural_index):
        _with_relatedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(status_1=None, status_2=None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert all(not (r.observation_source_value or "").startswith("related_to_treatment_") for r in result.rows)

    def test_one_set_one_none(self, static_index, structural_index):
        _with_relatedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(status_1=RelatedStatus.RELATED, status_2=None)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        rel_result = [r for r in result.rows if (r.observation_source_value or "").startswith("related_to_treatment_")]
        assert len(rel_result) == 1
        assert rel_result[0].observation_source_value == "related_to_treatment_1"

    def test_value_concept_falls_back_to_zero_when_unmapped(self, static_index, structural_index):
        """No relatedness static lookup: value_as_concept_id=0, row still emits."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(status_1=RelatedStatus.RELATED)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        rel_result = [r for r in result.rows if r.observation_source_value == "related_to_treatment_1"]
        assert len(rel_result) == 1
        assert rel_result[0].value_as_concept_id == 0
        assert rel_result[0].value_source_value == "related"

    def test_treatment_name_none_yields_null_qualifier(self, static_index, structural_index):
        _with_relatedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(status_1=RelatedStatus.RELATED, name_1=None)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        rel_result = [r for r in result.rows if r.observation_source_value == "related_to_treatment_1"]
        assert len(rel_result) == 1
        assert rel_result[0].qualifier_source_value is None


class TestAdverseEventExpectedness:
    """
    One observation row per non-None
    was_serious_grade_expected_treatment_<N>. Uses LOINC Expected /
    Not Expected concepts in value_as_concept_id. FK-linked.
    """

    @staticmethod
    def _make_patient(
        *,
        expected_1: bool | None = None,
        expected_2: bool | None = None,
        name_1: str | None = "Vemurafenib",
        name_2: str | None = None,
        sequence_id: int = 42,
    ) -> Patient:
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.was_serious_grade_expected_treatment_1 = expected_1
        ae.was_serious_grade_expected_treatment_2 = expected_2
        ae.treatment_1_name = name_1
        ae.treatment_2_name = name_2
        ae.sequence_id = sequence_id
        patient.adverse_events = [ae]
        return patient

    def test_true_maps_to_expected_concept(self, static_index, structural_index):
        _with_expectedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(expected_1=True)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        exp_result = [r for r in result.rows if r.observation_source_value == "was_serious_grade_expected_treatment_1"]
        assert len(exp_result) == 1
        row = exp_result[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == EXPECTED_CID
        assert row.value_source_value == "true"
        assert row.qualifier_source_value == "Vemurafenib"
        assert row.observation_event_id == 555
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_false_maps_to_not_expected_concept(self, static_index, structural_index):
        _with_expectedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(expected_1=False)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        exp_result = [r for r in result.rows if r.observation_source_value == "was_serious_grade_expected_treatment_1"]
        assert len(exp_result) == 1
        assert exp_result[0].value_as_concept_id == NOT_EXPECTED_CID
        assert exp_result[0].value_source_value == "false"

    def test_both_treatments_emit_two_result(self, static_index, structural_index):
        _with_expectedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(expected_1=True, expected_2=False, name_1="Vemurafenib", name_2="Cobimetinib")
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        exp_result = [r for r in result.rows if (r.observation_source_value or "").startswith("was_serious_grade_expected_treatment_")]
        assert len(exp_result) == 2
        by_field = {r.observation_source_value: r for r in exp_result}
        assert by_field["was_serious_grade_expected_treatment_1"].value_as_concept_id == EXPECTED_CID
        assert by_field["was_serious_grade_expected_treatment_1"].qualifier_source_value == "Vemurafenib"
        assert by_field["was_serious_grade_expected_treatment_2"].value_as_concept_id == NOT_EXPECTED_CID
        assert by_field["was_serious_grade_expected_treatment_2"].qualifier_source_value == "Cobimetinib"

    def test_none_emits_no_row(self, static_index, structural_index):
        _with_expectedness(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(expected_1=None, expected_2=None)

        result = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert all(not (r.observation_source_value or "").startswith("was_serious_grade_expected_treatment_") for r in result.rows)

    def test_value_concept_falls_back_to_zero_when_unmapped(self, static_index, structural_index):
        """No expectedness static: value_as_concept_id=0, row still emits."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(expected_1=True)
        ctx = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx, patient.adverse_events[0], 555)
        result = ObservationBuilder(concepts).build(ctx)

        exp_result = [r for r in result.rows if r.observation_source_value == "was_serious_grade_expected_treatment_1"]
        assert len(exp_result) == 1
        assert exp_result[0].value_as_concept_id == 0
        assert exp_result[0].value_source_value == "true"


class TestCombinedSources:
    def test_multi_source_uniqueness_and_determinism(self, static_index, structural_index):
        _with_yes_no(structural_index)
        _with_ae_outcome_topic(structural_index)
        static_index[("eot_reason", "other")] = _static("eot_reason", "other", 35821954, "observation")
        static_index[("lost_to_followup", "true")] = _static("lost_to_followup", "true", LOST_TO_FU_REASON_CID, "observation")
        static_index[("adverse_event_outcome", "fatal")] = _static("adverse_event_outcome", "fatal", 4236718, "observation")

        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 10),
            evaluable_for_efficacy_analysis=True,
        )

        cb = ClinicalBenefit(patient_id=PID)
        cb.week = 16
        cb.has_benefit = False
        cb.date = dt.date(2023, 4, 25)
        patient.clinical_benefit = cb

        eot = EndOfTreatment(patient_id=PID)
        eot.status = TrialOutcomeStatus.WITHDRAWN
        eot.reason = "Other"
        eot.date = dt.date(2023, 8, 1)
        patient.end_of_treatment = eot

        followup = FollowUp(patient_id=PID)
        followup.lost_to_followup = True
        followup.date_lost_to_followup = dt.date(2023, 9, 1)
        patient.lost_to_followup = followup

        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.outcome = "Fatal"
        ae.was_serious = True
        ae.turned_serious_date = dt.date(2023, 5, 5)
        ae.sequence_id = 11
        patient.adverse_events = [ae]

        ctx_a = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx_a, patient.adverse_events[0], 42)
        ctx_b = create_build_context(patient, PERSON_ID)
        publish_ae_condition(ctx_b, patient.adverse_events[0], 42)

        result_a = ObservationBuilder(concepts).build(ctx_a)
        result_b = ObservationBuilder(concepts).build(ctx_b)

        # 4 scalars/singleton (evaluable, clinical_benefit, eot, lost_to_followup)
        # and 3 AE-derived (outcome, was_serious, turned_serious) = 7 result
        assert len(result_a.rows) == 7
        ids = [r.observation_id for r in result_a.rows]
        assert len(ids) == len(set(ids)), "All observation_ids must be unique"

        ids_b = sorted(r.observation_id for r in result_b.rows)
        assert sorted(ids) == ids_b

    def test_multiple_adverse_events_each_independent(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        ae1 = AdverseEvent(patient_id=PID)
        ae1.term = "Fever"
        ae1.start_date = dt.date(2023, 5, 1)
        ae1.sequence_id = 1
        ae1.was_serious = True

        ae2 = AdverseEvent(patient_id=PID)
        ae2.term = "Nausea"
        ae2.start_date = dt.date(2023, 6, 1)
        ae2.sequence_id = 2
        ae2.was_serious = True

        patient.adverse_events = [ae1, ae2]
        ctx = create_build_context(patient, PERSON_ID)
        # patient.adverse_events is sorted by NK: ae1 ("Fever") sorts before ae2 ("Nausea")
        publish_ae_condition(ctx, patient.adverse_events[0], 100)
        publish_ae_condition(ctx, patient.adverse_events[1], 200)
        result = ObservationBuilder(concepts).build(ctx)

        assert len(result.rows) == 2
        by_event_id = {r.observation_event_id: r for r in result.rows}
        assert set(by_event_id.keys()) == {100, 200}
        assert by_event_id[100].observation_date == dt.date(2023, 5, 1)
        assert by_event_id[200].observation_date == dt.date(2023, 6, 1)
