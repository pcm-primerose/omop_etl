import datetime as dt
import logging

import pytest

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.observation import ObservationBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import (
    _static,
    _structural,
    create_build_context,
    create_patient,
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


def _with_cdm_field(static_index: dict) -> dict:
    """`cdm_field` static entry for AE → condition_occurrence FK linkage."""
    static_index[("cdm_field", "condition_occurrence.condition_occurrence_id")] = _static(
        "cdm_field",
        "condition_occurrence.condition_occurrence_id",
        CDM_FIELD_CID,
        "metadata",
    )
    return static_index


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

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestEvaluableForEfficacy:
    """Shape 1 (unmapped source attribute): concept_id=0, source_value=field
    name, value_source_value=lowercase literal."""

    def test_true_emits_row_with_yes_value(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            evaluable_for_efficacy_analysis=True,
            treatment_start_date=dt.date(2023, 1, 10),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 1, 10)
        assert row.observation_type_concept_id == 32817
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

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].value_as_concept_id == NO_CID
        assert rows[0].value_source_value == "false"

    def test_yes_no_missing_falls_back_to_zero(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            evaluable_for_efficacy_analysis=True,
            treatment_start_date=dt.date(2023, 1, 10),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].value_as_concept_id == 0

    def test_skipped_when_value_is_none(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 10))

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_skipped_when_treatment_start_date_missing(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, evaluable_for_efficacy_analysis=True)

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []
        assert any("treatment_start_date" in rec.message for rec in caplog.records)


class TestClinicalBenefit:
    """Shape 1, like evaluable. Prefers `clinical_benefit_at_week_16_date`
    scalar; falls back to treatment_start + 16w."""

    def test_uses_clinical_benefit_date_scalar_when_set(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            has_clinical_benefit_at_week_16=True,
            clinical_benefit_at_week_16_date=dt.date(2023, 4, 20),
            treatment_start_date=dt.date(2023, 1, 1),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 4, 20)
        assert row.observation_source_value == "has_clinical_benefit_at_week_16"
        assert row.value_as_concept_id == YES_CID
        assert row.value_source_value == "true"

    def test_falls_back_to_treatment_start_plus_16_weeks(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            has_clinical_benefit_at_week_16=True,
            treatment_start_date=dt.date(2023, 1, 1),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].observation_date == dt.date(2023, 1, 1) + dt.timedelta(weeks=16)

    def test_skipped_when_value_is_none(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_skipped_when_no_date_available(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, has_clinical_benefit_at_week_16=True)

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []
        assert any("clinical_benefit_at_week_16_date" in rec.message for rec in caplog.records)


class TestEndOfTreatmentReason:
    """Shape 1: concept_id=0, field name in source_value, mapped reason concept
    in value_as_concept_id (or 0 if unmapped), raw reason preserved in both
    value_as_string and value_source_value."""

    def test_mapped_reason_emits_row(self, static_index, structural_index):
        static_index[("eot_reason", "disease progression")] = _static("eot_reason", "disease progression", 1617595, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            end_of_treatment_reason="Disease progression",
            end_of_treatment_date=dt.date(2023, 8, 1),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.observation_date == dt.date(2023, 8, 1)
        assert row.observation_source_value == "end_of_treatment_reason"
        assert row.observation_source_concept_id == 0
        assert row.value_as_concept_id == 1617595
        assert row.value_as_string == "Disease progression"
        assert row.value_source_value == "Disease progression"

    def test_unmapped_reason_emits_row_with_value_concept_zero(self, static_index, structural_index):
        """No static mapping → row still emits, value_as_concept_id=0, raw
        reason preserved in value_as_string + value_source_value."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            end_of_treatment_reason="Some new reason not in mapping",
            end_of_treatment_date=dt.date(2023, 8, 1),
        )

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == 0
        assert row.value_as_string == "Some new reason not in mapping"
        assert row.value_source_value == "Some new reason not in mapping"[:50]
        assert row.observation_source_value == "end_of_treatment_reason"

    def test_skipped_without_eot_date(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, end_of_treatment_reason="Disease progression")

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []
        assert any("end_of_treatment_date" in rec.message for rec in caplog.records)


class TestLostToFollowup:
    """Shape 2 (mapped topic): concept_id=Lost-to-follow-up,
    source_value=field name, value_source_value=lowercase literal."""

    def test_lost_to_followup_true_emits_row(self, static_index, structural_index):
        _with_yes_no(structural_index)
        static_index[("lost_to_followup", "true")] = _static("lost_to_followup", "true", 4163894, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        followup = FollowUp(patient_id=PID)
        followup.lost_to_followup = True
        followup.date_lost_to_followup = dt.date(2023, 12, 1)
        patient.lost_to_followup = followup

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 4163894
        assert row.observation_date == dt.date(2023, 12, 1)
        assert row.value_as_concept_id == YES_CID
        assert row.observation_source_value == "lost_to_followup"
        assert row.observation_source_concept_id == 0
        assert row.value_source_value == "true"

    def test_lost_to_followup_false_emits_row_with_no_value(self, static_index, structural_index):
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        followup = FollowUp(patient_id=PID)
        followup.lost_to_followup = False
        followup.date_lost_to_followup = dt.date(2023, 12, 1)
        patient.lost_to_followup = followup

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].observation_concept_id == 0
        assert rows[0].value_as_concept_id == NO_CID
        assert rows[0].observation_source_value == "lost_to_followup"
        assert rows[0].value_source_value == "false"

    def test_singleton_absent_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_missing_date_skips(self, static_index, structural_index, caplog):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        followup = FollowUp(patient_id=PID)
        followup.lost_to_followup = True
        patient.lost_to_followup = followup

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []
        assert any("date_lost_to_followup" in rec.message for rec in caplog.records)


class TestAdverseEventOutcome:
    """Shape 2/3: topic concept (structural `adverse_event_outcome`) +
    answer concept (static `adverse_event_outcome,<text>`). Either lookup
    can miss and the row still emits with concept_id=0 fallback, as long
    as outcome and start_date are present. FK-linked."""

    def _make_patient(self, outcome: str | None) -> Patient:
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
        _with_cdm_field(static_index)
        static_index[("adverse_event_outcome", "recovering/resolving")] = _static("adverse_event_outcome", "recovering/resolving", 1074213, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Recovering/resolving")
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[1] = 999

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == AE_OUTCOME_TOPIC_CID
        assert row.observation_date == dt.date(2023, 5, 1)
        assert row.value_as_concept_id == 1074213
        assert row.observation_source_value == "outcome"
        assert row.observation_source_concept_id == 0
        assert row.value_source_value == "Recovering/resolving"
        assert row.observation_event_id == 999
        assert row.obs_event_field_concept_id == CDM_FIELD_CID

    def test_topic_structural_missing_falls_back_to_zero(self, static_index, structural_index):
        """No topic structural → concept_id=0 but row still emits with mapped
        value and raw outcome preserved in value_source_value."""
        _with_cdm_field(static_index)
        static_index[("adverse_event_outcome", "recovering/resolving")] = _static("adverse_event_outcome", "recovering/resolving", 1074213, "observation")
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Recovering/resolving")
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[1] = 999

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == 1074213
        assert row.value_source_value == "Recovering/resolving"

    def test_value_static_missing_falls_back_to_zero(self, static_index, structural_index):
        """No static mapping for the outcome text → value_as_concept_id=0,
        row still emits with topic concept and raw outcome preserved."""
        _with_ae_outcome_topic(structural_index)
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Some unmapped outcome")
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[1] = 999

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == AE_OUTCOME_TOPIC_CID
        assert row.value_as_concept_id == 0
        assert row.value_source_value == "Some unmapped outcome"

    def test_both_lookups_missing_emits_zero_row_with_raw_value(self, static_index, structural_index):
        """Worst case: no mappings at all, row still emits with both
        concept ids 0 and value_source_value preserving the raw text."""
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient("Some outcome")
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[1] = 999

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
        assert row.observation_concept_id == 0
        assert row.value_as_concept_id == 0
        assert row.value_source_value == "Some outcome"
        assert row.observation_source_value == "outcome"

    def test_outcome_none_emits_nothing(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(None)

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestAdverseEventWasSerious:
    """Shape 3: concept_id=0 + FK linkage. Emits for both True and False
    (records the assessment either way)."""

    def _make_patient(self, was_serious: bool | None) -> Patient:
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
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[42] = 123456789

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
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
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(False)
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[42] = 123456789

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        assert rows[0].value_as_concept_id == NO_CID
        assert rows[0].value_source_value == "false"

    def test_was_serious_none_emits_nothing(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(None)

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_no_fk_when_no_condition_row_published(self, static_index, structural_index, caplog):
        """AE with sequence_id but no published condition_occurrence row:
        observation still emits, FK fields left blank, warning logged."""
        _with_yes_no(structural_index)
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        # condition_id_by_ae_sequence_id stays empty

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        assert rows[0].observation_event_id is None
        assert rows[0].obs_event_field_concept_id is None
        assert any("missing event_id" in rec.message for rec in caplog.records)

    def test_no_fk_when_ae_missing_sequence_id(self, static_index, structural_index, caplog):
        _with_yes_no(structural_index)
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.was_serious = True
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        with caplog.at_level(logging.WARNING):
            rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        assert rows[0].observation_event_id is None
        assert rows[0].obs_event_field_concept_id is None
        assert any("missing sequence_id" in rec.message for rec in caplog.records)

    def test_raises_when_cdm_field_missing_but_fk_resolvable(self, static_index, structural_index):
        """cdm_field is required infrastructure: builder raises rather than
        emit a partially-linked row when the static entry is missing."""
        _with_yes_no(structural_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = self._make_patient(True)
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[42] = 987654321

        with pytest.raises(RuntimeError, match="cdm_field"):
            ObservationBuilder(concepts).build(ctx)


class TestAdverseEventTurnedSerious:
    def test_emits_row_on_turned_serious_date(self, static_index, structural_index):
        _with_yes_no(structural_index)
        _with_cdm_field(static_index)
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 5, 1)
        ae.sequence_id = 7
        ae.turned_serious_date = dt.date(2023, 5, 5)
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)
        ctx.condition_id_by_ae_sequence_id[7] = 555

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 1
        row = rows[0]
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

        rows = ObservationBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestCombinedSources:
    def test_multi_source_uniqueness_and_determinism(self, static_index, structural_index):
        _with_yes_no(structural_index)
        _with_cdm_field(static_index)
        _with_ae_outcome_topic(structural_index)
        static_index[("eot_reason", "other")] = _static("eot_reason", "other", 35821954, "observation")
        static_index[("lost_to_followup", "true")] = _static("lost_to_followup", "true", 4163894, "observation")
        static_index[("adverse_event_outcome", "fatal")] = _static("adverse_event_outcome", "fatal", 4236718, "observation")

        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 10),
            evaluable_for_efficacy_analysis=True,
            has_clinical_benefit_at_week_16=False,
            end_of_treatment_reason="Other",
            end_of_treatment_date=dt.date(2023, 8, 1),
        )

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
        ctx_a.condition_id_by_ae_sequence_id[11] = 42
        ctx_b = create_build_context(patient, PERSON_ID)
        ctx_b.condition_id_by_ae_sequence_id[11] = 42

        rows_a = ObservationBuilder(concepts).build(ctx_a)
        rows_b = ObservationBuilder(concepts).build(ctx_b)

        # 4 scalars/singleton (evaluable + clinical_benefit + eot + lost_to_followup)
        # + 3 AE-derived (outcome, was_serious, turned_serious) = 7 rows
        assert len(rows_a) == 7
        ids = [r.observation_id for r in rows_a]
        assert len(ids) == len(set(ids)), "All observation_ids must be unique"

        ids_b = sorted(r.observation_id for r in rows_b)
        assert sorted(ids) == ids_b

    def test_multiple_adverse_events_each_independent(self, static_index, structural_index):
        _with_yes_no(structural_index)
        _with_cdm_field(static_index)
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
        ctx.condition_id_by_ae_sequence_id[1] = 100
        ctx.condition_id_by_ae_sequence_id[2] = 200

        rows = ObservationBuilder(concepts).build(ctx)

        assert len(rows) == 2
        by_event_id = {r.observation_event_id: r for r in rows}
        assert set(by_event_id.keys()) == {100, 200}
        assert by_event_id[100].observation_date == dt.date(2023, 5, 1)
        assert by_event_id[200].observation_date == dt.date(2023, 6, 1)
