import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import (
    create_build_context,
    create_patient,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestVisitOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert VisitOccurrenceBuilder(concepts).table_name == "visit_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestBaselineVisitRows:
    def test_all_fields_from_assessment_date(self, static_index, structural_index):
        """Check all fields for baseline row with assessment_date."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.assessment_date = dt.date(2023, 1, 15)
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == PERSON_ID
        assert row.visit_start_date == dt.date(2023, 1, 15)
        assert row.visit_end_date == dt.date(2023, 1, 15)
        assert row.visit_source_value == "RECIST"
        assert row.visit_concept_id == 9202  # outpatient_visit
        assert row.visit_type_concept_id == 32817  # ecrf

    def test_date_fallback_to_target_lesion_measurement(self, static_index, structural_index):
        """When assessment_date is None, fall back to target_lesion_measurement_date."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.target_lesion_measurement_date = dt.date(2023, 2, 20)
        patient.tumor_assessment_baseline = baseline

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].visit_start_date == dt.date(2023, 2, 20)

    def test_date_fallback_to_off_target_lesion_measurement(self, static_index, structural_index):
        """
        When assessment_date and target_lesion_measurement_date are None,
        fall back to off_target_lesion_measurement_date.
        """
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.off_target_lesion_measurement_date = dt.date(2023, 3, 10)
        patient.tumor_assessment_baseline = baseline

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].visit_start_date == dt.date(2023, 3, 10)

    def test_no_dates_skips_baseline(self, static_index, structural_index):
        """Baseline with no usable dates produces no row."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestAssessmentVisitRows:
    def test_multiple_assessments_create_multiple_visits(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a1.event_id = "EVT001"
        a1.assessment_type = "RECIST"
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 4, 1)
        a2.event_id = "EVT002"
        a2.assessment_type = "iRECIST"
        patient.tumor_assessments = [a1, a2]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2
        assert rows[0].visit_start_date == dt.date(2023, 3, 1)
        assert rows[0].visit_source_value == "RECIST"
        assert rows[1].visit_start_date == dt.date(2023, 4, 1)
        assert rows[1].visit_source_value == "iRECIST"

    def test_assessment_without_date_is_skipped(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a_with = TumorAssessment(patient_id=PID)
        a_with.date = dt.date(2023, 3, 1)
        a_with.event_id = "EVT001"
        a_without = TumorAssessment(patient_id=PID)
        a_without.event_id = "EVT002"
        patient.tumor_assessments = [a_with, a_without]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].visit_start_date == dt.date(2023, 3, 1)

    def test_no_event_id_still_produces_row_if_date_exists(self, static_index, structural_index):
        """event_id no longer required, date should be sufficient for a visit row"""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 4, 1)
        patient.tumor_assessments = [a1, a2]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2


class TestDateGrouping:
    def test_same_date_groups_to_one_visit(self, static_index, structural_index):
        """Multiple assessments on the same date, e.g. target and non-target lesion
        measurements from the same encounter, produce one visit row."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a1.event_id = "V04"
        a1.assessment_type = "RECIST"
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 3, 1)
        a2.event_id = "V04"
        a2.assessment_type = "iRECIST"
        patient.tumor_assessments = [a1, a2]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1

    def test_different_event_ids_same_date_groups_to_one_visit(self, static_index, structural_index):
        """Same visit recorded with different event_id labels, e.g. week number vs W00,
        still collapses to one visit because the date is the same."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 6, 15)
        a1.event_id = "V16"
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 6, 15)
        a2.event_id = "W00"
        patient.tumor_assessments = [a1, a2]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1

    def test_different_dates_create_separate_visits(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 4, 1)
        patient.tumor_assessments = [a1, a2]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2
        assert rows[0].visit_occurrence_id != rows[1].visit_occurrence_id


class TestBaselineAndAssessmentsCombined:
    def test_baseline_and_assessments_combined(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.assessment_date = dt.date(2023, 1, 1)
        baseline.assessment_type = "Baseline"
        patient.tumor_assessment_baseline = baseline
        assessment = TumorAssessment(patient_id=PID)
        assessment.date = dt.date(2023, 2, 1)
        assessment.event_id = "EVT001"
        assessment.assessment_type = "Follow-up"
        patient.tumor_assessments = [assessment]

        rows = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2

    def test_row_ids_are_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.assessment_date = dt.date(2023, 1, 1)
        patient.tumor_assessment_baseline = baseline

        rows_a = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        rows_b = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows_a[0].visit_occurrence_id == rows_b[0].visit_occurrence_id
