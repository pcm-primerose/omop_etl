import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import BuildResult
from tests.omop.conftest import (
    create_build_context,
    create_patient,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


def _baseline(date: dt.date, assessment_type: str = "recist") -> TumorAssessment:
    """The baseline is the was_baseline assessment (V00) folded into the collection."""
    ta = TumorAssessment(patient_id=PID)
    ta.was_baseline = True
    ta.assessment_type = assessment_type
    ta.date = date
    ta.event_id = "V00"
    return ta


class TestVisitOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert VisitOccurrenceBuilder(concepts).table_name == "visit_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


class TestBaselineVisitRows:
    def test_all_fields_from_baseline_assessment(self, static_index, structural_index):
        """The was_baseline assessment produces a visit dated at its V00 date."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = _baseline(dt.date(2023, 1, 15), assessment_type="RECIST")
        patient.tumor_assessments = [baseline]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.person_id == PERSON_ID
        assert row.visit_start_date == dt.date(2023, 1, 15)
        assert row.visit_end_date == dt.date(2023, 1, 15)
        assert row.visit_source_value == "RECIST"
        assert row.visit_concept_id == 9202  # outpatient_visit
        assert row.visit_type_concept_id == 32817  # ecrf

    def test_baseline_without_date_is_skipped(self, static_index, structural_index):
        """A baseline assessment with no date produces no visit row."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessment(patient_id=PID)
        baseline.was_baseline = True
        baseline.assessment_type = "RECIST"
        baseline.event_id = "V00"
        patient.tumor_assessments = [baseline]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


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

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert result.rows[0].visit_start_date == dt.date(2023, 3, 1)
        assert result.rows[0].visit_source_value == "RECIST"
        assert result.rows[1].visit_start_date == dt.date(2023, 4, 1)
        assert result.rows[1].visit_source_value == "iRECIST"

    def test_assessment_without_date_is_skipped(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a_with = TumorAssessment(patient_id=PID)
        a_with.date = dt.date(2023, 3, 1)
        a_with.event_id = "EVT001"
        a_without = TumorAssessment(patient_id=PID)
        a_without.event_id = "EVT002"
        patient.tumor_assessments = [a_with, a_without]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].visit_start_date == dt.date(2023, 3, 1)

    def test_no_event_id_still_produces_row_if_date_exists(self, static_index, structural_index):
        """event_id no longer required, date should be sufficient for a visit row"""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 4, 1)
        patient.tumor_assessments = [a1, a2]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2


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
        a2.event_id = "W04"
        a2.assessment_type = "iRECIST"
        patient.tumor_assessments = [a1, a2]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1

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

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1

    def test_different_dates_create_separate_visits(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 4, 1)
        patient.tumor_assessments = [a1, a2]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert result.rows[0].visit_occurrence_id != result.rows[1].visit_occurrence_id

    def test_baseline_and_assessment_same_date_collapse_to_one_visit(self, static_index, structural_index):
        """The baseline (V00) and a same-date follow-up assessment collapse to one
        visit (one date = one encounter); publication must not raise a
        duplicate-publish error."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        baseline = _baseline(dt.date(2024, 7, 28))
        assessment = TumorAssessment(patient_id=PID)
        assessment.date = dt.date(2024, 7, 28)
        assessment.event_id = "UVT"
        assessment.assessment_type = "recist"
        patient.tumor_assessments = [baseline, assessment]

        ctx = create_build_context(patient, PERSON_ID)
        # build_and_populate applies publications; must not raise on the shared date
        rows = VisitOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 1
        assert rows[0].visit_start_date == dt.date(2024, 7, 28)
        # exactly one visit published for that date
        assert ctx.resolve_visit_id(dt.date(2024, 7, 28)) == rows[0].visit_occurrence_id


class TestBaselineAndAssessmentsCombined:
    def test_baseline_and_assessments_combined(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        baseline = _baseline(dt.date(2023, 1, 1), assessment_type="Baseline")
        assessment = TumorAssessment(patient_id=PID)
        assessment.date = dt.date(2023, 2, 1)
        assessment.event_id = "EVT001"
        assessment.assessment_type = "Follow-up"
        patient.tumor_assessments = [baseline, assessment]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2

    def test_row_ids_are_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [_baseline(dt.date(2023, 1, 1))]

        result_a = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        result_b = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result_a.rows[0].visit_occurrence_id == result_b.rows[0].visit_occurrence_id


class TestPrecedingVisitOccurrenceId:
    def test_single_baseline_has_no_preceding(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [_baseline(dt.date(2023, 1, 1))]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].preceding_visit_occurrence_id is None

    def test_single_assessment_has_no_preceding(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a = TumorAssessment(patient_id=PID)
        a.date = dt.date(2023, 3, 1)
        patient.tumor_assessments = [a]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].preceding_visit_occurrence_id is None

    def test_baseline_then_assessment_links_preceding(self, static_index, structural_index):
        """Assessment's preceding_visit_occurrence_id points to baseline."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a = TumorAssessment(patient_id=PID)
        a.date = dt.date(2023, 3, 1)
        patient.tumor_assessments = [_baseline(dt.date(2023, 1, 1)), a]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert result.rows[0].preceding_visit_occurrence_id is None
        assert result.rows[1].preceding_visit_occurrence_id == result.rows[0].visit_occurrence_id

    def test_chain_of_three_visits(self, static_index, structural_index):
        """Baseline -> assessment1 -> assessment2, each pointing to the previous."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 5, 1)
        patient.tumor_assessments = [_baseline(dt.date(2023, 1, 1)), a1, a2]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 3
        assert result.rows[0].preceding_visit_occurrence_id is None
        assert result.rows[1].preceding_visit_occurrence_id == result.rows[0].visit_occurrence_id
        assert result.rows[2].preceding_visit_occurrence_id == result.rows[1].visit_occurrence_id

    def test_assessments_only_chain(self, static_index, structural_index):
        """Without baseline, assessments still chain to each other."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        a1 = TumorAssessment(patient_id=PID)
        a1.date = dt.date(2023, 3, 1)
        a2 = TumorAssessment(patient_id=PID)
        a2.date = dt.date(2023, 5, 1)
        patient.tumor_assessments = [a1, a2]

        result = VisitOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert result.rows[0].preceding_visit_occurrence_id is None
        assert result.rows[1].preceding_visit_occurrence_id == result.rows[0].visit_occurrence_id
