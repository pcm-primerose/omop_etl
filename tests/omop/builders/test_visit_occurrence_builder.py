import datetime as dt

from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.visit_occurrence_builder import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint


class TestVisitOccurrenceBuilder:
    def test_table_name(self, mock_concepts_visit):
        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        assert builder.table_name == "visit_occurrence"

    def test_builds_baseline_visit_with_assessment_date(self, mock_concepts_visit):
        """Baseline assessment with assessment_date creates a visit."""
        patient = Patient(patient_id="P001", trial_id="TEST")
        baseline = TumorAssessmentBaseline(patient_id="P001")
        baseline.assessment_date = dt.date(2023, 1, 15)
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 1
        visit = visits[0]
        assert visit.person_id == person_id
        assert visit.visit_start_date == dt.date(2023, 1, 15)
        assert visit.visit_end_date == dt.date(2023, 1, 15)
        assert visit.visit_source_value == "RECIST"

    def test_baseline_uses_measurement_date_when_assessment_date_is_null(self, mock_concepts_visit):
        """When assessment_date is null, baseline should fallback to target_lesion_measurement_date."""
        patient = Patient(patient_id="P001", trial_id="TEST")
        baseline = TumorAssessmentBaseline(patient_id="P001")
        baseline.assessment_date = None
        baseline.target_lesion_measurement_date = dt.date(2023, 2, 20)
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 1
        assert visits[0].visit_start_date == dt.date(2023, 2, 20)
        assert visits[0].visit_end_date == dt.date(2023, 2, 20)

    def test_no_baseline_visit_when_no_dates_available(self, mock_concepts_visit):
        """No visit created when baseline has neither assessment_date nor measurement_date."""
        patient = Patient(patient_id="P001", trial_id="TEST")
        baseline = TumorAssessmentBaseline(patient_id="P001")
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 0

    def test_builds_visits_from_tumor_assessments(self, mock_concepts_visit):
        """Multiple tumor assessments create multiple visits."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        assessment1 = TumorAssessment(patient_id="P001")
        assessment1.date = dt.date(2023, 3, 1)
        assessment1.event_id = "EVT001"
        assessment1.assessment_type = "RECIST"

        assessment2 = TumorAssessment(patient_id="P001")
        assessment2.date = dt.date(2023, 4, 1)
        assessment2.event_id = "EVT002"
        assessment2.assessment_type = "iRECIST"

        patient.tumor_assessments = [assessment1, assessment2]

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 2
        assert visits[0].visit_start_date == dt.date(2023, 3, 1)
        assert visits[0].visit_source_value == "RECIST"
        assert visits[1].visit_start_date == dt.date(2023, 4, 1)
        assert visits[1].visit_source_value == "iRECIST"

    def test_same_date_different_event_id_creates_two_visits(self, mock_concepts_visit):
        """Two assessments on same date but different event_id should create two visits."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        assessment1 = TumorAssessment(patient_id="P001")
        assessment1.date = dt.date(2023, 3, 1)
        assessment1.event_id = "EVT001"
        assessment1.assessment_type = "RECIST"

        assessment2 = TumorAssessment(patient_id="P001")
        assessment2.date = dt.date(2023, 3, 1)  # same date
        assessment2.event_id = "EVT002"  # different event_id
        assessment2.assessment_type = "iRECIST"

        patient.tumor_assessments = [assessment1, assessment2]

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 2
        assert visits[0].visit_occurrence_id != visits[1].visit_occurrence_id, "should have different visit_occurrence_ids"

    def test_duplicate_date_and_event_id_collapses_to_one_visit(self, mock_concepts_visit):
        """Duplicate (date, event_id) rows should collapse to one visit."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        assessment1 = TumorAssessment(patient_id="P001")
        assessment1.date = dt.date(2023, 3, 1)
        assessment1.event_id = "EVT001"
        assessment1.assessment_type = "RECIST"

        assessment2 = TumorAssessment(patient_id="P001")
        assessment2.date = dt.date(2023, 3, 1)  # same date
        assessment2.event_id = "EVT001"  # same event_id
        assessment2.assessment_type = "iRECIST"

        patient.tumor_assessments = [assessment1, assessment2]

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 1, "should collapse to one visit since date+event_id are the same"

    def test_returns_empty_when_no_assessments(self, mock_concepts_visit):
        """Patient with no baseline or assessments returns empty list."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert visits == []

    def test_skips_assessment_without_date(self, mock_concepts_visit):
        """Assessments without dates are skipped."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        assessment_with_date = TumorAssessment(patient_id="P001")
        assessment_with_date.date = dt.date(2023, 3, 1)
        assessment_with_date.event_id = "EVT001"

        assessment_no_date = TumorAssessment(patient_id="P001")
        assessment_no_date.event_id = "EVT002"

        patient.tumor_assessments = [assessment_with_date, assessment_no_date]

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 1
        assert visits[0].visit_start_date == dt.date(2023, 3, 1)

    def test_combines_baseline_and_assessments(self, mock_concepts_visit):
        """Baseline and assessments are all included in visits."""
        patient = Patient(patient_id="P001", trial_id="TEST")

        baseline = TumorAssessmentBaseline(patient_id="P001")
        baseline.assessment_date = dt.date(2023, 1, 1)
        baseline.assessment_type = "Baseline"
        patient.tumor_assessment_baseline = baseline

        assessment = TumorAssessment(patient_id="P001")
        assessment.date = dt.date(2023, 2, 1)
        assessment.event_id = "EVT001"
        assessment.assessment_type = "Follow-up"
        patient.tumor_assessments = [assessment]

        builder = VisitOccurrenceBuilder(mock_concepts_visit)
        person_id = sha1_bigint("person", patient.patient_id)

        visits = builder.build(patient, person_id)

        assert len(visits) == 2
