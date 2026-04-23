import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.omop.builders.measurement import MeasurementBuilder
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import (
    create_build_context,
    create_patient,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestMeasurementBuilder:
    def test_table_name(self, static_index, structural_index):
        assert MeasurementBuilder(ConceptLookupService(static_index, structural_index)).table_name == "measurement"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        context = create_build_context(patient, PERSON_ID)
        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        assert len(rows) == 0


class TestEcogBaselineRows:
    def test_ecog_baseline_produces_rows(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.description = "GSV sleeper service"
        ecog_baseline.date = dt.date(2814, 1, 21)
        patient.ecog_baseline = ecog_baseline

        baseline = TumorAssessmentBaseline(patient_id=PID)
        baseline.assessment_date = dt.date(2814, 1, 21)
        baseline.assessment_type = "RECIST"
        patient.tumor_assessment_baseline = baseline

        context = create_build_context(patient, PERSON_ID)
        _ = VisitOccurrenceBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        ecog_rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert len(ecog_rows) == 1
        assert ecog_rows[0].person_id == PERSON_ID
        assert ecog_rows[0].measurement_concept_id == 36305384
        assert ecog_rows[0].measurement_type_concept_id == 32817
        assert ecog_rows[0].measurement_date == dt.date(2814, 1, 21)
        assert ecog_rows[0].value_as_concept_id == 36310827

        # optional fields
        assert ecog_rows[0].visit_occurrence_id == context.visit_id_by_date.get(dt.date(2814, 1, 21))
        assert ecog_rows[0].measurement_datetime == dt.datetime(year=2814, month=1, day=21)
        assert ecog_rows[0].value_as_number == 1.0
        assert ecog_rows[0].measurement_source_value == "1"

    def test_grade_0_valid(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 0
        ecog_baseline.date = dt.date(200, 1, 1)
        patient.ecog_baseline = ecog_baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows[0].value_as_number == 0.0
        assert rows[0].value_as_concept_id == 36309661
        assert rows[0].measurement_concept_id == 36305384
        assert rows[0].measurement_source_value == "0"

    def test_grade_without_static_mapping_emits_row_with_none_concept(self, static_index, structural_index):
        """When grade has no ecog_code mapping, still emit row but with value_as_concept_id=None."""
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 999
        ecog_baseline.date = dt.date(2023, 1, 1)
        patient.ecog_baseline = ecog_baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].value_as_concept_id == 0, "correct categorical result, but not for this specific value"
        assert rows[0].value_as_number == 999.0
        assert rows[0].measurement_source_value == "999"

    def test_missing_grade_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = None
        ecog_baseline.description = "something"
        patient.ecog_baseline = ecog_baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_missing_date_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        patient.ecog_baseline = ecog_baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_visit_id_none_when_no_matches(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.date = dt.date(1, 1, 1)
        patient.ecog_baseline = ecog_baseline

        tumor_assessment = TumorAssessmentBaseline(PID)
        tumor_assessment.assessment_type = "recist"
        tumor_assessment.assessment_date = dt.date(1, 1, 2)
        patient.tumor_assessment_baseline = tumor_assessment

        context = create_build_context(patient, PERSON_ID)

        _ = VisitOccurrenceBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert len(rows) == 1
        assert rows[0].visit_occurrence_id is None

    def test_row_id_deterministic(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.date = dt.date(1, 1, 1)
        patient.ecog_baseline = ecog_baseline

        context = create_build_context(patient, PERSON_ID)

        rows_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        rows_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert rows_1[0].measurement_id == rows_2[0].measurement_id
