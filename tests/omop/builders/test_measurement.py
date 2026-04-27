import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
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


class TestTumorAssessmentBaselineRows:
    def test_emits_target_lesion_size_row(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.assessment_type = "recist"
        baseline.assessment_date = dt.date(2040, 4, 1)
        baseline.target_lesion_size = 41
        baseline.target_lesion_nadir = 46  # not emitted
        baseline.target_lesion_measurement_date = dt.date(2040, 4, 19)
        patient.tumor_assessment_baseline = baseline

        context = create_build_context(patient, PERSON_ID)
        _ = VisitOccurrenceBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == PERSON_ID
        assert row.measurement_concept_id == 4084390  # lesion_size
        assert row.measurement_date == dt.date(2040, 4, 19)
        assert row.measurement_datetime == dt.datetime(2040, 4, 19)
        assert row.measurement_type_concept_id == 32817
        assert row.value_as_number == 41.0
        assert row.value_as_concept_id is None
        assert row.measurement_source_value == "41"
        assert row.visit_occurrence_id == context.visit_id_by_date.get(dt.date(2040, 4, 19))

    def test_missing_size_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.target_lesion_measurement_date = dt.date(2040, 4, 19)
        patient.tumor_assessment_baseline = baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_missing_date_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.target_lesion_size = 41
        patient.tumor_assessment_baseline = baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_no_structural_concept_returns_empty(self, static_index):
        # structural index without lesion_size: builder should log and skip the row
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.target_lesion_size = 41
        baseline.target_lesion_measurement_date = dt.date(2040, 4, 19)
        patient.tumor_assessment_baseline = baseline

        rows = MeasurementBuilder(ConceptLookupService(static_index, {})).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_row_id_deterministic(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.target_lesion_size = 41
        baseline.target_lesion_measurement_date = dt.date(2040, 4, 19)
        patient.tumor_assessment_baseline = baseline

        context = create_build_context(patient, PERSON_ID)
        rows_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        rows_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert rows_1[0].measurement_id == rows_2[0].measurement_id


def _make_tumor_assessments(
    date: dt.date,
    event_id: str,
    *,
    size: float | None = None,
    recist: str | None = None,
    irecist: str | None = None,
    rano: str | None = None,
) -> TumorAssessment:
    ta = TumorAssessment(PID)
    ta.date = date
    ta.event_id = event_id
    if size is not None:
        ta.target_lesion_size = size
    if recist is not None:
        ta.recist_response = recist
    if irecist is not None:
        ta.irecist_response = irecist
    if rano is not None:
        ta.rano_response = rano
    return ta


class TestTumorAssessmentRows:
    def test_emits_size_and_recist(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", size=13.98, recist="Partial Response (PR)"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2
        by_concept = {r.measurement_concept_id: r for r in rows}
        # lesion_size row
        size_row = by_concept[4084390]
        assert size_row.measurement_date == dt.date(2040, 6, 14)
        assert size_row.value_as_number == 13.98
        assert size_row.value_as_concept_id is None
        assert size_row.measurement_source_value == "13.98"
        # RECIST PR → precoordinated concept, no value_as_number or value_as_concept_id
        pr_row = by_concept[1633368]
        assert pr_row.measurement_date == dt.date(2040, 6, 14)
        assert pr_row.value_as_number is None
        assert pr_row.value_as_concept_id is None
        assert pr_row.measurement_source_value == "Partial Response (PR)"

    def test_rano_response(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 7, 1), "V04", rano="Stable Disease (SD)"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].measurement_concept_id == 1633447  # RANO-SD
        assert rows[0].measurement_source_value == "Stable Disease (SD)"

    def test_irecist_with_divergent_source_string(self, static_index, structural_index):
        # irecist source strings don't follow the same pattern (e.g. "iStable disease" no parens).
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 7, 1), "V04", irecist="iStable disease"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].measurement_concept_id == 1635887  # iRECIST-SD

    def test_unmapped_response_is_skipped(self, static_index, structural_index):
        # "Not Evaluable (NE)" is not in the static catalogue.
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 11, 22), "V05", size=28.987, recist="Not Evaluable (NE)"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1  # only the size row
        assert rows[0].measurement_concept_id == 4084390

    def test_missing_date_returns_empty_for_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ta = TumorAssessment(PID)
        ta.target_lesion_size = 50.0
        ta.recist_response = "Stable Disease (SD)"
        patient.tumor_assessments = [ta]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert rows == []

    def test_missing_size_still_emits_response(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", recist="Complete Response (CR)"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].measurement_concept_id == 1634772  # RECIST-CR

    def test_row_ids_unique_across_rows_of_same_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(
                dt.date(2040, 6, 14),
                "V03",
                size=13.98,
                recist="Partial Response (PR)",
                irecist="iComplete Response (CR)",
                rano="Stable Disease (SD)",
            ),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in rows]
        assert len(ids) == 4
        assert len(set(ids)) == 4

    def test_row_ids_unique_across_instances(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", recist="Stable Disease (SD)"),
            _make_tumor_assessments(dt.date(2040, 8, 23), "V04", recist="Stable Disease (SD)"),
        ]

        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in rows]
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_visit_id_populated_when_matches(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        baseline = TumorAssessmentBaseline(PID)
        baseline.assessment_type = "recist"
        baseline.assessment_date = dt.date(2040, 4, 19)
        patient.tumor_assessment_baseline = baseline
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", size=13.98),
        ]

        context = create_build_context(patient, PERSON_ID)
        _ = VisitOccurrenceBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        rows = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        # no visit exists on 2040-06-14, so visit_occurrence_id stays None
        assert rows[0].visit_occurrence_id is None
