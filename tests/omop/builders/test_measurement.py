import datetime as dt
import pytest

from omop_etl.omop.core.id_generator import sha256_bigint
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.measurement import MeasurementBuilder
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.omop.core.linkage import (
    BuildResult,
    OmopRowReference,
    SourceReference,
)
from omop_etl.omop.models.tables import OmopTables
from tests.omop.conftest import (
    _structural,
    concept,
    create_build_context,
    create_patient,
    mapping,
    publish_tumor_condition,
    semantic_index,
)


PID = "p1"
TRIAL = "test"
PERSON_ID = sha256_bigint("person", PID)


class TestMeasurementBuilder:
    def test_table_name(self, static_index, structural_index):
        assert MeasurementBuilder(ConceptLookupService(static_index, structural_index)).table_name == "measurement"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        context = create_build_context(patient, PERSON_ID)
        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        assert len(result.rows) == 0

    def test_visit_occurrence_id_resolves_from_context(self, static_index, structural_index):
        """Canonical visit-linkage test for the measurement builder.

        Every measurement source uses `ctx.resolve_visit_id(date)`, which reads
        visit_occurrence rows published by VisitOccurrenceBuilder under the Visit
        SourceReference `(patient_id, Collections.VISITS, (date,))`. Tested once
        here, negative case is covered by test_visit_id_none_when_no_matches.
        """
        patient = create_patient(PID, TRIAL)
        ecog = EcogBaseline(PID)
        ecog.grade = 1
        ecog.date = dt.date(2040, 5, 1)
        patient.ecog_baseline = ecog

        context = create_build_context(patient, PERSON_ID)
        sentinel = 999_888_777
        context.publish_rows(
            OmopTables.VISIT_OCCURRENCE,
            SourceReference(PID, Patient.Collections.VISITS, (dt.date(2040, 5, 1),)),
            [OmopRowReference(table=OmopTables.VISIT_OCCURRENCE, row_id=sentinel, primary_concept_id=9202)],
        )

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert len(result.rows) == 1
        assert result.rows[0].visit_occurrence_id == sentinel


class TestEcogBaselineRows:
    def test_ecog_baseline_produces_result(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.description = "GSV sleeper service"
        ecog_baseline.date = dt.date(2814, 1, 21)
        patient.ecog_baseline = ecog_baseline

        ecog_result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(ecog_result.rows) == 1
        assert ecog_result.rows[0].person_id == PERSON_ID
        assert ecog_result.rows[0].measurement_concept_id == 36305384
        assert ecog_result.rows[0].measurement_type_concept_id == 32809
        assert ecog_result.rows[0].measurement_date == dt.date(2814, 1, 21)
        assert ecog_result.rows[0].value_as_concept_id == 36310827
        assert ecog_result.rows[0].measurement_datetime == dt.datetime(year=2814, month=1, day=21)
        assert ecog_result.rows[0].value_as_number == 1.0
        assert ecog_result.rows[0].measurement_source_value == "1"

    def test_grade_0_valid(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 0
        ecog_baseline.date = dt.date(200, 1, 1)
        patient.ecog_baseline = ecog_baseline

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result.rows[0].value_as_number == 0.0
        assert result.rows[0].value_as_concept_id == 36309661
        assert result.rows[0].measurement_concept_id == 36305384
        assert result.rows[0].measurement_source_value == "0"

    def test_grade_without_static_mapping_emits_row_with_none_concept(self, static_index, structural_index):
        """When grade has no ecog_code mapping, still emit row but with value_as_concept_id=None."""
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 999
        ecog_baseline.date = dt.date(2023, 1, 1)
        patient.ecog_baseline = ecog_baseline

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == 0, "correct categorical result, but not for this specific value"
        assert result.rows[0].value_as_number == 999.0
        assert result.rows[0].measurement_source_value == "999"

    def test_missing_grade_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = None
        ecog_baseline.description = "something"
        patient.ecog_baseline = ecog_baseline

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result == BuildResult(rows=(), publications=())

    def test_missing_date_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        patient.ecog_baseline = ecog_baseline

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result == BuildResult(rows=(), publications=())

    def test_visit_id_none_when_no_matches(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.date = dt.date(1, 1, 1)
        patient.ecog_baseline = ecog_baseline

        # a baseline assessment with no size emits no measurement row of its own
        tumor_assessment = TumorAssessment(PID)
        tumor_assessment.was_baseline = True
        tumor_assessment.assessment_type = "recist"
        tumor_assessment.date = dt.date(1, 1, 2)
        tumor_assessment.event_id = "V00"
        patient.tumor_assessments = [tumor_assessment]

        context = create_build_context(patient, PERSON_ID)

        _ = VisitOccurrenceBuilder(ConceptLookupService(static_index, structural_index)).build_and_populate(context)
        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert len(result.rows) == 1
        assert result.rows[0].visit_occurrence_id is None

    def test_row_id_deterministic(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ecog_baseline = EcogBaseline(PID)
        ecog_baseline.grade = 1
        ecog_baseline.date = dt.date(1, 1, 1)
        patient.ecog_baseline = ecog_baseline

        context = create_build_context(patient, PERSON_ID)

        result_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        result_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert result_1.rows[0].measurement_id == result_2.rows[0].measurement_id


def _make_tumor_assessments(
    date: dt.date,
    event_id: str,
    *,
    size: float | None = None,
    recist: str | None = None,
    irecist: str | None = None,
    rano: str | None = None,
    was_baseline: bool = False,
) -> TumorAssessment:
    ta = TumorAssessment(PID)
    ta.date = date
    ta.event_id = event_id
    ta.was_baseline = was_baseline
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

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        # lesion_size row
        row_1 = result.rows[0]
        assert row_1.measurement_concept_id == 36768664
        assert row_1.measurement_date == dt.date(2040, 6, 14)
        assert row_1.value_as_number == 13.98
        assert row_1.value_as_concept_id is None
        assert row_1.measurement_source_value == "13.98"
        # RECIST: precoordinated concept, no value_as_number or value_as_concept_id
        row_2 = result.rows[1]
        assert row_2.measurement_concept_id == 1633368
        assert row_2.measurement_date == dt.date(2040, 6, 14)
        assert row_2.value_as_number is None
        assert row_2.value_as_concept_id is None
        assert row_2.measurement_source_value == "Partial Response (PR)"

    def test_baseline_flagged_assessment_emits_lesion_size(self, static_index, structural_index):
        """
        The folded baseline (was_baseline) assessment emits its target lesion
        size like any other assessment, dated at its (V00) date.
        """
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 4, 1), "V00", size=41.0, was_baseline=True),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.measurement_concept_id == 36768664  # lesion_size
        assert row.measurement_date == dt.date(2040, 4, 1)
        assert row.value_as_number == 41.0
        assert row.measurement_source_value == "41.0"

    def test_rano_response(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 7, 1), "V04", rano="Stable Disease (SD)"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 1633447  # RANO-SD
        assert result.rows[0].measurement_source_value == "Stable Disease (SD)"

    def test_irecist_with_divergent_source_string(self, static_index, structural_index):
        # irecist source strings don't follow the same pattern (e.g. "iStable disease" no parens).
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 7, 1), "V04", irecist="iStable disease"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 1635887  # iRECIST-SD

    def test_unmapped_response_is_skipped(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 11, 22), "V05", size=28.987, recist="invalid"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        # only the size row:
        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 36768664
        assert result.rows[0].value_as_number == 28.987

    def test_only_not_evaluable(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 11, 22), "V05", size=28.987, recist="Not Evaluable (NE)"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        # size row and Not Evaluable row
        assert len(result.rows) == 2
        assert result.rows[0].measurement_concept_id == 36768664  # lesion size
        assert result.rows[0].value_as_number == 28.987
        assert result.rows[1].measurement_concept_id == 734317  # RECIST structural
        assert result.rows[1].value_as_concept_id == 45878793  # NE qualifier
        assert result.rows[1].value_as_number is None

    def test_not_evaluable_and_evaluable_produce_four_result(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 11, 22), "V05", size=28.987, recist="Stable Disease (SD)"),
            _make_tumor_assessments(dt.date(2040, 12, 22), "V06", size=300.0, recist="Not Evaluable"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        # each TumorAssessment produces its own size row + its recist row: 4 total
        assert len(result.rows) == 4

        # V05: size & precoordinated SD response
        assert result.rows[0].measurement_concept_id == 36768664  # lesion size
        assert result.rows[0].value_as_number == 28.987
        assert result.rows[1].measurement_concept_id == 1634680  # RECIST SD precoordinated
        assert result.rows[1].value_as_concept_id is None

        # V06: size & NE (structural RECIST and NE qualifier)
        assert result.rows[2].measurement_concept_id == 36768664
        assert result.rows[2].value_as_number == 300.0
        assert result.rows[3].measurement_concept_id == 734317
        assert result.rows[3].value_as_concept_id == 45878793
        assert result.rows[3].value_as_number is None

    def test_missing_date_returns_empty_for_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        ta = TumorAssessment(PID)
        ta.target_lesion_size = 50.0
        ta.recist_response = "Stable Disease (SD)"
        patient.tumor_assessments = [ta]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result == BuildResult(rows=(), publications=())

    def test_missing_size_still_emits_response(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", recist="Complete Response (CR)"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 1634772  # RECIST-CR

    def test_row_ids_unique_across_result_of_same_instance(self, static_index, structural_index):
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

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 4
        assert len(set(ids)) == 4

    def test_row_ids_unique_across_instances(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", recist="Stable Disease (SD)"),
            _make_tumor_assessments(dt.date(2040, 8, 23), "V04", recist="Stable Disease (SD)"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 2
        assert len(set(ids)) == 2


def _make_c30(date: dt.date, event_name: str = "BASELINE", **answers: str | int) -> C30:
    c30 = C30(PID)
    c30.date = date
    c30.event_name = event_name
    for attr, value in answers.items():
        setattr(c30, attr, value)
    return c30


class TestC30Rows:
    def test_emits_q1_with_text_and_level(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="not at all", q1_code=1, q2="quite a bit", q2_code=3),
            _make_c30(dt.date(2041, 5, 1), q2="quite a bit", q2_code=3),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 3

        # c30_q1
        row_1 = result.rows[0]
        assert row_1.measurement_concept_id == 701340
        assert row_1.measurement_date == dt.date(2040, 5, 1)
        assert row_1.value_as_concept_id == 45883172
        assert row_1.value_as_number == 1.0
        assert row_1.measurement_source_value == "not at all"
        assert row_1.measurement_type_concept_id == 32809

        # c30_q2
        row_2 = result.rows[1]
        assert row_2.measurement_concept_id == 701341
        assert row_2.measurement_date == dt.date(2040, 5, 1)
        assert row_2.value_as_concept_id == 45884456
        assert row_2.value_as_number == 3.0
        assert row_2.measurement_source_value == "quite a bit"
        assert row_2.measurement_type_concept_id == 32809

        assert result.rows[2].measurement_date == dt.date(2041, 5, 1)

    def test_q29_uses_global_answer_scale(self, static_index, structural_index):
        # Q29 (overall health) uses c30_global_answer_code (1–7)
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q29="Excellent", q29_code=7),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 701367  # c30_q29
        assert result.rows[0].value_as_concept_id == 45881924  # c30_global_answer_code level 7
        assert result.rows[0].value_as_number == 7.0

    def test_multiple_questions_yield_multiple_result(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="A little", q1_code=2, q2="Quite a bit", q2_code=3),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        by_concept = {r.measurement_concept_id: r for r in result.rows}
        assert by_concept[701340].value_as_concept_id == 45876949  # level 2
        assert by_concept[701341].value_as_concept_id == 45884456  # level 3

    def test_question_with_no_text_or_level_skipped(self, static_index, structural_index):
        # only Q1 answered: rest skipped silently
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="Not at all", q1_code=1),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1

    def test_level_out_of_range_emits_value_as_concept_zero(self, static_index, structural_index):
        # 99 has no static mapping: value_as_concept_id = 0.
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="weird answer", q1_code=99),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_concept_id == 0
        assert result.rows[0].value_as_number == 99.0
        assert result.rows[0].measurement_source_value == "weird answer"

    def test_text_present_but_no_level(self, static_index, structural_index):
        # source has answer text but no level code: emit row with measurement concept,
        # value_as_concept_id NULL since no categorical level to look up, no value_as_number
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="Not at all"),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 701340
        assert result.rows[0].value_as_concept_id is None
        assert result.rows[0].value_as_number is None
        assert result.rows[0].measurement_source_value == "Not at all"

    def test_level_present_but_no_text(self, static_index, structural_index):
        # source has level but no text: emit row with stringified level as source_value.
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_number == 2.0
        assert result.rows[0].value_as_concept_id == 45876949
        assert result.rows[0].measurement_source_value == "2"

    def test_missing_date_returns_empty_for_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        c30 = C30(PID)
        c30.q1 = "Not at all"
        c30.q1_code = 1
        patient.c30_collection = [c30]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result == BuildResult(rows=(), publications=())

    def test_no_structural_concept_skips_question(self, static_index):
        # no c30_q1 in the structural index: that question is skipped, others continue
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="Not at all", q1_code=1, q2="A little", q2_code=2),
        ]
        # only c30_q2 in the structural index
        partial = {"c30_q2": _structural(701341, "measurement")}
        result = MeasurementBuilder(ConceptLookupService(static_index, partial)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 701341

    def test_row_ids_unique_within_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="Not at all", q1_code=1, q2="A little", q2_code=2, q29="Excellent", q29_code=7),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 3
        assert len(set(ids)) == 3

    def test_row_ids_unique_across_instances(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), event_name="BASELINE", q1="Not at all", q1_code=1),
            _make_c30(dt.date(2040, 8, 1), event_name="C04", q1="A little", q1_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 2
        assert len(set(ids)) == 2

    def test_row_id_deterministic(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.c30_collection = [
            _make_c30(dt.date(2040, 5, 1), q1="Not at all", q1_code=1),
        ]
        context = create_build_context(patient, PERSON_ID)

        result_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)
        result_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(context)

        assert result_1.rows[0].measurement_id == result_2.rows[0].measurement_id


def _make_eq5d(date: dt.date, event_name: str = "BASELINE", **answers: str | int) -> EQ5D:
    eq5d = EQ5D(PID)
    eq5d.date = date
    eq5d.event_name = event_name
    for attr, value in answers.items():
        setattr(eq5d, attr, value)
    return eq5d


class TestEQ5DRows:
    def test_dimension_uses_precoordinated_concept(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(
                dt.date(2040, 5, 1),
                q1="I have no problems in walking about",
                q1_code=1,
                q2="I am unable to walk about",
                q2_code=5,
            ),
            _make_eq5d(dt.date(2041, 5, 1), q1="I have slight problems", q1_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 3

        # q1 level 1
        row_1 = result.rows[0]
        assert row_1.value_as_number == 1.0
        assert row_1.measurement_source_value == "I have no problems in walking about"
        assert row_1.measurement_concept_id == 742346
        assert row_1.value_as_concept_id is None
        assert row_1.measurement_type_concept_id == 32809
        assert row_1.person_id == PERSON_ID
        assert row_1.measurement_date == dt.date(2040, 5, 1)
        assert row_1.measurement_datetime == dt.datetime(2040, 5, 1)
        assert row_1.measurement_id == 2245039028177716071  # fixme: assert on expected hash from collection's natural key instead

        # q2 level 5
        row_2 = result.rows[1]
        assert row_2.value_as_number == 5.0
        assert row_2.measurement_concept_id == 742355

        # q1 level 2 from the 2041 instance
        row_3 = result.rows[2]
        assert row_3.value_as_number == 2.0
        assert row_3.measurement_date == dt.date(2041, 5, 1)
        assert row_3.measurement_concept_id == 742347

    def test_levels_2_through_5_resolve_distinct_concepts(self, static_index, structural_index):
        # same dimension but different levels yields different precoordinated concepts
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1_code=1, q2_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        concept_ids = {r.measurement_concept_id for r in result.rows}
        assert concept_ids == {742346, 742352}

    def test_dimension_without_level_skipped(self, static_index, structural_index):
        # text field alone can't be used for precoordinated lookup, skip text only fields
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1="some text", q2_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 742352  # only q2

    def test_unmapped_level_skipped(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1_code=99),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_vas_emits_separate_row(self, static_index, structural_index):
        # qol_metric (VAS: 0–100) uses structural eq5d_qol_score with value_as_number
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), qol_metric=80),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.measurement_concept_id == 42537274  # eq5d_qol_score
        assert row.value_as_concept_id is None
        assert row.value_as_number == 80.0
        assert row.measurement_source_value == "80"

    def test_full_instance_emits_dimensions_and_vas(self, static_index, structural_index):
        # 1 dimension + VAS = 2 result
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1_code=1, qol_metric=80),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        concept_ids = {r.measurement_concept_id for r in result.rows}
        assert concept_ids == {742346, 42537274}

    def test_missing_date_returns_empty_for_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        eq5d = EQ5D(PID)
        eq5d.q1_code = 1
        eq5d.qol_metric = 80
        patient.eq5d_collection = [eq5d]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))
        assert result == BuildResult(rows=(), publications=())

    def test_vas_skipped_when_structural_concept_missing(self, static_index):
        # no eq5d_qol_score in the structural index: VAS row skipped, dimensions still emit result
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1_code=1, qol_metric=80),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, {})).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 742346

    def test_row_ids_unique_within_instance(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), q1_code=1, q2_code=2, qol_metric=80),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 3
        assert len(set(ids)) == 3

    def test_row_ids_unique_across_instances(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.eq5d_collection = [
            _make_eq5d(dt.date(2040, 5, 1), event_name="BASELINE", q1_code=1),
            _make_eq5d(dt.date(2040, 8, 1), event_name="C04", q1_code=2),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        ids = [r.measurement_id for r in result.rows]
        assert len(ids) == 2
        assert len(set(ids)) == 2


def _make_biomarkers(
    date: dt.date | None = dt.date(2040, 5, 1),
    target_biomarker: str | None = None,
) -> Biomarkers:
    b = Biomarkers(PID)
    if date is not None:
        b.date = date
    if target_biomarker is not None:
        b.target_biomarker = target_biomarker
    return b


class TestBiomarkerRows:
    def test_maps_target_biomarker(self, static_index, structural_index):
        # the builder maps a single resolved target_biomarker
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF pathway",
                concept(4002, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers(target_biomarker="BRAF pathway")

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 4002
        assert result.rows[0].measurement_source_value == "BRAF pathway"
        assert result.rows[0].measurement_date == dt.date(2040, 5, 1)
        assert result.rows[0].value_as_number is None
        assert result.rows[0].value_as_concept_id is None

    def test_compound_biomarker_emits_one_row_per_concept(self, static_index, structural_index):
        # single source term maps to two distinct Measurement concepts,
        # semantic mapping is single source of truth, one row per matched concept
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF V600E and KRAS G12C",
                concept(4001, "measurement"),
                concept(4002, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers(target_biomarker="BRAF V600E and KRAS G12C")

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        concept_ids = {r.measurement_concept_id for r in result.rows}
        assert concept_ids == {4001, 4002}
        assert all(r.measurement_source_value == "BRAF V600E and KRAS G12C" for r in result.rows), "both result reference the same source field"
        assert len({r.measurement_id for r in result.rows}) == 2, "row IDs distinct since concept_id is part of the key"

    def test_condition_domain_match_does_not_emit_here(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF V600E",
                concept(999, "condition"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers(target_biomarker="BRAF V600E")

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_field_populated_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers()

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_target_does_not_map_returns_empty(self, static_index, structural_index):
        # target_biomarker is populated but doesn't map, semantic index empty
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers(target_biomarker="x")

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_date_returns_empty(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF V600E",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        biomarkers = Biomarkers(PID)
        biomarkers.target_biomarker = "BRAF V600E"
        patient.biomarkers = biomarkers

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_row_id_deterministic(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF V600E",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.biomarkers = _make_biomarkers(target_biomarker="BRAF V600E")
        context = create_build_context(patient, PERSON_ID)

        result_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)
        result_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)

        assert result_1.rows[0].measurement_id == result_2.rows[0].measurement_id


def _make_ae(
    term: str = "Decreased platelet count",
    start_date: dt.date | None = dt.date(2040, 6, 1),
    grade: int | None = 2,
) -> AdverseEvent:
    ae = AdverseEvent(PID)
    ae.term = term
    if start_date is not None:
        ae.start_date = start_date
    if grade is not None:
        ae.grade = grade
    return ae


class TestAdverseEventMeasurementRows:
    def test_term_with_measurement_and_meas_value_emits_row(self, static_index, structural_index):
        # term maps to Measurement attribute and Meas Value qualifier
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
                concept(4002, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae(term="Decreased platelet count", grade=2)]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.measurement_concept_id == 4001  # measurement attribute
        assert row.value_as_concept_id == 4002  # meas Value qualifier
        assert row.value_as_number == 2.0  # grade
        assert row.measurement_source_value == "Decreased platelet count"
        assert row.measurement_date == dt.date(2040, 6, 1)

    def test_measurement_only_no_meas_value_uses_zero(self, static_index, structural_index):
        # AE term mapping present, but no meas value mapping: value_as_concept_id = 0.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae(grade=3)]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 4001
        assert result.rows[0].value_as_concept_id == 0
        assert result.rows[0].value_as_number == 3.0

    def test_meas_value_only_skips_row(self, static_index, structural_index):
        # no measurement-domain match: no row
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4002, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae()]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_semantic_match_skips_row(self, static_index, structural_index):
        # no semantic match in any domain: no row
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae(term="Some unmapped term")]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_start_date_skips_row(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(PID)
        ae.term = "Decreased platelet count"
        patient.adverse_events = [ae]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_grade_none_yields_value_as_number_none(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae(grade=None)]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].value_as_number is None

    def test_multiple_measurement_concepts_emit_one_row_each(self, static_index, structural_index):
        # if a term maps to multiple measurement concepts, one row per
        # concept: all share the same meas value qualifier when present
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
                concept(4003, "measurement"),
                concept(4002, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae()]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        row_1 = result.rows[0]
        row_2 = result.rows[1]

        assert row_1.measurement_concept_id == 4001
        assert row_1.value_as_concept_id == 4002
        assert row_2.measurement_concept_id == 4003
        assert row_2.value_as_concept_id == 4002

        assert row_1.measurement_id != row_2.measurement_id
        assert len({r.measurement_id for r in result.rows}) == 2, "row IDs distinct since concept_id is part of the key"

    def test_cross_product_when_multiple_meas_values(self, static_index, structural_index, caplog):
        # Unusual but supported: 2 Measurement attributes * 2 Meas Value qualifiers
        # yields 4 result, the cross-product. Builder logs the multi-Meas-Value case.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
                concept(4003, "measurement"),
                concept(4002, "meas value"),
                concept(4004, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae()]

        import logging

        with caplog.at_level(logging.WARNING, logger="omop_etl.omop.builders.measurement"):
            result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        # 2 Measurement * 2 Meas Value = 4 result
        assert len(result.rows) == 4
        pairs = {(r.measurement_concept_id, r.value_as_concept_id) for r in result.rows}
        assert pairs == {(4001, 4002), (4001, 4004), (4003, 4002), (4003, 4004)}
        # all four result have distinct measurement_ids (qualifier in row_id key)
        assert len({r.measurement_id for r in result.rows}) == 4

        # cardinality anomaly is logged
        assert any("Meas Value concepts" in rec.message for rec in caplog.records)

    def test_row_ids_unique_across_aes(self, static_index, structural_index):
        # two AEs: each maps independently
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
            ),
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased hemoglobin",
                concept(4011, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [
            _make_ae(term="Decreased platelet count", start_date=dt.date(2040, 6, 1)),
            _make_ae(term="Decreased hemoglobin", start_date=dt.date(2040, 7, 1)),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert len({r.measurement_id for r in result.rows}) == 2

    def test_row_id_deterministic(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                "Decreased platelet count",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.adverse_events = [_make_ae()]
        context = create_build_context(patient, PERSON_ID)

        result_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)
        result_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)

        assert result_1.rows[0].measurement_id == result_2.rows[0].measurement_id


def _make_mh(
    term: str = "Decreased hemoglobin",
    sequence_id: int = 1,
    start_date: dt.date | None = dt.date(2039, 1, 1),
    end_date: dt.date | None = None,
) -> MedicalHistory:
    mh = MedicalHistory(PID)
    mh.term = term
    mh.sequence_id = sequence_id
    if start_date is not None:
        mh.start_date = start_date
    if end_date is not None:
        mh.end_date = end_date
    return mh


class TestMedicalHistoryMeasurementRows:
    def test_term_with_measurement_and_meas_value_emits_row(self, static_index, structural_index):
        # "decreased hemoglobin": Hemoglobin measurement (Measurement) + Decreased (Meas Value)
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
                concept(4002, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh()]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.measurement_concept_id == 4001
        assert row.value_as_concept_id == 4002
        assert row.value_as_number is None  # MH has no grade
        assert row.measurement_source_value == "Decreased hemoglobin"
        assert row.measurement_date == dt.date(2039, 1, 1)

    def test_measurement_only_no_meas_value_uses_zero(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh()]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].measurement_concept_id == 4001
        assert result.rows[0].value_as_concept_id == 0

    def test_condition_only_skips_row(self, static_index, structural_index):
        # Condition-domain hit belongs to condition_occurrence builder, not here.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Hypertension",
                concept(999, "condition"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh(term="Hypertension")]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_meas_value_only_skips_row(self, static_index, structural_index):
        # Meas Value alone (no Measurement attribute): not a measurement row.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4002, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh()]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_semantic_match_skips_row(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh(term="Some unmapped term")]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_start_date_skips_row(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(PID)
        mh.term = "Decreased hemoglobin"
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_cross_product_when_multiple_meas_values(self, static_index, structural_index, caplog):
        # 2 Measurement * 2 Meas Value yields 4 result: warns on cardinality anomaly.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
                concept(4003, "measurement"),
                concept(4002, "meas value"),
                concept(4004, "meas value"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh()]

        import logging

        with caplog.at_level(logging.WARNING, logger="omop_etl.omop.builders.measurement"):
            result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 4
        pairs = {(r.measurement_concept_id, r.value_as_concept_id) for r in result.rows}
        assert pairs == {(4001, 4002), (4001, 4004), (4003, 4002), (4003, 4004)}
        assert len({r.measurement_id for r in result.rows}) == 4
        assert any("Meas Value concepts" in rec.message for rec in caplog.records)

    def test_row_ids_unique_across_histories(self, static_index, structural_index):
        # two MH instances with distinct sequence_ids: row_ids differ.
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
            ),
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Elevated glucose",
                concept(4011, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [
            _make_mh(term="Decreased hemoglobin", sequence_id=1, start_date=dt.date(2039, 1, 1)),
            _make_mh(term="Elevated glucose", sequence_id=2, start_date=dt.date(2039, 6, 1)),
        ]

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert len({r.measurement_id for r in result.rows}) == 2

    def test_row_id_deterministic(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                "Decreased hemoglobin",
                concept(4001, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        patient.medical_histories = [_make_mh()]
        context = create_build_context(patient, PERSON_ID)

        result_1 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)
        result_2 = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(context)

        assert result_1.rows[0].measurement_id == result_2.rows[0].measurement_id


class TestPrimaryCancerFKConsumption:
    """
    MeasurementBuilder resolves the primary cancer condition published by
    ConditionOccurrenceBuilder (via the tumor SourceReference) to set
    measurement_event_id + meas_event_field_concept_id on lesion-size
    (baseline + follow-up TumorAssessment) and biomarker result.

    Cancer modifier result (lesion size as Dimension of Tumor, biomarkers) link
    back to the primary cancer condition via measurement_event_id +
    meas_event_field_concept_id, per oncology CDM guidelines.
    """

    CDM_FIELD_CID = 1147127
    UNIT_MM_CID = 8588

    @staticmethod
    def _tumor_for(patient, icd10="C50.9", date=None):
        """
        Test helper: attach a tumor_type to the patient so its SourceReference
        can anchor primary-cancer FK publications.
        """
        import datetime as _dt

        tumor = TumorType(PID)
        tumor.icd10_code = icd10
        tumor.date = date or _dt.date(2040, 1, 1)
        patient.tumor_type = tumor
        return tumor

    @staticmethod
    def _baseline_patient() -> Patient:
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 4, 19), "V00", size=41.0, was_baseline=True),
        ]
        return patient

    def _with_millimeter(self, structural_index: dict) -> dict:
        """Add the millimeter unit concept (UCUM) so lesion-size result can populate unit_concept_id."""
        structural_index["millimeter"] = _structural(self.UNIT_MM_CID, "unit")
        return structural_index

    def test_baseline_lesion_size_links_to_primary_cancer(self, static_index, structural_index):
        self._with_millimeter(structural_index)
        patient = self._baseline_patient()
        tumor = self._tumor_for(patient)
        ctx = create_build_context(patient, PERSON_ID)
        publish_tumor_condition(ctx, tumor, 12345)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.measurement_event_id == 12345
        assert row.meas_event_field_concept_id == self.CDM_FIELD_CID
        assert row.unit_concept_id == self.UNIT_MM_CID

    def test_baseline_lesion_size_no_fk_when_primary_cancer_not_published(self, static_index, structural_index):
        self._with_millimeter(structural_index)
        patient = self._baseline_patient()
        ctx = create_build_context(patient, PERSON_ID)
        # no primary cancer condition published: lesion-size result stays unlinked

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].measurement_event_id is None
        assert result.rows[0].meas_event_field_concept_id is None
        # unit_concept_id is independent of FK linkage: still populated
        assert result.rows[0].unit_concept_id == self.UNIT_MM_CID

    def test_baseline_lesion_size_unit_missing_falls_back_to_none(self, static_index, structural_index):
        """structural index without millimeter: unit_concept_id is None."""
        patient = self._baseline_patient()
        ctx = create_build_context(patient, PERSON_ID)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].unit_concept_id is None

    def test_baseline_lesion_size_raises_when_primary_cancer_published_but_cdm_field_missing(self, static_index, structural_index):
        """If a primary cancer condition is published, the cdm_field entry is required"""
        self._with_millimeter(structural_index)
        # remove cdm_field from the shared fixture to hit the missing-mapping error path
        static_index.pop(("cdm_field", "condition_occurrence.condition_occurrence_id"), None)
        patient = self._baseline_patient()
        tumor = self._tumor_for(patient)
        ctx = create_build_context(patient, PERSON_ID)
        publish_tumor_condition(ctx, tumor, 12345)

        with pytest.raises(RuntimeError, match="cdm_field"):
            MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

    def test_tumor_assessment_lesion_size_links_to_primary_cancer(self, static_index, structural_index):
        self._with_millimeter(structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _make_tumor_assessments(dt.date(2040, 6, 14), "V03", size=20.5),
        ]
        tumor = self._tumor_for(patient)
        ctx = create_build_context(patient, PERSON_ID)
        publish_tumor_condition(ctx, tumor, 67890)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

        size_result = [r for r in result.rows if r.measurement_concept_id == 36768664]
        assert len(size_result) == 1
        assert size_result[0].measurement_event_id == 67890
        assert size_result[0].meas_event_field_concept_id == self.CDM_FIELD_CID
        assert size_result[0].unit_concept_id == self.UNIT_MM_CID

    def test_biomarker_links_to_primary_cancer(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF non-V600",
                concept(4000, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        biomarkers = Biomarkers(PID)
        biomarkers.target_biomarker = "BRAF non-V600"
        biomarkers.date = dt.date(2040, 1, 1)
        patient.biomarkers = biomarkers
        tumor = self._tumor_for(patient)
        ctx = create_build_context(patient, PERSON_ID)
        publish_tumor_condition(ctx, tumor, 77777)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].measurement_event_id == 77777
        assert result.rows[0].meas_event_field_concept_id == self.CDM_FIELD_CID

    def test_biomarker_no_fk_when_primary_cancer_not_published(self, static_index, structural_index):
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
                "BRAF non-V600",
                concept(4000, "measurement"),
            ),
        )
        patient = create_patient(PID, TRIAL)
        biomarkers = Biomarkers(PID)
        biomarkers.target_biomarker = "BRAF non-V600"
        biomarkers.date = dt.date(2040, 1, 1)
        patient.biomarkers = biomarkers
        ctx = create_build_context(patient, PERSON_ID)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].measurement_event_id is None
        assert result.rows[0].meas_event_field_concept_id is None

    def test_non_cancer_modifier_result_have_no_fk(self, static_index, structural_index):
        """
        ECOG, C30, EQ5D, AE-measurement, MH-measurement result are not
        cancer modifiers and should not link to primary cancer.
        Verified here on ECOG (AE/MH result are tested elsewhere).
        """
        patient = create_patient(PID, TRIAL)
        ecog = EcogBaseline(PID)
        ecog.grade = 1
        ecog.date = dt.date(2040, 1, 1)
        patient.ecog_baseline = ecog
        # publish a real primary cancer condition so the precondition holds:
        # the ECOG result must still not link to it (it is not a cancer modifier).
        tumor = self._tumor_for(patient)
        ctx = create_build_context(patient, PERSON_ID)
        publish_tumor_condition(ctx, tumor, 99999)

        result = MeasurementBuilder(ConceptLookupService(static_index, structural_index)).build(ctx)

        assert len(result.rows) == 1
        # ECOG result are not cancer modifiers: no FK
        assert result.rows[0].measurement_event_id is None
        assert result.rows[0].meas_event_field_concept_id is None
