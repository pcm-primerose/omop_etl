import datetime as dt

import pytest

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.core.linkage import (
    BuildResult,
    SourceReference,
)
from tests.omop.conftest import (
    create_patient,
    create_build_context,
    semantic_index,
    mapping,
    concept,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha256_bigint("person", PID)


def _cycle(
    treatment_number: int,
    cycle_number: int,
    start: dt.date,
    end: dt.date | None = None,
    name: str = "drugX",
) -> TreatmentCycleComponent:
    """Scaffolding: a treatment-cycle component. The treatment_number, cycle_number,
    name and dates are the test subject, passed in by each test."""
    c = TreatmentCycleComponent(patient_id=PID)
    c.source_treatment_name = name
    c.treatment_number = treatment_number
    c.cycle_number = cycle_number
    c.start_date = start
    c.end_date = end
    return c


def _tumor(main_tumor_type: str = "Melanoma", date: dt.date | None = None) -> TumorType:
    t = TumorType(patient_id=PID)
    t.main_tumor_type = main_tumor_type
    t.date = date
    return t


def _tumor_condition_mapping(condition_concept_id: int):
    """
    Semantic mapping so ConditionOccurrenceBuilder emits and publishes the primary
    cancer condition the Disease Episode scopes to.
    """
    return mapping(
        (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
        "Melanoma",
        concept(condition_concept_id, "condition"),
    )


def _assessment(date: dt.date, recist: str | None = None, irecist: str | None = None) -> TumorAssessment:
    """
    A tumor assessment carrying a response on one scale, event_id keyed on the
    date so each is NK-distinct.
    """
    ta = TumorAssessment(patient_id=PID)
    ta.assessment_type = "RECIST 1.1"
    ta.date = date
    ta.event_id = date.isoformat()
    ta.recist_response = recist
    ta.irecist_response = irecist
    return ta


def _dynamic_episode_concept(static_index: dict, value_set: str, response: str) -> int:
    """
    The Disease Dynamic Episode concept the builder derives for a response, through
    the same fixture chain it uses: response -> Measurement concept -> dynamic_status.
    """
    measurement = static_index[(value_set, response.lower())].concept_id
    return static_index[("dynamic_status", str(measurement).lower())].concept_id


@pytest.fixture
def regimen_only_structural(structural_index):
    """structural_index minus the cycle concept."""
    return {k: v for k, v in structural_index.items() if k != "treatment_cycle"}


@pytest.fixture
def cycle_only_structural(structural_index):
    """structural_index without the regimen concept."""
    return {k: v for k, v in structural_index.items() if k != "treatment_regimen"}


class TestEpisodeBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert EpisodeBuilder(concepts).table_name == "episode"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


class TestTreatmentRegimenEpisodes:
    def test_one_line_spans_its_cycles(self, static_index, regimen_only_structural):
        concepts = ConceptLookupService(static_index, regimen_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20), name="Erivedge"),
            _cycle(1, 2, dt.date(2023, 2, 1), dt.date(2023, 2, 20), name="Erivedge"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        ep = result.rows[0]
        assert ep.person_id == PERSON_ID
        assert ep.episode_concept_id == regimen_only_structural["treatment_regimen"].concept_id
        assert ep.episode_type_concept_id == regimen_only_structural["ecrf"].concept_id
        assert ep.episode_object_concept_id == 0  # no REGIMEN mapping seeded
        assert ep.episode_number == 1
        assert ep.episode_start_date == dt.date(2023, 1, 1)
        assert ep.episode_end_date == dt.date(2023, 2, 20)
        assert ep.episode_start_datetime == dt.datetime(2023, 1, 1)
        assert ep.episode_end_datetime == dt.datetime(2023, 2, 20)
        assert ep.episode_source_value == "Erivedge"

    def test_end_date_falls_back_to_cycle_start(self, static_index, regimen_only_structural):
        # a cycle with no end_date contributes its own start to the line span
        concepts = ConceptLookupService(static_index, regimen_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20)),
            _cycle(1, 2, dt.date(2023, 3, 1), end=None),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows[0].episode_end_date == dt.date(2023, 3, 1)

    def test_one_episode_per_line(self, static_index, regimen_only_structural):
        concepts = ConceptLookupService(static_index, regimen_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1)),
            _cycle(2, 1, dt.date(2023, 6, 1)),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert [r.episode_number for r in result.rows] == [1, 2]  # sorted by line
        assert len({r.episode_id for r in result.rows}) == 2

    def test_combination_line_does_not_raise(self, static_index, regimen_only_structural):
        # a line whose cycles carry distinct drug names (e.g. Piqray, Fulvestrant)
        # must not raise; the regimen logs and keeps the most common drug name
        concepts = ConceptLookupService(static_index, regimen_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), name="Piqray"),
            _cycle(1, 2, dt.date(2023, 1, 1), name="Fulvestrant"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_source_value in {"Piqray", "Fulvestrant"}
        assert result.rows[0].episode_object_concept_id == 0

    def test_no_cycle_concept_emits_regimens_only(self, static_index, regimen_only_structural):
        # the degraded path: with no treatment_cycle concept the build still emits
        # the line's regimen (the regimen_only_structural fixture is this condition)
        concepts = ConceptLookupService(static_index, regimen_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_concept_id == regimen_only_structural["treatment_regimen"].concept_id

    def test_regimens_are_not_published(self, static_index, structural_index):
        # regimens are no longer published (nothing resolves them); only cycles are.
        # the cycle->regimen link is episode_parent_id, set in-row.
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        EpisodeBuilder(concepts).build_and_populate(ctx)

        regimen_ref = ctx.resolve_rows(OmopTables.EPISODE, SourceReference(PID, Patient.Collections.TREATMENT_CYCLES, (1,)))
        assert regimen_ref == ()

    def test_parents_to_disease_episode(self, static_index, regimen_only_structural):
        # the line's regimen hangs off the patient's Disease Episode (the root)
        semantic = semantic_index(_tumor_condition_mapping(4112853))
        concepts = ConceptLookupService(static_index, regimen_only_structural, semantic)
        patient = create_patient(PID, TRIAL)
        patient.tumor_type = _tumor(date=dt.date(2023, 1, 10))
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 2, 1))]
        ctx = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        result = EpisodeBuilder(concepts).build(ctx)

        disease = next(r for r in result.rows if r.episode_parent_id is None)
        regimen = next(r for r in result.rows if r.episode_parent_id is not None)
        assert regimen.episode_parent_id == disease.episode_id


class TestTreatmentCycleEpisodes:
    def test_one_episode_per_cycle_number(self, static_index, cycle_only_structural):
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20)),
            _cycle(1, 2, dt.date(2023, 2, 1), dt.date(2023, 2, 20)),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert [c.episode_number for c in result.rows] == [1, 2]  # sorted by cycle number
        assert len({c.episode_id for c in result.rows}) == 2
        # each cycle spans its own component's dates, not the whole line
        first = result.rows[0]
        assert first.episode_type_concept_id == cycle_only_structural["ecrf"].concept_id
        assert first.episode_start_date == dt.date(2023, 1, 1)
        assert first.episode_end_date == dt.date(2023, 1, 20)
        assert first.episode_start_datetime == dt.datetime(2023, 1, 1)

    def test_combination_cycle_collapses_to_one_episode(self, static_index, cycle_only_structural):
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        c1 = TreatmentCycleComponent(patient_id=PID)
        c1.source_treatment_name = "Phesgo (Pertuzumab and Trastuzumab)"
        c1.ingredient_name = "Pertuzumab"
        c1.treatment_number = 1
        c1.cycle_number = 1
        c1.start_date = dt.date(2023, 1, 1)
        c1.end_date = dt.date(2023, 1, 20)
        c2 = TreatmentCycleComponent(patient_id=PID)
        c2.source_treatment_name = "Phesgo (Pertuzumab and Trastuzumab)"
        c2.ingredient_name = "Trastuzumab"
        c2.treatment_number = 1
        c2.cycle_number = 1
        c2.start_date = dt.date(2023, 1, 1)
        c2.end_date = dt.date(2023, 1, 20)
        patient.treatment_cycles = [c1, c2]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_number == 1
        assert result.rows[0].episode_source_value == "Phesgo (Pertuzumab and Trastuzumab)"
        assert result.rows[0].episode_start_date == dt.date(2023, 1, 1)
        assert result.rows[0].episode_end_date == dt.date(2023, 1, 20)

    def test_orphaned_without_regimen_concept(self, static_index, cycle_only_structural):
        # orphaned: no regimen parent (None) and no inherited object concept (0) but still emits
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_parent_id is None
        assert result.rows[0].episode_object_concept_id == 0

    def test_publishes_cycle_episode_ref(self, static_index, cycle_only_structural):
        # cycles publish under their (treatment_number, cycle_number) source key
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        refs = ctx.resolve_rows(
            OmopTables.EPISODE,
            SourceReference(PID, Patient.Collections.TREATMENT_CYCLES, (1, 1)),
        )
        assert len(refs) == 1
        assert refs[0].row_id == rows[0].episode_id

    def test_inherits_regimen_parent_and_object(self, static_index, structural_index):
        # full index: cycle parent_id is the line's regimen episode_id and the object
        # concept is inherited from the regimen (a real REGIMEN semantic match, non-zero)
        semantic = semantic_index(
            mapping(
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                "Trametinib",
                concept(35803140, "regimen"),
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), name="Trametinib"),
            _cycle(1, 2, dt.date(2023, 2, 1), name="Trametinib"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        regimen = next(r for r in result.rows if r.episode_parent_id is None)
        cycles = [r for r in result.rows if r.episode_parent_id is not None]
        assert regimen.episode_object_concept_id == 35803140
        assert len(cycles) == 2
        assert all(c.episode_parent_id == regimen.episode_id for c in cycles)
        assert all(c.episode_object_concept_id == 35803140 for c in cycles)

    def test_distinct_regimen_and_cycle_ids(self, static_index, structural_index):
        # full index: regimen and cycle of the same line hash to distinct episode_ids
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len({r.episode_id for r in result.rows}) == len(result.rows)


class TestDiseaseEpisodes:
    def test_disease_episode_from_published_condition(self, static_index, structural_index):
        # object concept and start date pusblished from ConditionOccurrenceBuilder
        # end date is the patient's death
        semantic = semantic_index(_tumor_condition_mapping(4112853))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL, date_of_death=dt.date(2024, 5, 1))
        patient.tumor_type = _tumor(date=dt.date(2023, 1, 10))
        ctx = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        result = EpisodeBuilder(concepts).build(ctx)

        assert len(result.rows) == 1
        ep = result.rows[0]
        assert ep.person_id == PERSON_ID
        assert ep.episode_object_concept_id == 4112853  # published condition's concept
        assert ep.episode_start_date == dt.date(2023, 1, 10)
        assert ep.episode_end_date == dt.date(2024, 5, 1)
        assert ep.episode_parent_id is None
        assert ep.episode_number is None
        assert ep.episode_type_concept_id == structural_index["ecrf"].concept_id
        assert ep.episode_source_value == "Melanoma"

    def test_end_date_null_when_alive(self, static_index, structural_index):
        semantic = semantic_index(_tumor_condition_mapping(4112853))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)  # no date_of_death
        patient.tumor_type = _tumor(date=dt.date(2023, 1, 10))
        ctx = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        result = EpisodeBuilder(concepts).build(ctx)

        assert result.rows[0].episode_end_date is None

    def test_no_disease_episode_without_tumor(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()

    def test_no_disease_episode_without_published_condition(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_type = _tumor(date=dt.date(2023, 1, 10))
        ctx = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        result = EpisodeBuilder(concepts).build(ctx)

        assert result.rows == ()


class TestDiseaseDynamicEpisodes:
    def test_run_length_encodes_responses(self, static_index, structural_index):
        # consecutive same-status assessments collapse into one episode each
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _assessment(dt.date(2023, 1, 1), recist="Complete Response (CR)"),
            _assessment(dt.date(2023, 2, 1), recist="Complete Response (CR)"),
            _assessment(dt.date(2023, 3, 1), recist="Partial Response (PR)"),
            _assessment(dt.date(2023, 4, 1), recist="Stable Disease (SD)"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert [r.episode_concept_id for r in result.rows] == [
            _dynamic_episode_concept(static_index, "response_recist", "Complete Response (CR)"),
            _dynamic_episode_concept(static_index, "response_recist", "Partial Response (PR)"),
            _dynamic_episode_concept(static_index, "response_recist", "Stable Disease (SD)"),
        ]
        # CR run spans its two assessments, ends the day before PR starts
        assert result.rows[0].episode_start_date == dt.date(2023, 1, 1)
        assert result.rows[0].episode_end_date == dt.date(2023, 2, 28)
        # PR run ends the day before SD starts
        assert result.rows[1].episode_start_date == dt.date(2023, 3, 1)
        assert result.rows[1].episode_end_date == dt.date(2023, 3, 31)
        # last run is open-ended
        assert result.rows[2].episode_start_date == dt.date(2023, 4, 1)
        assert result.rows[2].episode_end_date is None

    def test_not_evaluable_drops_and_merges_neighbours(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _assessment(dt.date(2023, 1, 1), recist="Complete Response (CR)"),
            _assessment(dt.date(2023, 2, 1), recist="Not evaluable"),
            _assessment(dt.date(2023, 3, 1), recist="Complete Response (CR)"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_concept_id == _dynamic_episode_concept(static_index, "response_recist", "Complete Response (CR)")
        assert result.rows[0].episode_start_date == dt.date(2023, 1, 1)
        assert result.rows[0].episode_end_date is None

    def test_no_dynamic_without_evaluable_response(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [_assessment(dt.date(2023, 1, 1), recist="Not evaluable")]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()

    def test_irecist_response_maps_through(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [_assessment(dt.date(2023, 1, 1), irecist="iComplete Response (CR)")]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_concept_id == _dynamic_episode_concept(static_index, "response_irecist", "iComplete Response (CR)")

    def test_parents_to_disease_episode(self, static_index, structural_index):
        semantic = semantic_index(_tumor_condition_mapping(4112853))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        patient.tumor_type = _tumor(date=dt.date(2023, 1, 10))
        patient.tumor_assessments = [_assessment(dt.date(2023, 2, 1), recist="Stable Disease (SD)")]
        ctx = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        result = EpisodeBuilder(concepts).build(ctx)

        disease = next(r for r in result.rows if r.episode_parent_id is None)
        dynamic = next(r for r in result.rows if r.episode_parent_id is not None)
        assert dynamic.episode_concept_id == _dynamic_episode_concept(static_index, "response_recist", "Stable Disease (SD)")
        assert dynamic.episode_parent_id == disease.episode_id
