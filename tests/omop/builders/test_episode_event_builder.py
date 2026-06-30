import datetime as dt
import pytest

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.builders.drug_exposure import DrugExposureBuilder
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.builders.episode_event import EpisodeEventBuilder
from omop_etl.omop.builders.measurement import MeasurementBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
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


def _cdm_field(static_index: dict, target_pk: str) -> int:
    """The cdm_field concept for a table's PK column."""
    return static_index[("cdm_field", target_pk)].concept_id


def _assessment(date: dt.date, recist: str) -> TumorAssessment:
    ta = TumorAssessment(patient_id=PID)
    ta.assessment_type = "RECIST 1.1"
    ta.date = date
    ta.event_id = date.isoformat()
    ta.recist_response = recist
    return ta


def _cycle(treatment_number: int, cycle_number: int, start: dt.date) -> TreatmentCycleComponent:
    c = TreatmentCycleComponent(patient_id=PID)
    c.source_treatment_name = "drugX"
    c.treatment_number = treatment_number
    c.cycle_number = cycle_number
    c.start_date = start
    return c


def _component(treatment_number: int, cycle_number: int, start: dt.date, ingredient: str) -> TreatmentCycleComponent:
    """One ingredient of a combination drug."""
    c = TreatmentCycleComponent(patient_id=PID)
    c.source_treatment_name = "Phesgo (Pertuzumab and Trastuzumab)"
    c.ingredient_name = ingredient
    c.treatment_number = treatment_number
    c.cycle_number = cycle_number
    c.start_date = start
    return c


@pytest.fixture
def cycle_only_structural(structural_index):
    """structural_index without the regimen concept."""
    return {k: v for k, v in structural_index.items() if k != "treatment_regimen"}


class TestEpisodeEventBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert EpisodeEventBuilder(concepts).table_name == "episode_event"


class TestTreatmentCycleDrugLinks:
    def test_combination_cycle_drugs_link_to_one_cycle_episode(self, static_index, cycle_only_structural):
        """
        Two drug components in one cycle (combination): both drug_exposure rows
        link to the single cycle episode for (treatment_number, cycle_number).
        """
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        c1 = _component(1, 1, dt.date(2023, 1, 1), ingredient="Pertuzumab")
        c2 = _component(1, 1, dt.date(2023, 1, 1), ingredient="Trastuzumab")
        patient.treatment_cycles = [c1, c2]
        ctx = create_build_context(patient, PERSON_ID)

        # populate context with published rows
        drug_rows = DrugExposureBuilder(concepts).build_and_populate(ctx)
        episode_rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert len(episode_rows) == 1  # just the cycle episode (regimen out of scope)
        assert len(result.rows) == 2
        assert all(r.episode_id == episode_rows[0].episode_id for r in result.rows)
        assert {r.event_id for r in result.rows} == {d.drug_exposure_id for d in drug_rows}
        assert all(r.episode_event_field_concept_id == _cdm_field(static_index, "drug_exposure.drug_exposure_id") for r in result.rows)

    def test_separate_cycles_link_to_separate_episodes(self, static_index, cycle_only_structural):
        """Two cycles (cycle 1 and cycle 2): each drug links to its own cycle episode."""
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1)), _cycle(1, 2, dt.date(2023, 2, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        drug_rows = DrugExposureBuilder(concepts).build_and_populate(ctx)
        EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert len(result.rows) == 2
        assert {r.event_id for r in result.rows} == {d.drug_exposure_id for d in drug_rows}

        # the two drugs link to two different cycle episodes
        episode_by_event = {r.event_id: r.episode_id for r in result.rows}
        assert len(set(episode_by_event.values())) == 2

    def test_no_drug_exposure_no_events(self, static_index, cycle_only_structural):
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        # cycle episode published, but DrugExposureBuilder never run so no drug rows
        EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert result.rows == ()

    def test_no_episode_no_events(self, static_index, cycle_only_structural):
        concepts = ConceptLookupService(static_index, cycle_only_structural)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        # drug published, but no episode published (EpisodeBuilder not run)
        DrugExposureBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert result.rows == ()


class TestDiseaseConditionLinks:
    def test_links_disease_episode_to_condition(self, static_index, structural_index):
        """The Disease Episode links to the primary-cancer condition_occurrence row."""
        semantic = semantic_index(
            mapping(
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                "Melanoma",
                concept(4112853, "condition"),
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.main_tumor_type = "Melanoma"
        tumor.date = dt.date(2023, 1, 10)
        patient.tumor_type = tumor
        ctx = create_build_context(patient, PERSON_ID)

        # ConditionOccurrenceBuilder publishes the cancer condition, EpisodeBuilder the Disease Episode
        condition_rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)
        episode_rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert len(episode_rows) == 1  # just the Disease Episode (no cycles, no assessments)
        assert len(result.rows) == 1
        ev = result.rows[0]
        assert ev.episode_id == episode_rows[0].episode_id
        assert ev.event_id == condition_rows[0].condition_occurrence_id
        assert ev.episode_event_field_concept_id == _cdm_field(static_index, "condition_occurrence.condition_occurrence_id")


class TestDiseaseDynamicMeasurementLinks:
    def test_each_run_links_to_its_response_measurement(self, static_index, structural_index):
        """
        Two single-assessment runs (SD then PD): each dynamic episode links to its
        assessment's response measurement row.
        """
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _assessment(dt.date(2023, 1, 1), recist="Stable Disease (SD)"),
            _assessment(dt.date(2023, 2, 1), recist="Progressive Disease (PD)"),
        ]
        ctx = create_build_context(patient, PERSON_ID)

        # MeasurementBuilder publishes the response rows, EpisodeBuilder the dynamic episodes
        MeasurementBuilder(concepts).build_and_populate(ctx)
        episode_rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert len(result.rows) == 2
        assert all(r.episode_event_field_concept_id == _cdm_field(static_index, "measurement.measurement_id") for r in result.rows)
        # all upstream episode rows are dynamic, so the events point at exactly them
        assert {r.episode_id for r in result.rows} == {r.episode_id for r in episode_rows}

    def test_run_links_to_all_its_assessments_measurements(self, static_index, structural_index):
        """
        A run of two same-status (SD) assessments collapses to one dynamic episode,
        both assessments' measurement rows link to that single episode.
        """
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.tumor_assessments = [
            _assessment(dt.date(2023, 1, 1), recist="Stable Disease (SD)"),
            _assessment(dt.date(2023, 2, 1), recist="Stable Disease (SD)"),
        ]
        ctx = create_build_context(patient, PERSON_ID)

        measurement_rows = MeasurementBuilder(concepts).build_and_populate(ctx)
        episode_rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        result = EpisodeEventBuilder(concepts).build(ctx)

        assert len(episode_rows) == 1  # two SD assessments are one run: one episode
        assert len(result.rows) == 2
        assert all(r.episode_id == episode_rows[0].episode_id for r in result.rows)
        assert {r.event_id for r in result.rows} == {m.measurement_id for m in measurement_rows}
