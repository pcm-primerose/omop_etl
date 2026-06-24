import datetime as dt

from tests.omop.conftest import create_patient, create_build_context, semantic_index, mapping, concept
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
from omop_etl.omop.core.linkage import BuildResult, SourceReference
from omop_etl.omop.models.rows import EpisodeRow
from omop_etl.omop.models.tables import OmopTables

PID = "p1"
TRIAL = "test"
PERSON_ID = sha256_bigint("person", PID)

# concept ids seeded in conftest.structural_index
TREATMENT_REGIMEN = 32531
TREATMENT_CYCLE = 32532
ECRF = 32809


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


def _of_kind(rows: tuple[EpisodeRow, ...], episode_concept_id: int) -> list[EpisodeRow]:
    """The episode rows of one kind (regimen / cycle), since one build emits both."""
    return [r for r in rows if r.episode_concept_id == episode_concept_id]


class TestEpisodeBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert EpisodeBuilder(concepts).table_name == "episode"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_regimen_concept_skips_all(self, static_index, structural_index):
        # treatment_regimen is the line's defining concept; without it the builder
        # emits nothing (a 0 here is meaningless). Cycles parent to the regimen, so
        # they are skipped too even though treatment_cycle is still present.
        index = {k: v for k, v in structural_index.items() if k != "treatment_regimen"}
        concepts = ConceptLookupService(static_index, index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()


class TestTreatmentRegimenEpisodes:
    def test_one_line_spans_its_cycles(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20), name="Erivedge"),
            _cycle(1, 2, dt.date(2023, 2, 1), dt.date(2023, 2, 20), name="Erivedge"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        regimens = _of_kind(result.rows, TREATMENT_REGIMEN)
        assert len(regimens) == 1
        ep = regimens[0]
        assert ep.person_id == PERSON_ID
        assert ep.episode_concept_id == TREATMENT_REGIMEN
        assert ep.episode_type_concept_id == ECRF
        assert ep.episode_object_concept_id == 0  # no REGIMEN mapping seeded
        assert ep.episode_number == 1
        assert ep.episode_start_date == dt.date(2023, 1, 1)
        assert ep.episode_end_date == dt.date(2023, 2, 20)
        assert ep.episode_start_datetime == dt.datetime(2023, 1, 1)
        assert ep.episode_end_datetime == dt.datetime(2023, 2, 20)
        assert ep.episode_source_value == "Erivedge"

    def test_end_date_falls_back_to_cycle_start(self, static_index, structural_index):
        # a cycle with no end_date contributes its own start to the line span
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20)),
            _cycle(1, 2, dt.date(2023, 3, 1), end=None),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert _of_kind(result.rows, TREATMENT_REGIMEN)[0].episode_end_date == dt.date(2023, 3, 1)

    def test_one_episode_per_line(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1)),
            _cycle(2, 1, dt.date(2023, 6, 1)),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        regimens = _of_kind(result.rows, TREATMENT_REGIMEN)
        assert len(regimens) == 2
        assert [r.episode_number for r in regimens] == [1, 2]  # sorted by line
        assert len({r.episode_id for r in regimens}) == 2

    def test_combination_line_does_not_raise(self, static_index, structural_index):
        # a line whose cycles carry distinct drug names (e.g. Piqray, Fulvestrant)
        # must not raise; the regimen logs and keeps the most common drug name
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), name="Piqray"),
            _cycle(1, 2, dt.date(2023, 1, 1), name="Fulvestrant"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        regimens = _of_kind(result.rows, TREATMENT_REGIMEN)
        assert len(regimens) == 1
        assert regimens[0].episode_source_value in {"Piqray", "Fulvestrant"}
        assert regimens[0].episode_object_concept_id == 0

    def test_publishes_episode_ref_per_line(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]
        ctx = create_build_context(patient, PERSON_ID)

        rows = EpisodeBuilder(concepts).build_and_populate(ctx)

        refs = ctx.resolve_rows(
            OmopTables.EPISODE,
            SourceReference(PID, Patient.Collections.TREATMENT_CYCLES, (1,)),
        )
        regimen = _of_kind(rows, TREATMENT_REGIMEN)[0]
        assert len(refs) == 1
        assert refs[0].row_id == regimen.episode_id


class TestTreatmentCycleEpisodes:
    def test_one_episode_per_cycle_number(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20)),
            _cycle(1, 2, dt.date(2023, 2, 1), dt.date(2023, 2, 20)),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        cycles = _of_kind(result.rows, TREATMENT_CYCLE)
        assert [c.episode_number for c in cycles] == [1, 2]  # sorted by cycle number
        assert len({c.episode_id for c in cycles}) == 2
        # each cycle spans its own component's dates, not the whole line
        first = cycles[0]
        assert first.episode_concept_id == TREATMENT_CYCLE
        assert first.episode_type_concept_id == ECRF
        assert first.episode_start_date == dt.date(2023, 1, 1)
        assert first.episode_end_date == dt.date(2023, 1, 20)
        assert first.episode_start_datetime == dt.datetime(2023, 1, 1)

    def test_cycle_inherits_regimen_parent_and_object(self, static_index, structural_index):
        # parent_id is the line's regimen episode_id; object concept is inherited
        # from the regimen (here a real REGIMEN semantic match, so non-zero)
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

        regimen = _of_kind(result.rows, TREATMENT_REGIMEN)[0]
        cycles = _of_kind(result.rows, TREATMENT_CYCLE)
        assert regimen.episode_object_concept_id == 35803140
        assert len(cycles) == 2
        assert all(c.episode_parent_id == regimen.episode_id for c in cycles)
        assert all(c.episode_object_concept_id == 35803140 for c in cycles)

    def test_combination_cycle_collapses_to_one_episode(self, static_index, structural_index):
        # a combination drug splits into per-ingredient components sharing one
        # (treatment_number, cycle_number) -> a single cycle episode, not one per drug
        concepts = ConceptLookupService(static_index, structural_index)
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

        cycles = _of_kind(result.rows, TREATMENT_CYCLE)
        assert len(cycles) == 1
        assert cycles[0].episode_number == 1
        assert cycles[0].episode_source_value == "Phesgo (Pertuzumab and Trastuzumab)"
        assert cycles[0].episode_start_date == dt.date(2023, 1, 1)
        assert cycles[0].episode_end_date == dt.date(2023, 1, 20)

    def test_distinct_regimen_and_cycle_ids(self, static_index, structural_index):
        # regimen and cycle of the same line hash to distinct episode_ids
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len({r.episode_id for r in result.rows}) == len(result.rows)

    def test_cycles_skipped_without_cycle_concept(self, static_index, structural_index):
        # treatment_cycle missing but treatment_regimen present -> regimen rows only
        index = {k: v for k, v in structural_index.items() if k != "treatment_cycle"}
        concepts = ConceptLookupService(static_index, index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert _of_kind(result.rows, TREATMENT_REGIMEN)
        assert _of_kind(result.rows, TREATMENT_CYCLE) == []
