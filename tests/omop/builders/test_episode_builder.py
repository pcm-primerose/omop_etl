import datetime as dt

from tests.omop.conftest import create_patient, create_build_context
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
from omop_etl.omop.core.linkage import BuildResult, SourceReference
from omop_etl.omop.models.tables import OmopTables

PID = "p1"
TRIAL = "test"
PERSON_ID = sha256_bigint("person", PID)

# concept ids seeded in conftest.structural_index
TREATMENT_REGIMEN = 32531
ECRF = 32809


def _cycle(
    treatment_number: int,
    cycle_number: int,
    start: dt.date,
    end: dt.date | None = None,
    name: str = "drugX",
) -> TreatmentCycleComponent:
    """Scaffolding: a treatment-cycle component. The treatment_number, name and
    dates are the test subject, passed in by each test."""
    c = TreatmentCycleComponent(patient_id=PID)
    c.source_treatment_name = name
    c.treatment_number = treatment_number
    c.cycle_number = cycle_number
    c.start_date = start
    c.end_date = end
    return c


class TestEpisodeBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert EpisodeBuilder(concepts).table_name == "episode"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_missing_regimen_concept_skips(self, static_index, structural_index):
        # episode_concept_id is the defining field; with no 'treatment_regimen'
        # concept the builder skips rather than emit concept_id=0 garbage.
        index = {k: v for k, v in structural_index.items() if k != "treatment_regimen"}
        concepts = ConceptLookupService(static_index, index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [_cycle(1, 1, dt.date(2023, 1, 1))]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()


class TestEpisodeRows:
    def test_one_line_spans_its_cycles(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), dt.date(2023, 1, 20), name="Erivedge"),
            _cycle(1, 2, dt.date(2023, 2, 1), dt.date(2023, 2, 20), name="Erivedge"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        ep = result.rows[0]
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

        assert result.rows[0].episode_end_date == dt.date(2023, 3, 1)

    def test_one_episode_per_line(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1)),
            _cycle(2, 1, dt.date(2023, 6, 1)),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 2
        assert [r.episode_number for r in result.rows] == [1, 2]  # sorted by line
        assert len({r.episode_id for r in result.rows}) == 2

    def test_combination_line_does_not_raise(self, static_index, structural_index):
        # two distinct study drugs in one line (e.g. Piqray + Fulvestrant) - a real
        # data shape; must not raise. The builder logs and keeps the most common drug.
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.treatment_cycles = [
            _cycle(1, 1, dt.date(2023, 1, 1), name="Piqray"),
            _cycle(1, 2, dt.date(2023, 1, 1), name="Fulvestrant"),
        ]

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].episode_source_value in {"Piqray", "Fulvestrant"}
        assert result.rows[0].episode_object_concept_id == 0

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
        assert len(refs) == 1
        assert refs[0].row_id == rows[0].episode_id
