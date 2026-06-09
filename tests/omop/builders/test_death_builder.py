import datetime as dt

from tests.omop.conftest import create_patient, create_build_context
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.death import DeathBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import BuildResult


PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestDeathBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert DeathBuilder(concepts).table_name == "death"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = DeathBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


class TestDeathRows:
    def test_death_builder_populate_rows(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.date_of_death = dt.date(1900, 1, 1)

        result = DeathBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.person_id == PERSON_ID
        assert row.death_date == dt.date(1900, 1, 1)
        assert row.death_type_concept_id == 32809
        assert row.death_datetime == dt.datetime(1900, 1, 1)

    def test_missing_date_of_date_returns_no_rows(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.date_of_death = None

        result = DeathBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
