import datetime as dt

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.location import LocationBuilder
from omop_etl.omop.builders.person import PersonBuilder
from omop_etl.omop.core.id_generator import row_id, sha256_bigint
from omop_etl.omop.models.tables import OmopTables
from tests.omop.conftest import create_patient, create_build_context


PID = "p1"
PERSON_ID = sha256_bigint("person", PID)

NORWAY = 4330432
NETHERLANDS = 4330435


def _country(concept_id: int, name: str) -> MappedConcept:
    return MappedConcept(concept_id=concept_id, concept_code="", concept_name=name, domain_id="geography", vocabulary_id="", validity="valid")


def _trial_country(*entries: tuple[str, int, str]) -> dict:
    """static-map entries: (trial_id, country concept_id, country name). The static
    resolve lowercases its input, so the keys are lowercased here too."""
    return {("trial_country", trial.lower()): _country(concept_id, name) for trial, concept_id, name in entries}


class TestLocationBuilder:
    def test_one_location_per_distinct_country(self, static_index, structural_index):
        static = {**static_index, **_trial_country(("IMPRESS", NORWAY, "Norway"), ("DRUP", NETHERLANDS, "Netherlands"))}
        concepts = ConceptLookupService(static, structural_index)
        p1 = create_patient("p1", "IMPRESS")
        p2 = create_patient("p2", "IMPRESS")  # same country as p1 -> deduped
        p3 = create_patient("p3", "DRUP")

        locations = LocationBuilder(concepts).build([p1, p2, p3])

        assert len(locations) == 2
        assert {loc.country_concept_id for loc in locations} == {NORWAY, NETHERLANDS}
        assert {loc.country_source_value for loc in locations} == {"Norway", "Netherlands"}

    def test_country_fields_and_id(self, static_index, structural_index):
        static = {**static_index, **_trial_country(("IMPRESS", NORWAY, "Norway"))}
        concepts = ConceptLookupService(static, structural_index)
        patient = create_patient(PID, "IMPRESS")

        loc = LocationBuilder(concepts).build([patient])[0]

        assert loc.location_id == row_id(OmopTables.LOCATION, NORWAY)
        assert loc.country_concept_id == NORWAY
        assert loc.country_source_value == "Norway"

    def test_unmapped_trial_yields_no_location(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)  # no trial_country mapping
        patient = create_patient(PID, "IMPRESS")

        assert LocationBuilder(concepts).build([patient]) == []


class TestPersonLocation:
    def test_person_location_id_joins_location(self, static_index, structural_index):
        static = {**static_index, **_trial_country(("IMPRESS", NORWAY, "Norway"))}
        concepts = ConceptLookupService(static, structural_index)
        patient = create_patient(PID, "IMPRESS", date_of_birth=dt.date(1980, 1, 1), sex="m")

        person_row = PersonBuilder(concepts).build(create_build_context(patient, PERSON_ID)).rows[0]
        location = LocationBuilder(concepts).build([patient])[0]

        assert person_row.location_id == location.location_id == row_id(OmopTables.LOCATION, NORWAY)

    def test_person_location_none_when_trial_unmapped(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)  # no trial_country mapping
        patient = create_patient(PID, "IMPRESS", date_of_birth=dt.date(1980, 1, 1))

        person_row = PersonBuilder(concepts).build(create_build_context(patient, PERSON_ID)).rows[0]

        assert person_row.location_id is None
