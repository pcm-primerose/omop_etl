import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.person import PersonBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.models.rows import PersonRow
from tests.omop.conftest import create_patient


class TestPersonBuilder:
    def test_all_fields_populated(self, static_index, structural_index):
        """check every PersonRow field has the expected value."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient("p1", "test", sex="m", date_of_birth=dt.date(1980, 5, 15))
        person_id = sha1_bigint("person", "p1")

        rows: list[PersonRow] = PersonBuilder(concepts).build(patient, person_id)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == person_id
        assert row.gender_concept_id == 8507
        assert row.year_of_birth == 1980
        assert row.month_of_birth == 5
        assert row.day_of_birth == 15
        assert row.birth_datetime is None
        assert row.person_source_value == "p1"
        assert row.gender_source_value == "m"
        assert row.gender_source_concept_id == 0
        assert row.race_concept_id == 0
        assert row.race_source_value is None
        assert row.race_source_concept_id == 0
        assert row.ethnicity_concept_id == 0
        assert row.ethnicity_source_value is None
        assert row.ethnicity_source_concept_id == 0

    def test_female_sex_maps_correctly(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient("p1", "test", sex="f", date_of_birth=dt.date(1990, 3, 20))

        rows: list[PersonRow] = PersonBuilder(concepts).build(patient, sha1_bigint("person", "p1"))

        assert len(rows) == 1
        assert rows[0].gender_concept_id == 8532
        assert rows[0].gender_source_value == "f"

    def test_missing_dob_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient("p1", "test", sex="m")

        rows: list[PersonRow] = PersonBuilder(concepts).build(patient, sha1_bigint("person", "p1"))

        assert rows == []

    def test_missing_sex_uses_concept_id_zero(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient("p1", "test", date_of_birth=dt.date(1975, 12, 1))

        rows: list[PersonRow] = PersonBuilder(concepts).build(patient, sha1_bigint("person", "p1"))

        assert len(rows) == 1
        assert rows[0].gender_concept_id == 0
        assert rows[0].gender_source_value is None

    def test_row_id_is_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient("p1", "test", sex="m", date_of_birth=dt.date(1980, 1, 1))
        person_id = sha1_bigint("person", "p1")

        rows_a: list[PersonRow] = PersonBuilder(concepts).build(patient, person_id)
        rows_b: list[PersonRow] = PersonBuilder(concepts).build(patient, person_id)

        assert rows_a[0].person_id == rows_b[0].person_id
