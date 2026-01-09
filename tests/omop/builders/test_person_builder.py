import pytest

from omop_etl.omop.builders.person_builder import PersonBuilder
from omop_etl.omop.core.id_generator import sha1_bigint


class TestPersonBuilder:
    def test_builds_person_row(self, mock_concepts, patient_complete):
        builder = PersonBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        rows = builder.build(patient_complete, person_id)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == person_id
        assert row.gender_concept_id == 8507  # Male
        assert row.year_of_birth == 1980
        assert row.month_of_birth == 5
        assert row.day_of_birth == 15
        assert row.person_source_value == "P001"
        assert row.gender_source_value == "M"

    def test_builds_female_person(self, mock_concepts, patient_female):
        builder = PersonBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_female.patient_id)

        rows = builder.build(patient_female, person_id)

        assert len(rows) == 1
        assert rows[0].gender_concept_id == 8532  # Female
        assert rows[0].gender_source_value == "F"

    def test_returns_empty_when_dob_missing(self, mock_concepts, patient_missing_dob):
        builder = PersonBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_missing_dob.patient_id)

        rows = builder.build(patient_missing_dob, person_id)

        assert rows == []

    def test_handles_missing_sex(self, mock_concepts, patient_no_sex):
        builder = PersonBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_no_sex.patient_id)

        rows = builder.build(patient_no_sex, person_id)

        assert len(rows) == 1
        assert rows[0].gender_concept_id == 0
        assert rows[0].gender_source_value is None

    def test_raises_on_unknown_sex_mapping(self, mock_concepts, patient_complete):
        patient_complete.sex = "UNKNOWN"
        builder = PersonBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        with pytest.raises(ValueError, match="Unknown sex mapping"):
            builder.build(patient_complete, person_id)

    def test_table_name(self, mock_concepts):
        builder = PersonBuilder(mock_concepts)
        assert builder.table_name == "person"
