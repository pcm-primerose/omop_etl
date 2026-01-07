import datetime as dt

import pytest

from omop_etl.omop.builders.person_builder import PersonBuilder
from omop_etl.omop.builders.observation_period_builder import ObservationPeriodBuilder
from omop_etl.omop.builders.cdm_source_builder import CdmSourceBuilder
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


class TestObservationPeriodBuilder:
    def test_builds_observation_period(self, mock_concepts, patient_complete):
        builder = ObservationPeriodBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        rows = builder.build(patient_complete, person_id)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == person_id
        assert row.observation_period_start_date == dt.date(2023, 1, 1)
        assert row.observation_period_end_date == dt.date(2023, 6, 30)
        assert row.period_type_concept_id == 32817  # ecrf type

    def test_returns_empty_when_treatment_start_missing(self, mock_concepts, patient_missing_treatment_start):
        builder = ObservationPeriodBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_missing_treatment_start.patient_id)

        rows = builder.build(patient_missing_treatment_start, person_id)

        assert rows == []

    def test_handles_missing_end_date(self, mock_concepts, patient_minimal):
        builder = ObservationPeriodBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_minimal.patient_id)

        rows = builder.build(patient_minimal, person_id)

        assert len(rows) == 1
        assert rows[0].observation_period_start_date == dt.date(2024, 1, 1)
        assert rows[0].observation_period_end_date is None

    def test_generates_deterministic_period_id(self, mock_concepts, patient_complete):
        builder = ObservationPeriodBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        rows1 = builder.build(patient_complete, person_id)
        rows2 = builder.build(patient_complete, person_id)

        assert rows1[0].observation_period_id == rows2[0].observation_period_id

    def test_table_name(self, mock_concepts):
        builder = ObservationPeriodBuilder(mock_concepts)
        assert builder.table_name == "observation_period"


class TestCdmSourceBuilder:
    def test_builds_cdm_source(self, mock_concepts):
        builder = CdmSourceBuilder(mock_concepts)

        row = builder.build()

        assert row.cdm_source_name == "test ETL"
        assert row.cdm_holder == "PRIME-ROSE"
        assert row.cdm_version == "v5.4"
        assert row.cdm_version_concept_id == 756265
        assert row.vocabulary_version == "0"  # str(concept_id)
        assert row.source_release_date == dt.date.today()
