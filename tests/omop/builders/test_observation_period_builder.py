import datetime as dt

from omop_etl.omop.builders.observation_period_builder import ObservationPeriodBuilder
from omop_etl.omop.core.id_generator import sha1_bigint


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

        assert rows == [], "Builder requires valid start and end dates to emit rows"
        assert len(rows) == 0

    def test_generates_deterministic_period_id(self, mock_concepts, patient_complete):
        builder = ObservationPeriodBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        rows1 = builder.build(patient_complete, person_id)
        rows2 = builder.build(patient_complete, person_id)

        assert rows1[0].observation_period_id == rows2[0].observation_period_id

    def test_table_name(self, mock_concepts):
        builder = ObservationPeriodBuilder(mock_concepts)
        assert builder.table_name == "observation_period"
