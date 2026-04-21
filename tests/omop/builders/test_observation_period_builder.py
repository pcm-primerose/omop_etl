import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.observation_period import ObservationPeriodBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import create_patient

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestObservationPeriodBuilder:
    def test_all_fields_populated(self, static_index, structural_index):
        """Check every ObservationPeriodRow field has the expected value."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 1),
            end_of_treatment_date=dt.date(2023, 6, 30),
        )

        rows = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == PERSON_ID
        assert row.observation_period_start_date == dt.date(2023, 1, 1)
        assert row.observation_period_end_date == dt.date(2023, 6, 30)
        assert row.period_type_concept_id == 32817

    def test_missing_treatment_start_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, end_of_treatment_date=dt.date(2023, 6, 30))

        rows = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_missing_both_end_dates_returns_empty(self, static_index, structural_index):
        """When both end_of_treatment_date and treatment_start_last_cycle are None, skip."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))

        rows = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_end_date_falls_back_to_last_cycle(self, static_index, structural_index):
        """When end_of_treatment_date is None, treatment_start_last_cycle is used."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 1),
            treatment_start_last_cycle=dt.date(2023, 5, 15),
        )

        rows = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].observation_period_end_date == dt.date(2023, 5, 15)

    def test_end_of_treatment_date_preferred_over_last_cycle(self, static_index, structural_index):
        """When both end dates exist, end_of_treatment_date takes priority."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 1),
            end_of_treatment_date=dt.date(2023, 6, 30),
            treatment_start_last_cycle=dt.date(2023, 5, 15),
        )

        rows = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].observation_period_end_date == dt.date(2023, 6, 30)

    def test_row_id_is_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            PID,
            TRIAL,
            treatment_start_date=dt.date(2023, 1, 1),
            end_of_treatment_date=dt.date(2023, 6, 30),
        )

        rows_a = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)
        rows_b = ObservationPeriodBuilder(concepts).build(patient, PERSON_ID)

        assert rows_a[0].observation_period_id == rows_b[0].observation_period_id

    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert ObservationPeriodBuilder(concepts).table_name == "observation_period"
