import datetime as dt

from omop_etl.omop.service import OmopService


class TestOmopService:
    def test_builds_all_tables(self, mock_concepts, patient_complete):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete])

        assert len(tables.person) == 1
        assert len(tables.observation_period) == 1
        assert tables.cdm_source is not None

    def test_builds_multiple_patients(self, mock_concepts, patient_complete, patient_female):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete, patient_female])

        assert len(tables.person) == 2
        assert len(tables.observation_period) == 2
        assert tables.cdm_source is not None

    def test_skips_patient_missing_dob(self, mock_concepts, patient_complete, patient_missing_dob):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete, patient_missing_dob])

        # patient_missing_dob should be skipped for Person but not affect others
        assert len(tables.person) == 1
        # patient_missing_dob has treatment_start, so observation_period is built
        assert len(tables.observation_period) == 2

    def test_skips_patient_missing_treatment_start(self, mock_concepts, patient_complete, patient_missing_treatment_start):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete, patient_missing_treatment_start])

        # patient_missing_treatment_start has DOB, so Person is built
        assert len(tables.person) == 2
        # but no treatment_start, so ObservationPeriod is skipped
        assert len(tables.observation_period) == 1

    def test_empty_patients_list(self, mock_concepts):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([])

        assert len(tables.person) == 0
        assert len(tables.observation_period) == 0
        assert tables.cdm_source is not None  # always created

    def test_person_ids_are_unique(self, mock_concepts, patient_complete, patient_female):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete, patient_female])

        person_ids = [row.person_id for row in tables.person]
        assert len(person_ids) == len(set(person_ids))

    def test_person_ids_are_deterministic(self, mock_concepts, patient_complete):
        service = OmopService(concepts=mock_concepts)

        tables1 = service.build([patient_complete])
        tables2 = service.build([patient_complete])

        assert tables1.person[0].person_id == tables2.person[0].person_id

    def test_table_access_via_property(self, mock_concepts, patient_complete):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete])

        # typed property access
        assert tables.person[0].person_source_value == "P001"
        assert tables.observation_period[0].observation_period_start_date == dt.date(2023, 1, 1)
        assert tables.cdm_source.cdm_holder == "PRIME-ROSE"

    def test_table_access_via_getitem(self, mock_concepts, patient_complete):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete])

        assert len(tables["person"]) == 1
        assert len(tables["observation_period"]) == 1
        assert len(tables["cdm_source"]) == 1

    def test_table_get_with_default(self, mock_concepts, patient_complete):
        service = OmopService(concepts=mock_concepts)

        tables = service.build([patient_complete])

        # non-existent table returns default
        assert tables.get("condition_occurrence") == []
        assert tables.get("drug_exposure", []) == []

    def test_deduplicates_same_patient_twice(self, mock_concepts, patient_complete):
        """Building from same patient twice should dedupe by natural key."""
        service = OmopService(concepts=mock_concepts)

        # build twice
        tables = service.build([patient_complete, patient_complete])

        # should only have one person row, deduped by person_id
        assert len(tables.person) == 1
        assert len(tables.observation_period) == 1
