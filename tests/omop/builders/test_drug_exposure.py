import datetime as dt

from omop_etl.omop.builders.drug_exposure_builder import DrugExposureBuilder
from omop_etl.omop.core.id_generator import sha1_bigint


class TestDrugExposureBuilder:
    def test_builds_drug_exposure(self, mock_concepts, patient_complete):
        builder = DrugExposureBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_complete.patient_id)

        rows = builder.build(patient_complete, person_id)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == person_id
        assert row.drug_exposure_start_date == dt.date(2024, 8, 20) # Exposure during one cycle with 28 days 
        assert row.drug_exposure_end_date == dt.date(2024, 9, 16)
        assert row.drug_type_concept_id == 32809  # Case report form

    def test_returns_empty_when_treatment_start_missing(self, mock_concepts, patient_missing_treatment_start):
        """
        Tests treatment start date is not empty
        """
        builder = DrugExposureBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_missing_treatment_start.patient_id)

        rows = builder.build(patient_missing_treatment_start, person_id)

        assert rows == []

    def test_handles_missing_end_date(self, mock_concepts, patient_minimal):
        """
        Tests treatment end date is not empty
        """
        builder = DrugExposureBuilder(mock_concepts)
        person_id = sha1_bigint("person", patient_minimal.patient_id)

        rows = builder.build(patient_minimal, person_id)

        assert rows == [], "Builder requires valid start and end dates to emit rows"
        assert len(rows) == 0

    def test_table_name(self, mock_concepts):
        """
        Tests table name
        """
        builder = DrugExposureBuilder(mock_concepts)
        assert builder.table_name == "drug_exposure"

        """
        Add more tests
        """