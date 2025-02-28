import pytest
import polars as pl
from src.harmonization.harmonizers.impress import ImpressHarmonizer
from src.harmonization.harmonizers.base import BaseHarmonizer


@pytest.fixture
def subject_id_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPR-X-0005_1"],
        }
    )


@pytest.fixture
def cohort_name_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPR-X-0005_1"],
            "COH_COHORTNAME": ["BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib", "NA", "NA", "", "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"],
        }
    )


class TestImpressHarmonizer:

    """Tests for IMRPESS harmonization"""

    def test_impress_subject_id_processing(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()

        assert len(harmonizer.patient_data) == 5

        expected_ids = ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPR-X-0005_1"]
        assert set(expected_ids) == set(harmonizer.patient_data)

        for patient_id, patient_data in harmonizer.patient_data.items():
            assert patient_data.patient_id == patient_id
            assert patient_data.trial_id == "IMPRESS_TEST"

    def test_impress_cohort_name_processing(self, cohort_name_fixture):
        harmonizer = ImpressHarmonizer(data=cohort_name_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()
        harmonizer._process_cohort_name()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].cohort_name == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].cohort_name is None or harmonizer.patient_data["IMPRESS-X_0002_1"].cohort_name == ""
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].cohort_name is None or harmonizer.patient_data["IMPRESS-X_0003_1"].cohort_name == ""
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].cohort_name == ""
        assert harmonizer.patient_data["IMPR-X-0005_1"].cohort_name == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"

    def test_basic_inheritance(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")

        assert isinstance(harmonizer, BaseHarmonizer)

        assert harmonizer.data is subject_id_fixture
        assert harmonizer.trial_id == "IMPRESS_TEST"

        assert hasattr(harmonizer, "patient_data")
        assert isinstance(harmonizer.patient_data, dict)
