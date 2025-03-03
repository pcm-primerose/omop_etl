import pytest
import polars as pl
from src.harmonization.harmonizers.impress import ImpressHarmonizer
from src.harmonization.harmonizers.base import BaseHarmonizer
from src.utils.helpers import date_parser_helper


@pytest.fixture
def subject_id_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"],
        }
    )


@pytest.fixture
def cohort_name_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"],
            "COH_COHORTNAME": ["BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib", "NA", "NA", "", "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"],
        }
    )


@pytest.fixture
def age_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"],
            "DM_BRTHDAT": ["1900-06-02", "1950", "2000-02-14", "1970", "1990-12-07"],
            "TR_TRC1_DT": ["1990-01-30", "1990-01-01", "2020-03-25", "2000", "2000-01"],
        }
    )


@pytest.fixture
def gender_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"],
            "DM_SEX": ["Female", "Male", "female", "male", "error"],
        }
    )


@pytest.fixture
def tumor_type_fixture():
    # note that COH_COHTTYPE/CD and COH_COHTTYPE__2/CD are mutually exlusive
    # two separate dropdown lists
    return pl.DataFrame(
        data={
            "SubjectId": ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"],
            "COH_ICD10COD": ["C30", "C40.50", "C07", "C70.1", "C23.20"],
            "COH_ICD10DES": ["tumor1", "CRC", "tumor2", "tumor3", "tumor4"],
            "COH_COHTTYPE": ["tumor1_subtype1", "NA", "tumor2_subtype1", "tumor3_subtype1", "NA"],
            "COH_COHTTYPECD": ["50", "NA", "70", "10", "NA"],
            "COH_COHTTYPE__2": ["NA", "CRC_subtype", "NA", "NA", "tumor4_subtype1"],
            "COH_COHTTYPE__2CD": ["NA", "40", "NA", "NA", "30"],
            "COH_COHTT": ["tumor1_subtype2", "NA", "tumor2_subtype2", "NA", "NA"],
            "COH_COHTTOSP": ["tumor1_subtype3", "NA", "NA", "tumor3_subtype2", "tumor4_subtype2"],
        }
    )


class TestImpressHarmonizer:

    """Tests for IMRPESS harmonization"""

    def test_impress_subject_id_processing(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()

        assert len(harmonizer.patient_data) == 5

        expected_ids = ["IMPRESS-X_0001_1", "IMPRESS-X_0002_1", "IMPRESS-X_0003_1", "IMPRESS-X_0004_1", "IMPRESS-X_0005_1"]
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
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].cohort_name == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"

    def test_gender_processing(self, gender_fixture):
        harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()
        harmonizer._process_gender()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].sex == "female"
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].sex == "male"
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].sex == "female"
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].sex == "male"
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].sex is None

    def test_age_processing(self, age_fixture):
        harmonizer = ImpressHarmonizer(data=age_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()
        harmonizer._process_age()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].age == 90
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].age == 40
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].age == 20
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].age == 30
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].age == 9

    def test_basic_inheritance(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")

        assert isinstance(harmonizer, BaseHarmonizer)

        assert harmonizer.data is subject_id_fixture
        assert harmonizer.trial_id == "IMPRESS_TEST"

        assert hasattr(harmonizer, "patient_data")
        assert isinstance(harmonizer.patient_data, dict)


def test_date_parser_helper():
    df = pl.DataFrame({"dates": ["1900-02-02", "1900", "1950-06"]})

    result_colname_as_str = df.with_columns(parsed_dates=date_parser_helper("dates"))

    result_column = df.with_columns(parsed_dates=date_parser_helper(pl.col("dates")))

    assert result_colname_as_str["parsed_dates"][0] == "1900-02-02"
    assert result_colname_as_str["parsed_dates"][1] == "1900-01-01"
    assert result_colname_as_str["parsed_dates"][2] == "1950-06-01"
    assert result_column["parsed_dates"][0] == "1900-02-02"
    assert result_column["parsed_dates"][1] == "1900-01-01"
    assert result_column["parsed_dates"][2] == "1950-06-01"
