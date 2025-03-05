import pytest
import polars as pl
import datetime as dt

from src.harmonization.datamodels import TumorType, StudyDrugs, Patient
from src.harmonization.harmonizers.impress import ImpressHarmonizer
from src.harmonization.harmonizers.base import BaseHarmonizer
from src.utils.helpers import date_parser_helper


@pytest.fixture
def subject_id_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
        }
    )


@pytest.fixture
def cohort_name_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_COHORTNAME": [
                "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib",
                "NA",
                "NA",
                "",
                "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab",
            ],
        }
    )


@pytest.fixture
def age_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "DM_BRTHDAT": ["1900-06-02", "1950", "2000-02-14", "1970", "1990-12-07"],
            "TR_TRC1_DT": ["1990-01-30", "1990-01-01", "2020-03-25", "2000", "2000-01"],
        }
    )


@pytest.fixture
def gender_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "DM_SEX": ["Female", "Male", "female", "male", "error"],
        }
    )


@pytest.fixture
def tumor_type_fixture():
    # note that COH_COHTTYPE/CD and COH_COHTTYPE__2/CD are mutually exlusive
    # two separate dropdown lists
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_ICD10COD": ["C30", "C40.50", "C07", "C70.1", "C23.20"],
            "COH_ICD10DES": ["tumor1", "CRC", "tumor2", "tumor3", "tumor4"],
            "COH_COHTTYPE": [
                "tumor1_subtype1",
                "NA",
                "tumor2_subtype1",
                "tumor3_subtype1",
                "NA",
            ],
            "COH_COHTTYPECD": ["50", "NA", "70", "10", "NA"],
            "COH_COHTTYPE__2": ["NA", "CRC_subtype", "NA", "NA", "tumor4_subtype1"],
            "COH_COHTTYPE__2CD": ["NA", "40", "NA", "NA", "30"],
            "COH_COHTT": ["tumor1_subtype2", "NA", "tumor2_subtype2", "NA", "NA"],
            "COH_COHTTOSP": [
                "tumor1_subtype3",
                "NA",
                "NA",
                "tumor3_subtype2",
                "tumor4_subtype2",
            ],
        }
    )


@pytest.fixture
def study_drugs_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_COHALLO1": ["NA", "some drug", "mismatch_1", "NA", "partial"],
            "COH_COHALLO1CD": ["NA", "99", "10", "NA", "NA"],
            "COH_COHALLO1__2": ["Traztuzumab", "NA", "NA", "mismatch_2", "NA"],
            "COH_COHALLO1__2CD": ["31", "NA", "NA", "50", "NA"],
            "COH_COHALLO2": ["NA", "some drug 2", "NA", "mismatch_2_1", "NA"],
            "COH_COHALLO2CD": ["NA", "1", "NA", "60", "NA"],
            "COH_COHALLO2__2": ["Tafinlar", "NA", "mismatch_1_2", "NA", "NA"],
            "COH_COHALLO2__2CD": ["10", "NA", "12", "NA", "5"],
        }
    )


@pytest.fixture
def biomarker_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_GENMUT1": [
                "BRAF activating mutations",
                "NA",
                "BRCA1 inactivating mutation",
                "SDHAF2 mutation",
            ],
            "COH_GENMUT1CD": ["21", "NA", 2, -1, "NA"],
            "COH_COHCTN": [
                "BRAF Non-V600activating mutations",
                "some info",
                "BRCA1 stop-gain del exon 11",
                "more info",
                "NA",
            ],
            "COH_COHTMN": [
                "BRAF Non-V600 activating mutations" "some target",
                "BRCA1 stop-gain deletion",
                "NA",
                "NA",
            ],
        }
    )


@pytest.fixture
def date_of_death_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "EOS_DEATHDTC": [
                "1990-10-02",
                "1961-09-12",
                "NA",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": [
                "1990-10-02",
                "2016-09-12",
                "1900-01-01",
                "NA",
                "invalid date",
            ],
        }
    )


@pytest.fixture
def lost_to_followup_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "FU_FUPALDAT": [
                "1990-10-02",
                "NA",
                "1800-01-01",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": ["NA", "206-09-12", "NA", "NA", "invalid date"],
            "FU_FUPSST": ["Alive", "Death", "lost to follow up", "alive", "NA"],
            "FU_FUPSSTCD": ["1", "2", "3", "NA", "NA"],
        }
    )


class TestImpressHarmonizer:

    """Tests for IMRPESS harmonization"""

    def test_impress_subject_id_processing(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")
        harmonizer._process_patient_id()

        assert len(harmonizer.patient_data) == 5

        expected_ids = [
            "IMPRESS-X_0001_1",
            "IMPRESS-X_0002_1",
            "IMPRESS-X_0003_1",
            "IMPRESS-X_0004_1",
            "IMPRESS-X_0005_1",
        ]
        assert set(expected_ids) == set(harmonizer.patient_data)

        for patient_id, patient_data in harmonizer.patient_data.items():
            assert patient_data.patient_id == patient_id
            assert patient_data.trial_id == "IMPRESS_TEST"

    def test_impress_cohort_name_processing(self, cohort_name_fixture):
        harmonizer = ImpressHarmonizer(
            data=cohort_name_fixture, trial_id="IMPRESS_TEST"
        )

        for subject_id in (
            cohort_name_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_cohort_name()

        assert (
            harmonizer.patient_data["IMPRESS-X_0001_1"].cohort_name
            == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0002_1"].cohort_name is None
            or harmonizer.patient_data["IMPRESS-X_0002_1"].cohort_name == ""
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0003_1"].cohort_name is None
            or harmonizer.patient_data["IMPRESS-X_0003_1"].cohort_name == ""
        )
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].cohort_name == ""
        assert (
            harmonizer.patient_data["IMPRESS-X_0005_1"].cohort_name
            == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"
        )

    def test_gender_processing(self, gender_fixture):
        harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="IMPRESS_TEST")

        for subject_id in (
            gender_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_gender()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].sex == "female"
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].sex == "male"
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].sex == "female"
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].sex == "male"
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].sex is None

    def test_age_processing(self, age_fixture):
        harmonizer = ImpressHarmonizer(data=age_fixture, trial_id="IMPRESS_TEST")

        for subject_id in (
            age_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_age()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].age == 89
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].age == 39
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].age == 20
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].age == 30
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].age == 9

    def test_tumor_processing(self, tumor_type_fixture):
        harmonizer = ImpressHarmonizer(data=tumor_type_fixture, trial_id="IMPRESS_TEST")

        for subject_id in (
            tumor_type_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_tumor_type()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].tumor_type == TumorType(
            icd10_code="C30",
            icd10_description="tumor1",
            tumor_type="tumor1_subtype1",
            tumor_type_code=50,
            cohort_tumor_type="tumor1_subtype2",
            other_tumor_type="tumor1_subtype3",
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].tumor_type == TumorType(
            icd10_code="C40.50",
            icd10_description="CRC",
            tumor_type="CRC_subtype",
            tumor_type_code=40,
            cohort_tumor_type=None,
            other_tumor_type=None,
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].tumor_type == TumorType(
            icd10_code="C07",
            icd10_description="tumor2",
            tumor_type="tumor2_subtype1",
            tumor_type_code=70,
            cohort_tumor_type="tumor2_subtype2",
            other_tumor_type=None,
        )

        assert harmonizer.patient_data["IMPRESS-X_0004_1"].tumor_type == TumorType(
            icd10_code="C70.1",
            icd10_description="tumor3",
            tumor_type="tumor3_subtype1",
            tumor_type_code=10,
            cohort_tumor_type=None,
            other_tumor_type="tumor3_subtype2",
        )

        assert harmonizer.patient_data["IMPRESS-X_0005_1"].tumor_type == TumorType(
            icd10_code="C23.20",
            icd10_description="tumor4",
            tumor_type="tumor4_subtype1",
            tumor_type_code=30,
            cohort_tumor_type=None,
            other_tumor_type="tumor4_subtype2",
        )

    def test_study_drugs_processing(self, study_drugs_fixture):
        harmonizer = ImpressHarmonizer(
            data=study_drugs_fixture, trial_id="IMPRESS_TEST"
        )

        for subject_id in (
            study_drugs_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_study_drugs()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].study_drugs == StudyDrugs(
            primary_treatment_drug="Traztuzumab",
            primary_treatment_drug_code=31,
            secondary_treatment_drug="Tafinlar",
            secondary_treatment_drug_code=10,
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].study_drugs == StudyDrugs(
            primary_treatment_drug="some drug",
            primary_treatment_drug_code=99,
            secondary_treatment_drug="some drug 2",
            secondary_treatment_drug_code=1,
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].study_drugs == StudyDrugs(
            primary_treatment_drug="mismatch_1",
            primary_treatment_drug_code=10,
            secondary_treatment_drug="mismatch_1_2",
            secondary_treatment_drug_code=12,
        )

        assert harmonizer.patient_data["IMPRESS-X_0004_1"].study_drugs == StudyDrugs(
            primary_treatment_drug="mismatch_2",
            primary_treatment_drug_code=50,
            secondary_treatment_drug="mismatch_2_1",
            secondary_treatment_drug_code=60,
        )

        assert harmonizer.patient_data["IMPRESS-X_0005_1"].study_drugs == StudyDrugs(
            primary_treatment_drug="partial",
            primary_treatment_drug_code=None,
            secondary_treatment_drug=None,
            secondary_treatment_drug_code=5,
        )

    def test_date_of_death_processing(self, date_of_death_fixture):
        harmonizer = ImpressHarmonizer(
            data=date_of_death_fixture, trial_id="IMPRESS_TEST"
        )

        for subject_id in (
            date_of_death_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_date_of_death()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].date_of_death == dt.datetime(
            1990, 10, 2
        )
        assert harmonizer.patient_data["IMPRESS-X_0002_1"].date_of_death == dt.datetime(
            2016, 9, 12
        )
        assert harmonizer.patient_data["IMPRESS-X_0003_1"].date_of_death == dt.datetime(
            1900, 1, 1
        )
        assert harmonizer.patient_data["IMPRESS-X_0004_1"].date_of_death == dt.datetime(
            1999, 9, 9
        )
        assert harmonizer.patient_data["IMPRESS-X_0005_1"].date_of_death is None

    # def test_biomarker_processing(self, biomarker_fixture):
    #     harmonizer = ImpressHarmonizer(data=biomarker_fixture, trial_id="IMPRESS_TEST")
    #
    #     for subject_id in biomarker_fixture.select("SubjectId").unique().to_series().to_list():
    #         harmonizer.patient_data[subject_id] = Patient(trial_id="IMPRESS_TEST", patient_id=subject_id)
    #
    #     harmonizer._process_biomarkers()

    # def test_lost_to_followup(self, lost_to_followup_fixture):
    #     # harmonizer = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="IMPRESS_TEST")
    #     # harmonizer._process_patient_id()
    #     pass

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
