import polars as pl
import datetime as dt
from src.harmonization.harmonizers.impress import ImpressHarmonizer
from src.harmonization.harmonizers.base import BaseHarmonizer
from src.utils.helpers import parse_flexible_date, parse_date_column
from src.harmonization.datamodels import (
    TumorType,
    StudyDrugs,
    Patient,
    Biomarkers,
    FollowUp,
)
from tests.harmonization.fixtures.impress_fixtures import (
    subject_id_fixture,
    cohort_name_fixture,
    age_fixture,
    gender_fixture,
    tumor_type_fixture,
    study_drugs_fixture,
    biomarker_fixture,
    date_of_death_fixture,
    lost_to_followup_fixture,
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

    def test_biomarker_processing(self, biomarker_fixture):
        harmonizer = ImpressHarmonizer(data=biomarker_fixture, trial_id="IMPRESS_TEST")

        for subject_id in (
            biomarker_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                trial_id="IMPRESS_TEST", patient_id=subject_id
            )

        harmonizer._process_biomarkers()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].biomarker == Biomarkers(
            gene_and_mutation="BRAF activating mutations",
            gene_and_mutation_code=21,
            cohort_target_name="BRAF Non-V600activating mutations",
            cohort_target_mutation="BRAF Non-V600 activating mutations",
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].biomarker == Biomarkers(
            gene_and_mutation=None,
            gene_and_mutation_code=None,
            cohort_target_name="some info",
            cohort_target_mutation=None,
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].biomarker == Biomarkers(
            gene_and_mutation="BRCA1 inactivating mutation",
            gene_and_mutation_code=2,
            cohort_target_name="BRCA1 stop-gain del exon 11",
            cohort_target_mutation="BRCA1 stop-gain deletion",
        )

        assert harmonizer.patient_data["IMPRESS-X_0004_1"].biomarker == Biomarkers(
            gene_and_mutation="SDHAF2 mutation",
            gene_and_mutation_code=-1,
            cohort_target_name="more info",
            cohort_target_mutation=None,
        )

        assert harmonizer.patient_data["IMPRESS-X_0005_1"].biomarker == Biomarkers(
            gene_and_mutation=None,
            gene_and_mutation_code=10,
            cohort_target_name=None,
            cohort_target_mutation="some other info",
        )

    def test_lost_to_followup(self, lost_to_followup_fixture):
        harmonizer = ImpressHarmonizer(
            data=lost_to_followup_fixture, trial_id="IMPRESS_TEST"
        )
        for subject_id in (
            lost_to_followup_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                patient_id=subject_id, trial_id="IMPRESS_TEST"
            )

        harmonizer._process_date_lost_to_followup()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].lost_to_followup == FollowUp(
            lost_to_followup=False, date_lost_to_followup=None
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].lost_to_followup == FollowUp(
            lost_to_followup=False, date_lost_to_followup=None
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].lost_to_followup == FollowUp(
            lost_to_followup=True, date_lost_to_followup=dt.datetime(1900, 1, 1)
        )

        assert harmonizer.patient_data["IMPRESS-X_0004_1"].lost_to_followup == FollowUp(
            lost_to_followup=False, date_lost_to_followup=None
        )

        assert harmonizer.patient_data["IMPRESS-X_0005_1"].lost_to_followup == FollowUp(
            lost_to_followup=False, date_lost_to_followup=None
        )

    def test_basic_inheritance(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")

        assert isinstance(harmonizer, BaseHarmonizer)

        assert harmonizer.data is subject_id_fixture
        assert harmonizer.trial_id == "IMPRESS_TEST"

        assert hasattr(harmonizer, "patient_data")
        assert isinstance(harmonizer.patient_data, dict)


def test_parse_flexible_date_function():
    assert parse_flexible_date("1900-02-02") == dt.datetime(1900, 2, 2)
    assert parse_flexible_date("1950-06") == dt.datetime(1950, 6, 15)
    assert parse_flexible_date("1900") == dt.datetime(1900, 7, 15)


def test_parse_date_column_function():
    """Test the vectorized date parsing function with a dataframe"""
    df = pl.DataFrame({"dates": ["1900-02-02", "1950-06", "1900"]})

    # with col name
    result_by_name = df.with_columns(parsed_dates=parse_date_column("dates"))
    assert result_by_name["parsed_dates"][0] == dt.datetime(1900, 2, 2)
    assert result_by_name["parsed_dates"][1] == dt.datetime(1950, 6, 15)
    assert result_by_name["parsed_dates"][2] == dt.datetime(1900, 7, 15)

    # with col expression
    result_by_expr = df.with_columns(parsed_dates=parse_date_column(pl.col("dates")))
    assert result_by_expr["parsed_dates"][0] == dt.datetime(1900, 2, 2)
    assert result_by_expr["parsed_dates"][1] == dt.datetime(1950, 6, 15)
    assert result_by_expr["parsed_dates"][2] == dt.datetime(1900, 7, 15)
