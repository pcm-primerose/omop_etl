import polars as pl
import datetime as dt
from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer
from omop_etl.harmonization.parsing.core import (
    CoreParsers,
    PolarsParsers,
    TypeCoercion,
)
from omop_etl.harmonization.datamodels import (
    TumorType,
    StudyDrugs,
    Patient,
    Biomarkers,
    FollowUp,
    Ecog,
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
    evaluability_fixture,
    ecog_fixture,
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


def test_tumor_processing(tumor_type_fixture):
    harmonizer = ImpressHarmonizer(data=tumor_type_fixture, trial_id="IMPRESS_TEST")

    for subject_id in (
        tumor_type_fixture.select("SubjectId").unique().to_series().to_list()
    ):
        harmonizer.patient_data[subject_id] = Patient(
            trial_id="IMPRESS_TEST", patient_id=subject_id
        )

    harmonizer._process_tumor_type()

    tumor_instance_1 = harmonizer.patient_data["IMPRESS-X_0001_1"].tumor_type
    assert tumor_instance_1.icd10_code == "C30"
    assert tumor_instance_1.icd10_description == "tumor1"
    assert tumor_instance_1.main_tumor_type == "tumor1_subtype1"
    assert tumor_instance_1.main_tumor_type_code == 50
    assert tumor_instance_1.cohort_tumor_type == "tumor1_subtype2"
    assert tumor_instance_1.other_tumor_type == "tumor1_subtype3"

    tumor_instance_2 = harmonizer.patient_data["IMPRESS-X_0002_1"].tumor_type
    assert tumor_instance_2.icd10_code == "C40.50"
    assert tumor_instance_2.icd10_description == "CRC"
    assert tumor_instance_2.main_tumor_type == "CRC_subtype"
    assert tumor_instance_2.main_tumor_type_code == 40
    assert tumor_instance_2.cohort_tumor_type is None
    assert tumor_instance_2.other_tumor_type is None

    tumor_instance_3 = harmonizer.patient_data["IMPRESS-X_0003_1"].tumor_type
    assert tumor_instance_3.icd10_code == "C07"
    assert tumor_instance_3.icd10_description == "tumor2"
    assert tumor_instance_3.main_tumor_type == "tumor2_subtype1"
    assert tumor_instance_3.main_tumor_type_code == 70
    assert tumor_instance_3.cohort_tumor_type == "tumor2_subtype2"
    assert tumor_instance_3.other_tumor_type is None

    tumor_instance_4 = harmonizer.patient_data["IMPRESS-X_0004_1"].tumor_type
    assert tumor_instance_4.icd10_code == "C70.1"
    assert tumor_instance_4.icd10_description == "tumor3"
    assert tumor_instance_4.main_tumor_type == "tumor3_subtype1"
    assert tumor_instance_4.main_tumor_type_code == 10
    assert tumor_instance_4.cohort_tumor_type is None
    assert tumor_instance_4.other_tumor_type == "tumor3_subtype2"

    tumor_instance_5 = harmonizer.patient_data["IMPRESS-X_0005_1"].tumor_type
    assert tumor_instance_5.icd10_code == "C23.20"
    assert tumor_instance_5.icd10_description == "tumor4"
    assert tumor_instance_5.main_tumor_type == "tumor4_subtype1"
    assert tumor_instance_5.main_tumor_type_code == 30
    assert tumor_instance_5.cohort_tumor_type is None
    assert tumor_instance_5.other_tumor_type == "tumor4_subtype2"

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

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].study_drugs == (
            StudyDrugs.primary_treatment_drug == "Traztuzumab",
            StudyDrugs.primary_treatment_drug_code == 31,
            StudyDrugs.secondary_treatment_drug == "Tafinlar",
            StudyDrugs.secondary_treatment_drug_code == 10,
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].study_drugs == (
            StudyDrugs.primary_treatment_drug == "some drug",
            StudyDrugs.primary_treatment_drug_code == 99,
            StudyDrugs.secondary_treatment_drug == "some drug 2",
            StudyDrugs.secondary_treatment_drug_code == 1,
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].study_drugs == (
            StudyDrugs.primary_treatment_drug == "mismatch_1",
            StudyDrugs.primary_treatment_drug_code == 10,
            StudyDrugs.secondary_treatment_drug == "mismatch_1_2",
            StudyDrugs.secondary_treatment_drug_code == 12,
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

    def test_evaluability(self, evaluability_fixture):
        harmonizer = ImpressHarmonizer(
            data=evaluability_fixture, trial_id="IMPRESS_TEST"
        )
        for subject_id in (
            evaluability_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                patient_id=subject_id, trial_id="IMPRESS_TEST"
            )

        harmonizer._process_evaluability()

        assert (
            harmonizer.patient_data["IMPRESS-X_0001_1"].evaluable_for_efficacy_analysis
            is True
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0002_1"].evaluable_for_efficacy_analysis
            is True
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0003_1"].evaluable_for_efficacy_analysis
            is True
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0004_1"].evaluable_for_efficacy_analysis
            is False
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0005_1"].evaluable_for_efficacy_analysis
            is False
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0006_1"].evaluable_for_efficacy_analysis
            is False
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0007_1"].evaluable_for_efficacy_analysis
            is False
        )
        assert (
            harmonizer.patient_data["IMPRESS-X_0008_1"].evaluable_for_efficacy_analysis
            is False
        )

    def test_ecog(self, ecog_fixture):
        harmonizer = ImpressHarmonizer(data=ecog_fixture, trial_id="IMPRESS_TEST")

        for subject_id in (
            ecog_fixture.select("SubjectId").unique().to_series().to_list()
        ):
            harmonizer.patient_data[subject_id] = Patient(
                patient_id=subject_id, trial_id="IMPRESS_TEST"
            )

        harmonizer._process_ecog()

        assert harmonizer.patient_data["IMPRESS-X_0001_1"].ecog == Ecog(
            description="all", grade=1
        )

        assert harmonizer.patient_data["IMPRESS-X_0002_1"].ecog == Ecog(
            description="no code", grade=None
        )

        assert harmonizer.patient_data["IMPRESS-X_0003_1"].ecog == Ecog(
            description=None, grade=2
        )

        assert harmonizer.patient_data["IMPRESS-X_0004_1"].ecog == Ecog(
            description="wrong ID", grade=3
        )

        assert harmonizer.patient_data["IMPRESS-X_0005_1"].ecog == Ecog(
            description=None, grade=None
        )

    def test_basic_inheritance(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")

        assert isinstance(harmonizer, BaseHarmonizer)

        assert harmonizer.data is subject_id_fixture
        assert harmonizer.trial_id == "IMPRESS_TEST"

        assert hasattr(harmonizer, "patient_data")
        assert isinstance(harmonizer.patient_data, dict)


def test_parse_flexible_date_function():
    assert CoreParsers.parse_date_flexible("1900-02-02") == dt.date(1900, 2, 2)
    assert CoreParsers.parse_date_flexible("1950-06") == dt.date(1950, 6, 15)
    assert CoreParsers.parse_date_flexible("1900") == dt.date(1900, 7, 15)


def test_parse_date_column_function():
    """Test the vectorized date parsing function with a dataframe"""
    df = pl.DataFrame(
        {
            "dates": [
                "1900-02-02",
                "1950-06",
                "1900",
                "1900-02-Nk",
                "1900-nk-NK",
                "1900-Nk-10",
            ]
        }
    )

    # with col name
    result_by_name = df.with_columns(
        parsed_dates=PolarsParsers.parse_date_column(column=pl.col("dates"))
    )
    assert result_by_name["parsed_dates"][0] == dt.date(1900, 2, 2)
    assert result_by_name["parsed_dates"][1] == dt.date(1950, 6, 15)
    assert result_by_name["parsed_dates"][2] == dt.date(1900, 7, 15)
    assert result_by_name["parsed_dates"][3] == dt.date(1900, 2, 15)
    assert result_by_name["parsed_dates"][4] == dt.date(1900, 7, 15)
    assert result_by_name["parsed_dates"][5] == dt.date(1900, 7, 10)
