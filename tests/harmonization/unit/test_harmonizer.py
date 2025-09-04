# tests/harmonization/unit/test_harmonizer.py
from typing import Dict, List, Tuple

import polars as pl
import datetime as dt

import pytest

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
    EcogBaseline,
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
    medical_history_fixture,
    adverse_event_number_fixture,
    serious_adverse_event_number_fixture,
)


def test_impress_subject_id_processing(subject_id_fixture):
    harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="IMPRESS_TEST")
    harmonizer._process_patient_id()

    assert len(harmonizer.patient_data) == 6

    expected_ids = [
        "IMPRESS-X_0001_1",
        "IMPRESS-X_0002_1",
        "IMPRESS-X_0003_1",
        "IMPRESS-X_0004_1",
        "IMPRESS-X_0005_1",
        "IMPRESS-X_0005_2",  # todo: collapsing not implemented yet
    ]
    assert set(expected_ids) == set(harmonizer.patient_data)

    for patient_id, patient_data in harmonizer.patient_data.items():
        assert patient_data.patient_id == patient_id
        assert patient_data.trial_id == "IMPRESS_TEST"


def test_impress_cohort_name_processing(cohort_name_fixture):
    harmonizer = ImpressHarmonizer(data=cohort_name_fixture, trial_id="IMPRESS_TEST")

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


def test_gender_processing(gender_fixture):
    harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="IMPRESS_TEST")

    for subject_id in gender_fixture.select("SubjectId").unique().to_series().to_list():
        harmonizer.patient_data[subject_id] = Patient(
            trial_id="IMPRESS_TEST", patient_id=subject_id
        )

    harmonizer._process_gender()

    assert harmonizer.patient_data["IMPRESS-X_0001_1"].sex == "female"
    assert harmonizer.patient_data["IMPRESS-X_0002_1"].sex == "male"
    assert harmonizer.patient_data["IMPRESS-X_0003_1"].sex == "female"
    assert harmonizer.patient_data["IMPRESS-X_0004_1"].sex == "male"
    assert harmonizer.patient_data["IMPRESS-X_0005_1"].sex is None
    assert harmonizer.patient_data["IMPRESS-X_0006_1"].sex is None
    assert harmonizer.patient_data["IMPRESS-X_0007_1"].sex == "female"
    assert harmonizer.patient_data["IMPRESS-X_0008_1"].sex == "male"


def test_age_processing(age_fixture):
    harmonizer = ImpressHarmonizer(data=age_fixture, trial_id="IMPRESS_TEST")

    for subject_id in age_fixture.select("SubjectId").unique().to_series().to_list():
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


def test_study_drugs_processing(study_drugs_fixture):
    harmonizer = ImpressHarmonizer(data=study_drugs_fixture, trial_id="IMPRESS_TEST")

    for subject_id in (
        study_drugs_fixture.select("SubjectId").unique().to_series().to_list()
    ):
        harmonizer.patient_data[subject_id] = Patient(
            trial_id="IMPRESS_TEST", patient_id=subject_id
        )

    harmonizer._process_study_drugs()

    study_drug_instance_1 = harmonizer.patient_data["IMPRESS-X_0001_1"].study_drugs
    assert study_drug_instance_1.primary_treatment_drug == "Traztuzumab"
    assert study_drug_instance_1.primary_treatment_drug_code == 31
    assert study_drug_instance_1.secondary_treatment_drug == "Tafinlar"
    assert study_drug_instance_1.secondary_treatment_drug_code == 10

    study_drug_instance_2 = harmonizer.patient_data["IMPRESS-X_0002_1"].study_drugs
    assert study_drug_instance_2.primary_treatment_drug == "some drug"
    assert study_drug_instance_2.primary_treatment_drug_code == 99
    assert study_drug_instance_2.secondary_treatment_drug == "some drug 2"
    assert study_drug_instance_2.secondary_treatment_drug_code == 1

    study_drug_instance_3 = harmonizer.patient_data["IMPRESS-X_0003_1"].study_drugs
    assert study_drug_instance_3.primary_treatment_drug == "mismatch_1"
    assert study_drug_instance_3.primary_treatment_drug_code == 10
    assert study_drug_instance_3.secondary_treatment_drug == "mismatch_1_2"
    assert study_drug_instance_3.secondary_treatment_drug_code == 12

    study_drug_instance_4 = harmonizer.patient_data["IMPRESS-X_0004_1"].study_drugs
    assert study_drug_instance_4.primary_treatment_drug == "mismatch_2"
    assert study_drug_instance_4.primary_treatment_drug_code == 50
    assert study_drug_instance_4.secondary_treatment_drug == "mismatch_2_1"
    assert study_drug_instance_4.secondary_treatment_drug_code == 60

    # data with mutually exclusive fields colliding is logged, and returned as None
    assert harmonizer.patient_data["IMPRESS-X_0005_1"].study_drugs is None


def test_date_of_death_processing(date_of_death_fixture):
    harmonizer = ImpressHarmonizer(data=date_of_death_fixture, trial_id="IMPRESS_TEST")

    for subject_id in (
        date_of_death_fixture.select("SubjectId").unique().to_series().to_list()
    ):
        harmonizer.patient_data[subject_id] = Patient(
            trial_id="IMPRESS_TEST", patient_id=subject_id
        )

    harmonizer._process_date_of_death()

    assert harmonizer.patient_data["IMPRESS-X_0001_1"].date_of_death == dt.date(
        1990, 7, 2
    )
    assert harmonizer.patient_data["IMPRESS-X_0002_1"].date_of_death == dt.date(
        2016, 9, 15
    )
    assert harmonizer.patient_data["IMPRESS-X_0003_1"].date_of_death == dt.date(
        1900, 1, 1
    )
    assert harmonizer.patient_data["IMPRESS-X_0004_1"].date_of_death == dt.date(
        1999, 9, 9
    )
    assert harmonizer.patient_data["IMPRESS-X_0005_1"].date_of_death is None


def test_biomarker_processing(biomarker_fixture):
    harmonizer = ImpressHarmonizer(data=biomarker_fixture, trial_id="IMPRESS_TEST")

    for subject_id in (
        biomarker_fixture.select("SubjectId").unique().to_series().to_list()
    ):
        harmonizer.patient_data[subject_id] = Patient(
            trial_id="IMPRESS_TEST", patient_id=subject_id
        )

    harmonizer._process_biomarkers()

    biomarker_1 = harmonizer.patient_data["IMPRESS-X_0001_1"].biomarker
    assert biomarker_1.gene_and_mutation == "BRAF activating mutations"
    assert biomarker_1.gene_and_mutation_code == 21
    assert biomarker_1.cohort_target_name == "BRAF Non-V600 activating mutations"
    assert biomarker_1.cohort_target_mutation == "BRAF Non-V600 activating mutations"
    assert biomarker_1.event_date == dt.date(1900, 7, 15)

    biomarker_2 = harmonizer.patient_data["IMPRESS-X_0002_1"].biomarker
    assert biomarker_2.gene_and_mutation is None
    assert biomarker_2.gene_and_mutation_code is None
    assert biomarker_2.cohort_target_name == "some info"
    assert biomarker_2.cohort_target_mutation is None
    assert biomarker_2.event_date == dt.date(1980, 2, 15)

    biomarker_3 = harmonizer.patient_data["IMPRESS-X_0003_1"].biomarker
    assert biomarker_3.gene_and_mutation == "BRCA1 inactivating mutation"
    assert biomarker_3.gene_and_mutation_code == 2
    assert biomarker_3.cohort_target_name == "BRCA1 stop-gain del exon 11"
    assert biomarker_3.cohort_target_mutation == "BRCA1 stop-gain deletion"
    assert biomarker_3.event_date is None

    biomarker_4 = harmonizer.patient_data["IMPRESS-X_0004_1"].biomarker
    assert biomarker_4.gene_and_mutation == "SDHAF2 mutation"
    assert biomarker_4.gene_and_mutation_code == -1
    assert biomarker_4.cohort_target_name == "more info"
    assert biomarker_4.cohort_target_mutation is None
    assert biomarker_4.event_date == dt.date(1999, 7, 11)

    biomarker_5 = harmonizer.patient_data["IMPRESS-X_0005_1"].biomarker
    assert biomarker_5.gene_and_mutation is None
    assert biomarker_5.gene_and_mutation_code == 10
    assert biomarker_5.cohort_target_name is None
    assert biomarker_5.cohort_target_mutation == "some other info"
    assert biomarker_5.event_date is None


def test_lost_to_followup(lost_to_followup_fixture):
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

    ins_1 = harmonizer.patient_data["IMPRESS-X_0001_1"].lost_to_followup
    assert not ins_1.lost_to_followup
    assert ins_1.date_lost_to_followup is None

    ins_2 = harmonizer.patient_data["IMPRESS-X_0002_1"].lost_to_followup
    assert not ins_2.lost_to_followup
    assert ins_2.date_lost_to_followup is None

    ins_3 = harmonizer.patient_data["IMPRESS-X_0003_1"].lost_to_followup
    assert ins_3.lost_to_followup
    assert ins_3.date_lost_to_followup == dt.date(1900, 1, 1)

    ins_4 = harmonizer.patient_data["IMPRESS-X_0004_1"].lost_to_followup
    assert not ins_4.lost_to_followup
    assert ins_4.date_lost_to_followup is None

    ins_5 = harmonizer.patient_data["IMPRESS-X_0005_1"].lost_to_followup
    assert not ins_5.lost_to_followup
    assert ins_5.date_lost_to_followup is None


@pytest.mark.parametrize(
    "patient_id,expected",
    [
        pytest.param("IMPRESS-X_0001_1", False, id="one IV row: not evaluable"),
        pytest.param(
            "IMPRESS-X_0002_1", False, id="two IV rows, gap lt 21: not evaluable"
        ),
        pytest.param("IMPRESS-X_0003_1", True, id="two IV rows, gap gte 21: evaluable"),
        pytest.param(
            "IMPRESS-X_0004_1", True, id="IV none, oral sufficient: evaluable"
        ),
        pytest.param("IMPRESS-X_0005_1", True, id="IV sufficient, oral not: evaluable"),
        pytest.param("IMPRESS-X_0006_1", False, id="oral missing end: not evaluable"),
        pytest.param(
            "IMPRESS-X_0007_1", False, id="oral end not a date: not evaluable"
        ),
        pytest.param(
            "IMPRESS-X_0008_1", False, id="oral start not a date: not evaluable"
        ),
        pytest.param("IMPRESS-X_0009_1", False, id="oral missing start: not evaluable"),
        pytest.param("IMPRESS-X_0010_1", False, id="IV one start null: not evaluable"),
        pytest.param("IMPRESS-X_0011_1", False, id="IV gap lte 21: not evaluable"),
        pytest.param("IMPRESS-X_0012_1", False, id="oral length lte 28: not evaluable"),
        pytest.param(
            "IMPRESS-X_0013_1", False, id="IV gap across drugs: not evaluable"
        ),
        pytest.param("IMPRESS-X_0014_1", False, id="IV one invalid row: not evaluable"),
        pytest.param(
            "IMPRESS-X_0015_1", False, id="oral sufficient but invalid: not evaluable"
        ),
    ],
)
def test_evaluability_cases(evaluability_fixture, patient_id, expected):
    # instantiate Patient with eval data
    harmonizer = ImpressHarmonizer(data=evaluability_fixture, trial_id="IMPRESS_TEST")
    for subject_id in evaluability_fixture.select("SubjectId").unique().to_series():
        harmonizer.patient_data[subject_id] = Patient(
            patient_id=subject_id, trial_id="IMPRESS_TEST"
        )

    # process evaluability
    harmonizer._process_evaluability()
    result = harmonizer.patient_data[patient_id].evaluable_for_efficacy_analysis

    assert result is expected


def test_ecog(ecog_fixture):
    harmonizer = ImpressHarmonizer(data=ecog_fixture, trial_id="IMPRESS_TEST")

    for subject_id in ecog_fixture.select("SubjectId").unique().to_series().to_list():
        harmonizer.patient_data[subject_id] = Patient(
            patient_id=subject_id, trial_id="IMPRESS_TEST"
        )

    harmonizer._process_ecog_baseline()

    ins1 = harmonizer.patient_data["IMPRESS-X_0001_1"].ecog_baseline
    assert ins1.description == "all"
    assert ins1.grade == 1
    assert ins1.date == dt.date(1900, 1, 1)

    ins2 = harmonizer.patient_data["IMPRESS-X_0002_1"].ecog_baseline
    assert ins2.description == "no code"
    assert ins2.grade is None
    assert ins2.date == dt.date(1900, 7, 1)

    ins3 = harmonizer.patient_data["IMPRESS-X_0003_1"].ecog_baseline
    assert ins3.description is None
    assert ins3.grade == 2
    assert ins3.date == dt.date(1900, 1, 15)

    ins4 = harmonizer.patient_data["IMPRESS-X_0006_1"].ecog_baseline
    assert ins4.description is None
    assert ins4.grade == 1
    assert ins4.date == dt.date(1900, 7, 15)

    ins5 = harmonizer.patient_data["IMPRESS-X_0007_1"].ecog_baseline
    assert ins5.description == "code"
    assert ins5.grade == 4
    assert ins5.date is None

    assert harmonizer.patient_data["IMPRESS-X_0004_1"].ecog_baseline is None
    assert harmonizer.patient_data["IMPRESS-X_0005_1"].ecog_baseline is None


def test_medical_history(medical_history_fixture):
    harmonizer = ImpressHarmonizer(
        data=medical_history_fixture, trial_id="IMPRESS_TEST"
    )

    # one Patient per unique SubjectId
    for sid in medical_history_fixture.select("SubjectId").unique().to_series():
        harmonizer.patient_data[sid] = Patient(patient_id=sid, trial_id="IMPRESS_TEST")

    harmonizer._process_medical_histories()

    p1 = harmonizer.patient_data["IMPRESS-X_0001_1"]
    p2 = harmonizer.patient_data["IMPRESS-X_0002_1"]
    p3 = harmonizer.patient_data["IMPRESS-X_0003_1"]
    p4 = harmonizer.patient_data["IMPRESS-X_0004_1"]
    p5 = harmonizer.patient_data["IMPRESS-X_0005_1"]
    p6 = harmonizer.patient_data["IMPRESS-X_0006_1"]

    mh1 = list(p1.medical_histories)
    assert len(mh1) == 2

    # todo: add getters later
    assert mh1[0].term == "pain"
    assert mh1[0].sequence_id == 1
    assert mh1[0].start_date == dt.date(1900, 9, 15)
    assert mh1[0].end_date is None
    assert mh1[0].status == "Current/active"
    assert mh1[0].status_code == 1

    assert mh1[-1].term == "something"
    assert mh1[-1].sequence_id == 5
    assert mh1[-1].start_date == dt.date(1900, 7, 2)
    assert mh1[-1].end_date == dt.date(1990, 1, 1)
    assert mh1[-1].status == "Past"
    assert mh1[-1].status_code == 3

    assert p2.medical_histories[0].term == "hypertension"
    assert p2.medical_histories[0].sequence_id == 2
    assert p2.medical_histories[0].start_date == dt.date(1901, 10, 2)
    assert p2.medical_histories[0].end_date == dt.date(1901, 11, 2)
    assert p2.medical_histories[0].status == "Past"
    assert p2.medical_histories[0].status_code == 3

    assert p3.medical_histories[0].term == "dizziness"
    assert p3.medical_histories[0].sequence_id == 3
    assert p3.medical_histories[0].start_date == dt.date(1902, 7, 15)
    assert p3.medical_histories[0].end_date == dt.date(1903, 7, 15)
    assert p3.medical_histories[0].status == "Present/dormant"
    assert p3.medical_histories[0].status_code == 2

    assert p4.medical_histories[0].term == "pain"
    assert p4.medical_histories[0].sequence_id == 1
    assert p4.medical_histories[0].start_date == dt.date(1840, 2, 2)
    assert p4.medical_histories[0].end_date is None  # == dt.date(1940, 7, 2)
    assert p4.medical_histories[0].status == "Past"
    assert p4.medical_histories[0].status_code == 3

    assert p5.medical_histories[0].term == "rigor mortis"
    assert p5.medical_histories[0].sequence_id == 1
    assert p5.medical_histories[0].start_date == dt.date(1740, 2, 2)
    assert p5.medical_histories[0].end_date == dt.date(1940, 2, 2)
    assert p5.medical_histories[0].status == "Past"
    assert p5.medical_histories[0].status_code == 1

    assert p6.medical_histories == ()


# todo: implement for rest of treatments
def test_previous_treatments():
    pass


def test_treatment_start():
    pass


def test_treatment_ends():
    pass


def test_last_treatment_start():
    pass


def test_treatment_cycles():
    pass


def test_concomitant_medications():
    pass


def test_has_any_adverse_events():
    pass


def test_adverse_event_number(adverse_event_number_fixture):
    harmonizer = ImpressHarmonizer(
        data=adverse_event_number_fixture, trial_id="IMPRESS_TEST"
    )
    for sid in adverse_event_number_fixture.select("SubjectId").unique().to_series():
        harmonizer.patient_data[sid] = Patient(patient_id=sid, trial_id="IMPRESS_TEST")

    harmonizer._process_number_of_adverse_events()

    p1 = harmonizer.patient_data["IMPRESS-X_0001_1"]
    p2 = harmonizer.patient_data["IMPRESS-X_0002_1"]
    p3 = harmonizer.patient_data["IMPRESS-X_0003_1"]
    p4 = harmonizer.patient_data["IMPRESS-X_0004_1"]
    p5 = harmonizer.patient_data["IMPRESS-X_0005_1"]

    assert p1.number_of_adverse_events == 2
    assert p2.number_of_adverse_events == 3
    assert p3.number_of_adverse_events == 1
    assert p4.number_of_adverse_events == 1
    assert p5.number_of_adverse_events == 0


def test_serious_adverse_event_number(serious_adverse_event_number_fixture):
    harmonizer = ImpressHarmonizer(
        data=serious_adverse_event_number_fixture, trial_id="IMPRESS_TEST"
    )
    for sid in (
        serious_adverse_event_number_fixture.select("SubjectId").unique().to_series()
    ):
        harmonizer.patient_data[sid] = Patient(patient_id=sid, trial_id="IMPRESS_TEST")

    harmonizer._process_number_of_serious_adverse_events()

    p1 = harmonizer.patient_data["IMPRESS-X_0001_1"]
    p2 = harmonizer.patient_data["IMPRESS-X_0002_1"]
    p3 = harmonizer.patient_data["IMPRESS-X_0003_1"]
    p4 = harmonizer.patient_data["IMPRESS-X_0004_1"]
    p5 = harmonizer.patient_data["IMPRESS-X_0005_1"]

    assert p1.number_of_serious_adverse_events == 1
    assert p2.number_of_serious_adverse_events == 2
    assert p3.number_of_serious_adverse_events == 1
    assert p4.number_of_serious_adverse_events == 0
    assert p5.number_of_serious_adverse_events == 0


# def test_serious_adverse_events_number(serious_adverse_events_fixture):
#     pass
#
# def test_adverse_events(adverse_events_fixture):
#     pass


def test_basic_inheritance(subject_id_fixture):
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
    assert CoreParsers.parse_date_flexible("1900-02-Nk") == dt.date(1900, 2, 15)
    assert CoreParsers.parse_date_flexible("1900-nK-NK") == dt.date(1900, 7, 15)
    assert CoreParsers.parse_date_flexible("1900-nK-10") == dt.date(1900, 7, 10)


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

    parsed_df: pl.DataFrame = df.with_columns(
        parsed_dates=PolarsParsers.parse_date_column(column=pl.col("dates"))
    )
    assert parsed_df["parsed_dates"][0] == dt.date(1900, 2, 2)
    assert parsed_df["parsed_dates"][1] == dt.date(1950, 6, 15)
    assert parsed_df["parsed_dates"][2] == dt.date(1900, 7, 15)
    assert parsed_df["parsed_dates"][3] == dt.date(1900, 2, 15)
    assert parsed_df["parsed_dates"][4] == dt.date(1900, 7, 15)
    assert parsed_df["parsed_dates"][5] == dt.date(1900, 7, 10)
