import datetime as dt
from typing import ClassVar

import pytest
import polars as pl

from omop_etl.harmonization.harmonizers.base import (
    CollectionSpec,
    ScalarSpec,
    SingletonSpec,
)
from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer
from omop_etl.harmonization.models.patient import Patient


class TestProcessCohortNameDF:
    def test_returns_expected_columns(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        assert set(df.columns) == {"SubjectId", "cohort_name"}

    def test_extracts_cohort_names(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        row = df.filter(pl.col("SubjectId") == "cohort_hit_1")
        assert row.item(0, "cohort_name") == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"

        row = df.filter(pl.col("SubjectId") == "cohort_hit_2")
        assert row.item(0, "cohort_name") == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"

    def test_empty_values_filtered_out(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        subject_ids = set(df["SubjectId"].to_list())
        assert "cohort_empty_1" not in subject_ids  # explicit None
        assert "cohort_empty_2" not in subject_ids  # empty string
        assert "cohort_empty_3" not in subject_ids  # missing value


class TestProcessSexDF:
    def test_returns_expected_columns(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        assert set(df.columns) == {"SubjectId", "sex"}

    def test_normalizes_to_lowercase(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        values = set(df["sex"].to_list())
        assert values <= {"male", "female"}, "all values should be 'male' or 'female'"

    def test_maps_variations_correctly(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        def get_sex(subject_id: str) -> str | None:
            row = df.filter(pl.col("SubjectId") == subject_id)
            if row.height == 0:
                return None
            return row.item(0, "sex")

        assert get_sex("female_titlecase") == "female"
        assert get_sex("male_titlecase") == "male"
        assert get_sex("female_short_f") == "female"
        assert get_sex("male_short_m") == "male"
        assert get_sex("female_lowercase") == "female"
        assert get_sex("male_lowercase") == "male"

    def test_invalid_values_filtered_out(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        subject_ids = set(df["SubjectId"].to_list())
        assert "invalid_value" not in subject_ids
        assert "empty_value" not in subject_ids


class TestProcessAgeDF:
    def test_returns_expected_columns(self, age_fixture):
        h = ImpressHarmonizer(data=age_fixture, trial_id="T")
        df = h._process_age()

        assert "SubjectId" in df.columns
        assert "age" in df.columns

    def test_calculates_age_correctly(self, age_fixture):
        h = ImpressHarmonizer(data=age_fixture, trial_id="T")
        df = h._process_age()

        def get_age(subject_id: str) -> int | None:
            row = df.filter(pl.col("SubjectId") == subject_id)
            if row.height == 0:
                return None
            return row.item(0, "age")

        assert get_age("birth_full_tx_full") == 89
        assert get_age("birth_year_tx_full") == 39
        assert get_age("birth_full_tx_full_recent") == 20
        assert get_age("birth_year_tx_year") == 30
        assert get_age("birth_full_tx_year_month") == 9


class TestProcessTumorTypeDF:
    def test_returns_expected_columns(self, tumor_type_fixture):
        h = ImpressHarmonizer(data=tumor_type_fixture, trial_id="T")
        df = h._process_tumor_type()

        from omop_etl.harmonization.models.domain.tumor_type import TumorType

        # should have SubjectId + all TumorType data fields
        expected_cols = {"SubjectId"} | set(TumorType.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_tumor_fields(self, tumor_type_fixture):
        h = ImpressHarmonizer(data=tumor_type_fixture, trial_id="T")
        df = h._process_tumor_type()

        row = df.filter(pl.col("SubjectId") == "tumor1_multi_subtypes")
        assert row.item(0, "icd10_code") == "C30"
        assert row.item(0, "icd10_description") == "tumor1"
        assert row.item(0, "main_tumor_type") == "tumor1_subtype1"
        assert row.item(0, "main_tumor_type_code") == 50
        assert row.item(0, "cohort_tumor_type") == "tumor1_subtype2"
        assert row.item(0, "other_tumor_type") == "tumor1_subtype3"

        # crc_subtype_slot2
        row = df.filter(pl.col("SubjectId") == "crc_subtype_slot2")
        assert row.item(0, "icd10_code") == "C40.50"
        assert row.item(0, "icd10_description") == "CRC"
        assert row.item(0, "main_tumor_type") == "CRC_subtype"
        assert row.item(0, "main_tumor_type_code") == 40
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") is None

        # tumor2_dual_subtypes
        row = df.filter(pl.col("SubjectId") == "tumor2_dual_subtypes")
        assert row.item(0, "icd10_code") == "C07"
        assert row.item(0, "icd10_description") == "tumor2"
        assert row.item(0, "main_tumor_type") == "tumor2_subtype1"
        assert row.item(0, "main_tumor_type_code") == 70
        assert row.item(0, "cohort_tumor_type") == "tumor2_subtype2"
        assert row.item(0, "other_tumor_type") is None

        # tumor3_sp_subtype
        row = df.filter(pl.col("SubjectId") == "tumor3_sp_subtype")
        assert row.item(0, "icd10_code") == "C70.1"
        assert row.item(0, "icd10_description") == "tumor3"
        assert row.item(0, "main_tumor_type") == "tumor3_subtype1"
        assert row.item(0, "main_tumor_type_code") == 10
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") == "tumor3_subtype2"

        # tumor4_slot2_and_sp
        row = df.filter(pl.col("SubjectId") == "tumor4_slot2_and_sp")
        assert row.item(0, "icd10_code") == "C23.20"
        assert row.item(0, "icd10_description") == "tumor4"
        assert row.item(0, "main_tumor_type") == "tumor4_subtype1"
        assert row.item(0, "main_tumor_type_code") == 30
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") == "tumor4_subtype2"


class TestProcessStudyDrugsDF:
    def test_returns_expected_columns(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        df = h._process_study_drugs()

        from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs

        expected_cols = {"SubjectId"} | set(StudyDrugs.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_drug_fields(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        df = h._process_study_drugs()

        row = df.filter(pl.col("SubjectId") == "sd_from_alt_slots")
        assert row.item(0, "primary_treatment_drug") == "Traztuzumab"
        assert row.item(0, "primary_treatment_drug_code") == 31
        assert row.item(0, "secondary_treatment_drug") == "Tafinlar"
        assert row.item(0, "secondary_treatment_drug_code") == 10

        # sd1_match_sd2_match
        row = df.filter(pl.col("SubjectId") == "sd1_match_sd2_match")
        assert row.item(0, "primary_treatment_drug") == "some drug"
        assert row.item(0, "primary_treatment_drug_code") == 99
        assert row.item(0, "secondary_treatment_drug") == "some drug 2"
        assert row.item(0, "secondary_treatment_drug_code") == 1

        # sd1_mismatch1_sd2_mismatch1_2
        row = df.filter(pl.col("SubjectId") == "sd1_mismatch1_sd2_mismatch1_2")
        assert row.item(0, "primary_treatment_drug") == "mismatch_1"
        assert row.item(0, "primary_treatment_drug_code") == 10
        assert row.item(0, "secondary_treatment_drug") == "mismatch_1_2"
        assert row.item(0, "secondary_treatment_drug_code") == 12

        # sd1_mismatch2_sd2_mismatch2_1
        row = df.filter(pl.col("SubjectId") == "sd1_mismatch2_sd2_mismatch2_1")
        assert row.item(0, "primary_treatment_drug") == "mismatch_2"
        assert row.item(0, "primary_treatment_drug_code") == 50
        assert row.item(0, "secondary_treatment_drug") == "mismatch_2_1"
        assert row.item(0, "secondary_treatment_drug_code") == 60

    def test_collision_filtered_out(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        df = h._process_study_drugs()

        # collision subject should be filtered out
        subject_ids = set(df["SubjectId"].to_list())
        assert "sd_collision" not in subject_ids


class TestAdverseEventsRunOne:
    def test_hydrates_simple_adverse_event(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        h._create_patients()
        h.run_one("adverse_events")

        (ae,) = h.patient_data["simple"].adverse_events
        assert ae.term == "Headache"
        assert ae.grade == 2
        assert ae.outcome == "Recovered"
        assert ae.was_serious is False
        assert ae.turned_serious_date is None
        assert ae.related_to_treatment_1_status == "related"
        assert ae.related_to_treatment_2_status == "unknown"
        assert ae.was_serious_grade_expected_treatment_1 is True
        assert ae.was_serious_grade_expected_treatment_2 is False
        assert ae.treatment_1_name == "Drug A"
        assert ae.treatment_2_name == "Drug B"
        assert ae.start_date == dt.date(1900, 1, 1)
        assert ae.end_date == dt.date(1900, 1, 3)

    def test_serious_event_fills_end_from_death(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        h._create_patients()
        h.run_one("adverse_events")

        (ae,) = h.patient_data["serious_fill_end_from_death"].adverse_events
        assert ae.was_serious is True
        assert ae.turned_serious_date == dt.date(1900, 1, 12)
        assert ae.end_date == dt.date(1900, 2, 1)
        assert ae.related_to_treatment_1_status == "not_related"
        assert ae.related_to_treatment_2_status is None
        assert ae.was_serious_grade_expected_treatment_1 is False
        assert ae.was_serious_grade_expected_treatment_2 is True

    def test_multiple_events_per_patient(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        h._create_patients()
        h.run_one("adverse_events")

        aes = h.patient_data["multi"].adverse_events
        assert {ae.term for ae in aes} == {"Nausea", "Vomiting"}

        n = next(a for a in aes if a.term == "Nausea")
        v = next(a for a in aes if a.term == "Vomiting")

        assert n.start_date == dt.date(1900, 3, 1)
        assert n.end_date == dt.date(1900, 3, 2)
        assert n.related_to_treatment_1_status == "unknown"
        assert n.related_to_treatment_2_status == "related"
        assert n.was_serious is False

        assert v.start_date == dt.date(1900, 3, 5)
        assert v.end_date is None
        assert v.related_to_treatment_1_status is None
        assert v.related_to_treatment_2_status == "not_related"
        assert v.was_serious is False

    def test_null_term_filtered_out(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        h._create_patients()
        h.run_one("adverse_events")

        assert h.patient_data["drop_null_term"].adverse_events == ()


class TestMedicalHistoriesRunOne:
    def test_hydrates_multiple_histories(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        mh_list = list(h.patient_data["two_rows"].medical_histories)
        assert len(mh_list) == 2

        # ordered by start_date ascending: 1900-07-02 < 1900-09-15
        assert mh_list[0].term == "something"
        assert mh_list[0].sequence_id == 5
        assert mh_list[0].start_date == dt.date(1900, 7, 2)
        assert mh_list[0].end_date == dt.date(1990, 1, 1)
        assert mh_list[0].status == "Past"
        assert mh_list[0].status_code == 3

        assert mh_list[1].term == "pain"
        assert mh_list[1].sequence_id == 1
        assert mh_list[1].start_date == dt.date(1900, 9, 15)
        assert mh_list[1].end_date is None
        assert mh_list[1].status == "Current/active"
        assert mh_list[1].status_code == 1

    def test_missing_data_returns_empty(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        assert h.patient_data["missing"].medical_histories == ()


class TestTumorAssessmentsRunOne:
    def test_recist_full(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["recist_full"].tumor_assessments[0]
        assert ta.assessment_type == "recist"
        assert ta.target_lesion_change_from_baseline == 0.25
        assert ta.target_lesion_change_from_nadir == 0
        assert ta.was_new_lesions_registered_after_baseline is True
        assert ta.date == dt.date(1900, 1, 10)
        assert ta.recist_response == "PR"
        assert ta.recist_date_of_progression == dt.date(1900, 2, 1)
        assert ta.event_id == "V01"

    def test_no_signal_returns_empty(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        assert h.patient_data["no_signal"].tumor_assessments == ()


class TestProcessDateOfDeathDF:
    def test_returns_expected_columns(self, date_of_death_fixture):
        h = ImpressHarmonizer(data=date_of_death_fixture, trial_id="T")
        df = h._process_date_of_death()

        assert set(df.columns) == {"SubjectId", "date_of_death"}

    def test_extracts_death_dates(self, date_of_death_fixture):
        h = ImpressHarmonizer(data=date_of_death_fixture, trial_id="T")
        df = h._process_date_of_death()

        def get_date(subject_id: str) -> dt.date | None:
            row = df.filter(pl.col("SubjectId") == subject_id)
            if row.height == 0:
                return None
            return row.item(0, "date_of_death")

        assert get_date("both_partial_nk") == dt.date(1990, 7, 2)
        assert get_date("eos_valid_fu_partial_nk") == dt.date(2016, 9, 15)
        assert get_date("fu_valid_only") == dt.date(1900, 1, 1)
        assert get_date("eos_valid_fu_partial_upper_nk") == dt.date(1999, 9, 9)

    def test_invalid_dates_filtered_out(self, date_of_death_fixture):
        h = ImpressHarmonizer(data=date_of_death_fixture, trial_id="T")
        df = h._process_date_of_death()

        subject_ids = set(df["SubjectId"].to_list())
        assert "both_invalid" not in subject_ids


class TestProcessBiomarkersDF:
    def test_returns_expected_columns(self, biomarkers_fixture):
        h = ImpressHarmonizer(data=biomarkers_fixture, trial_id="T")
        df = h._process_biomarkers()

        from omop_etl.harmonization.models.domain.biomarkers import Biomarkers

        expected_cols = {"SubjectId"} | set(Biomarkers.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_biomarker_values(self, biomarkers_fixture):
        h = ImpressHarmonizer(data=biomarkers_fixture, trial_id="T")
        df = h._process_biomarkers()

        row = df.filter(pl.col("SubjectId") == "mut_braf_activating")
        assert row.item(0, "gene_and_mutation") == "BRAF activating mutations"
        assert row.item(0, "gene_and_mutation_code") == 21
        assert row.item(0, "cohort_target_name") == "BRAF Non-V600 activating mutations"
        assert row.item(0, "cohort_target_mutation") == "BRAF Non-V600 activating mutations"
        assert row.item(0, "date") == dt.date(1900, 7, 15)

        # some_info_no_mut
        row = df.filter(pl.col("SubjectId") == "some_info_no_mut")
        assert row.item(0, "gene_and_mutation") is None
        assert row.item(0, "gene_and_mutation_code") is None
        assert row.item(0, "cohort_target_name") == "some info"
        assert row.item(0, "cohort_target_mutation") is None
        assert row.item(0, "date") == dt.date(1980, 2, 15)

        # brca1_inactivating
        row = df.filter(pl.col("SubjectId") == "brca1_inactivating")
        assert row.item(0, "gene_and_mutation") == "BRCA1 inactivating mutation"
        assert row.item(0, "gene_and_mutation_code") == 2
        assert row.item(0, "cohort_target_name") == "BRCA1 stop-gain del exon 11"
        assert row.item(0, "cohort_target_mutation") == "BRCA1 stop-gain deletion"
        assert row.item(0, "date") is None

        # sdhaf2_mut
        row = df.filter(pl.col("SubjectId") == "sdhaf2_mut")
        assert row.item(0, "gene_and_mutation") == "SDHAF2 mutation"
        assert row.item(0, "gene_and_mutation_code") == -1
        assert row.item(0, "cohort_target_name") == "more info"
        assert row.item(0, "cohort_target_mutation") is None
        assert row.item(0, "date") == dt.date(1999, 7, 11)

        # code_only_misc
        row = df.filter(pl.col("SubjectId") == "code_only_misc")
        assert row.item(0, "gene_and_mutation") is None
        assert row.item(0, "gene_and_mutation_code") == 10
        assert row.item(0, "cohort_target_name") is None
        assert row.item(0, "cohort_target_mutation") == "some other info"
        assert row.item(0, "date") is None


class TestProcessLostToFollowupDF:
    def test_returns_expected_columns(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        df = h._process_lost_to_followup()

        from omop_etl.harmonization.models.domain.followup import FollowUp

        expected_cols = {"SubjectId"} | set(FollowUp.data_fields())
        assert set(df.columns) == expected_cols

    def test_ltfu_status_and_dates(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        df = h._process_lost_to_followup()

        def get_row(sid: str):
            return df.filter(pl.col("SubjectId") == sid)

        # alive -> not lost
        r = get_row("alive_valid")
        assert r.item(0, "lost_to_followup") is False
        assert r.item(0, "date_lost_to_followup") is None

        # death -> not lost
        r = get_row("death_valid")
        assert r.item(0, "lost_to_followup") is False

        # ltfu -> lost with date
        r = get_row("ltfu_valid")
        assert r.item(0, "lost_to_followup") is True
        assert r.item(0, "date_lost_to_followup") == dt.date(1900, 1, 1)


class TestProcessEvaluabilityDF:
    @pytest.mark.parametrize(
        "patient_id,expected",
        [
            ("iv_single", False),
            ("iv_two_rows_a", False),
            ("iv_two_rows_b", True),
            ("iv_then_oral", True),
            ("iv_two_then_oral_short", True),
            ("oral_ongoing_a", False),
            ("oral_only", False),
            ("iv_two_courses", False),
        ],
    )
    def test_evaluability_values(self, evaluability_fixture, patient_id, expected):
        h = ImpressHarmonizer(data=evaluability_fixture, trial_id="T")
        df = h._process_evaluable_for_efficacy_analysis()

        row = df.filter(pl.col("SubjectId") == patient_id)
        assert row.item(0, "evaluable_for_efficacy_analysis") is expected


class TestProcessEcogBaselineDF:
    def test_returns_expected_columns(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        df = h._process_ecog_baseline()

        from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline

        expected_cols = {"SubjectId"} | set(EcogBaseline.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_ecog_values(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        df = h._process_ecog_baseline()

        row = df.filter(pl.col("SubjectId") == "all_data")
        assert row.item(0, "description") == "all"
        assert row.item(0, "grade") == 1
        assert row.item(0, "date") == dt.date(1900, 1, 1)

    def test_wrong_event_id_filtered(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        df = h._process_ecog_baseline()

        # V02 event ID should be filtered out
        subject_ids = set(df["SubjectId"].to_list())
        assert "wrong_event_id" not in subject_ids


class TestProcessMedicalHistoriesDF:
    def test_returns_expected_columns(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        from omop_etl.harmonization.models.domain.medical_history import MedicalHistory

        expected_cols = {"SubjectId"} | set(MedicalHistory.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_medical_history_values(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        rows = df.filter(pl.col("SubjectId") == "two_rows")
        assert rows.height == 2

        pain = rows.filter(pl.col("term") == "pain")
        assert pain.item(0, "sequence_id") == 1
        assert pain.item(0, "status") == "Current/active"
        assert pain.item(0, "status_code") == 1

    def test_missing_data_returns_no_rows(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        missing = df.filter(pl.col("SubjectId") == "missing")
        assert missing.height == 0


class TestProcessAdverseEventNumberDF:
    def test_returns_expected_columns(self, adverse_event_number_fixture):
        h = ImpressHarmonizer(data=adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_adverse_events()

        assert "SubjectId" in df.columns
        assert "number_of_adverse_events" in df.columns

    def test_counts_adverse_events(self, adverse_event_number_fixture):
        h = ImpressHarmonizer(data=adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_adverse_events()

        def get_count(sid: str) -> int:
            return df.filter(pl.col("SubjectId") == sid).item(0, "number_of_adverse_events")

        assert get_count("2_events") == 2
        assert get_count("3_events") == 3
        assert get_count("1_event_code_only") == 1
        assert get_count("1_event_term_only") == 1
        assert get_count("missing_data") == 0


class TestProcessSeriousAdverseEventNumberDF:
    def test_counts_serious_events(self, serious_adverse_event_number_fixture):
        h = ImpressHarmonizer(data=serious_adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_serious_adverse_events()

        def get_count(sid: str) -> int:
            return df.filter(pl.col("SubjectId") == sid).item(0, "number_of_serious_adverse_events")

        assert get_count("1_event_two_rows") == 1
        assert get_count("2_events_with_missing_fields") == 2
        assert get_count("1_event_missing_date") == 1
        assert get_count("0_events_missing_date") == 0
        assert get_count("0_events_no_data") == 0


class TestProcessBaselineTumorAssessmentDF:
    def test_returns_expected_columns(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline

        expected_cols = {"SubjectId"} | set(TumorAssessmentBaseline.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_assessment_values(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        row = df.filter(pl.col("SubjectId") == "vituma_only")
        assert row.item(0, "assessment_type") == "PD"
        assert row.item(0, "assessment_date") == dt.date(2020, 1, 2)

        row = df.filter(pl.col("SubjectId") == "ra_valid")
        assert row.item(0, "target_lesion_size") == 12
        assert row.item(0, "target_lesion_nadir") == 12


class TestProcessPreviousTreatmentsDF:
    def test_returns_expected_columns(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        df = h._process_previous_treatments()

        from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments

        expected_cols = {"SubjectId"} | set(PreviousTreatments.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_treatment_values(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        df = h._process_previous_treatments()

        row = df.filter(pl.col("SubjectId") == "has_treatment")
        assert row.item(0, "treatment") == "abc"
        assert row.item(0, "treatment_code") == 2
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 2)
        assert row.item(0, "additional_treatment") == "def"

    def test_missing_treatment_filtered(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        df = h._process_previous_treatments()

        subject_ids = set(df["SubjectId"].to_list())
        assert "missing_treatment" not in subject_ids


class TestProcessTreatmentStartDateDF:
    def test_returns_expected_columns(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_date()

        assert "SubjectId" in df.columns
        assert "treatment_start_date" in df.columns

    def test_extracts_earliest_date(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_date()

        row = df.filter(pl.col("SubjectId") == "multirow")
        assert row.item(0, "treatment_start_date") == dt.date(1900, 1, 1)

        row = df.filter(pl.col("SubjectId") == "single_row")
        assert row.item(0, "treatment_start_date") == dt.date(1900, 1, 2)

    def test_missing_name_returns_null(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_date()

        row = df.filter(pl.col("SubjectId") == "missing_treatment_none")
        assert row.height == 0 or row.item(0, "treatment_start_date") is None


class TestProcessEndOfTreatmentDateDF:
    def test_returns_expected_columns(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        df = h._process_end_of_treatment_date()

        assert "SubjectId" in df.columns
        assert "end_of_treatment_date" in df.columns

    def test_extracts_latest_date_with_precedence(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        df = h._process_end_of_treatment_date()

        # EOT takes precedence
        row = df.filter(pl.col("SubjectId") == "eot_precedence")
        assert row.item(0, "end_of_treatment_date") == dt.date(1900, 1, 2)

        # multirow picks max
        row = df.filter(pl.col("SubjectId") == "multirow")
        assert row.item(0, "end_of_treatment_date") == dt.date(1900, 1, 1)


class TestProcessTreatmentStartLastCycleDF:
    def test_returns_expected_columns(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_last_cycle()

        assert "SubjectId" in df.columns
        assert "treatment_start_last_cycle" in df.columns

    def test_extracts_latest_cycle_start(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_last_cycle()

        row = df.filter(pl.col("SubjectId") == "two_rows_both_valid")
        assert row.item(0, "treatment_start_last_cycle") == dt.date(1900, 1, 2)


class TestProcessTreatmentCycleDF:
    def test_returns_expected_columns(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        from omop_etl.harmonization.models.domain.treatment_cycle import TreatmentCycle

        expected_cols = {"SubjectId"} | set(TreatmentCycle.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_iv_cycle(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        rows = df.filter(pl.col("SubjectId") == "iv_two_cycles")
        assert rows.height == 2
        assert set(rows["cycle_type"].to_list()) == {"IV"}

    def test_extracts_oral_cycle(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        row = df.filter(pl.col("SubjectId") == "oral_single")
        assert row.item(0, "cycle_type") == "oral"
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 20)

    def test_null_name_filtered(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_no_name" not in subject_ids


class TestProcessConcomitantMedicationDF:
    def test_returns_expected_columns(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication

        expected_cols = {"SubjectId"} | set(ConcomitantMedication.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_medication_values(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        row = df.filter(pl.col("SubjectId") == "all_fields")
        assert row.item(0, "medication_name") == "Paracetamol"
        assert row.item(0, "was_taken_due_to_medical_history_event") is True
        assert row.item(0, "was_taken_due_to_adverse_event") is True
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 10)

    def test_null_name_filtered(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_null_name" not in subject_ids


class TestProcessHasAnyAdverseEventsDF:
    def test_returns_expected_columns(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        df = h._process_has_any_adverse_events()

        assert "SubjectId" in df.columns
        assert "has_any_adverse_events" in df.columns

    def test_detects_adverse_events(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        df = h._process_has_any_adverse_events()

        def get_flag(sid: str) -> bool:
            return df.filter(pl.col("SubjectId") == sid).item(0, "has_any_adverse_events")

        assert get_flag("none_all_empty") is False
        assert get_flag("none_whitespace_only") is False
        assert get_flag("text_only") is True
        assert get_flag("date_only") is True
        assert get_flag("grade_only") is True


class TestProcessAdverseEventsDF:
    def test_returns_expected_columns(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent

        expected_cols = {"SubjectId"} | set(AdverseEvent.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_adverse_event_values(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        row = df.filter(pl.col("SubjectId") == "simple")
        assert row.item(0, "term") == "Headache"
        assert row.item(0, "grade") == 2
        assert row.item(0, "outcome") == "Recovered"
        assert row.item(0, "was_serious") is False
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 3)

    def test_null_term_filtered(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_null_term" not in subject_ids


class TestProcessTumorAssessmentsDF:
    def test_returns_expected_columns(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment

        expected_cols = {"SubjectId"} | set(TumorAssessment.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_recist_values(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "recist_full")
        assert row.item(0, "assessment_type") == "recist"
        assert row.item(0, "target_lesion_change_from_baseline") == 0.25
        assert row.item(0, "recist_response") == "PR"
        assert row.item(0, "event_id") == "V01"

    def test_extracts_rano_values(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "rano_full")
        assert row.item(0, "assessment_type") == "rano"
        assert row.item(0, "rano_response") == "RANO-PR"


class TestProcessBestOverallResponseDF:
    def test_returns_expected_columns(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        from omop_etl.harmonization.models.domain.best_overall_response import BestOverallResponse

        expected_cols = {"SubjectId"} | set(BestOverallResponse.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_response_values(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "recist_only")
        assert row.item(0, "response") == "PR"
        assert row.item(0, "code") == 20
        assert row.item(0, "date") == dt.date(1900, 1, 10)

        row = df.filter(pl.col("SubjectId") == "rano_only")
        assert row.item(0, "response") == "RANO-PR"
        assert row.item(0, "code") == 15


class TestProcessClinicalBenefitDF:
    def test_returns_expected_columns(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        df = h._process_clinical_benefit()

        assert "SubjectId" in df.columns
        assert "has_clinical_benefit_at_week16" in df.columns

    def test_detects_clinical_benefit(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        df = h._process_clinical_benefit()

        def get_benefit(sid: str) -> bool | None:
            row = df.filter(pl.col("SubjectId") == sid)
            if row.height == 0:
                return None
            return row.item(0, "has_clinical_benefit_at_week16")

        assert get_benefit("recist_le3") is True
        assert get_benefit("recist_gt3") is False
        assert get_benefit("irecist_le3") is True
        assert get_benefit("rano_le3") is True


class TestProcessEotReasonDF:
    def test_returns_expected_columns(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        df = h._process_eot_reason()

        assert "SubjectId" in df.columns
        assert "end_of_treatment_reason" in df.columns

    def test_extracts_reason_with_trim(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        df = h._process_eot_reason()

        row = df.filter(pl.col("SubjectId") == "reason_trim")
        assert row.item(0, "end_of_treatment_reason") == "Progression"

    def test_empty_reasons_filtered(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        df = h._process_eot_reason()

        subject_ids = set(df["SubjectId"].to_list())
        assert "reason_empty_string" not in subject_ids
        assert "reason_whitespace_only" not in subject_ids
        assert "reason_none" not in subject_ids


class TestScalarSpecsRunOne:
    def test_cohort_name_hydration(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        h._create_patients()
        h.run_one("cohort_name")

        assert h.patient_data["cohort_hit_1"].cohort_name == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"
        assert h.patient_data["cohort_empty_1"].cohort_name is None

    def test_sex_hydration(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        h._create_patients()
        h.run_one("sex")

        assert h.patient_data["female_titlecase"].sex == "female"
        assert h.patient_data["male_titlecase"].sex == "male"
        assert h.patient_data["invalid_value"].sex is None

    def test_age_hydration(self, age_fixture):
        h = ImpressHarmonizer(data=age_fixture, trial_id="T")
        h._create_patients()
        h.run_one("age")

        assert h.patient_data["birth_full_tx_full"].age == 89
        assert h.patient_data["birth_year_tx_full"].age == 39


class TestEvaluabilityRunOne:
    @pytest.mark.parametrize(
        "patient_id,expected",
        [
            pytest.param("iv_single", False, id="one IV row: not evaluable"),
            pytest.param("iv_two_rows_a", False, id="two IV rows, gap lt 21: not evaluable"),
            pytest.param("iv_two_rows_b", True, id="two IV rows, gap gte 21: evaluable"),
            pytest.param("iv_then_oral", True, id="IV none, oral sufficient: evaluable"),
            pytest.param("iv_two_then_oral_short", True, id="IV sufficient, oral not: evaluable"),
            pytest.param("oral_ongoing_a", False, id="oral missing end: not evaluable"),
            pytest.param("oral_ongoing_b", False, id="oral end not a date: not evaluable"),
            pytest.param("oral_missing_start_a", False, id="oral start not a date: not evaluable"),
            pytest.param("oral_missing_start_b", False, id="oral missing start: not evaluable"),
            pytest.param("iv_then_iv_empty_date", False, id="IV one start null: not evaluable"),
            pytest.param("iv_two_rows_gap", False, id="IV gap lte 21: not evaluable"),
            pytest.param("oral_only", False, id="oral length lte 28: not evaluable"),
            pytest.param("iv_two_courses", False, id="IV gap across drugs: not evaluable"),
            pytest.param("iv_with_cyclic_and_non", False, id="IV one invalid row: not evaluable"),
            pytest.param("oral_non_cyclic", False, id="oral sufficient but invalid: not evaluable"),
        ],
    )
    def test_evaluability_cases(self, evaluability_fixture, patient_id, expected):
        h = ImpressHarmonizer(data=evaluability_fixture, trial_id="T")
        h._create_patients()
        h.run_one("evaluable_for_efficacy_analysis")

        result = h.patient_data[patient_id].evaluable_for_efficacy_analysis
        assert result is expected


class TestBaselineTumorAssessmentRunOne:
    def test_missing_data_returns_none(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        assert h.patient_data["missing_data"].tumor_assessment_baseline is None
        assert h.patient_data["vi_none"].tumor_assessment_baseline is None
        assert h.patient_data["no_ntl"].tumor_assessment_baseline is None
        assert h.patient_data["rntmnt_ntl_wrong_event_id"].tumor_assessment_baseline is None
        assert h.patient_data["rcnt_invalid_int"].tumor_assessment_baseline is None
        assert h.patient_data["missing_baseline_size"].tumor_assessment_baseline is None

    def test_vituma_only(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["vituma_only"].tumor_assessment_baseline
        assert ta.assessment_type == "PD"
        assert ta.assessment_date == dt.date(2020, 1, 2)

    def test_vituma_2_only(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["vituma__2_only"].tumor_assessment_baseline
        assert ta.assessment_type == "CR"
        assert ta.assessment_date == dt.date(2020, 1, 3)

    def test_vi_no_date(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["vi_no_date"].tumor_assessment_baseline
        assert ta.assessment_type == "SD"

    def test_both_ntl_cols(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["both_ntl_cols"].tumor_assessment_baseline
        assert ta.off_target_lesions_number == 5
        assert ta.off_target_lesion_measurement_date == dt.date(2020, 2, 1)

    def test_rntmnt_only(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["rntmnt_only"].tumor_assessment_baseline
        assert ta.off_target_lesions_number == 4
        assert ta.off_target_lesion_measurement_date == dt.date(2020, 2, 2)

    def test_rcnt_only(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["rcnt_only"].tumor_assessment_baseline
        assert ta.off_target_lesions_number == 3
        assert ta.off_target_lesion_measurement_date == dt.date(2020, 2, 4)

    def test_ntl_no_date(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["ntl_no_date"].tumor_assessment_baseline
        assert ta.off_target_lesions_number == 6
        assert ta.off_target_lesion_measurement_date is None

    def test_ra_valid(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["ra_valid"].tumor_assessment_baseline
        assert ta.target_lesion_size == 12
        assert ta.target_lesion_nadir == 12
        assert ta.target_lesion_measurement_date == dt.date(2018, 7, 27)

    def test_rnrsp_valid(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["rnrsp_valid"].tumor_assessment_baseline
        assert ta.target_lesion_size == 20
        assert ta.target_lesion_nadir == 18
        assert ta.target_lesion_measurement_date == dt.date(2019, 1, 1)

    def test_ra_no_date(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["ra_no_date"].tumor_assessment_baseline
        assert ta.target_lesion_size == 8
        assert ta.target_lesion_nadir == 7
        assert ta.target_lesion_measurement_date is None

    def test_rnrsp_no_date(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["rnrsp_no_date"].tumor_assessment_baseline
        assert ta.target_lesion_size == 9
        assert ta.target_lesion_nadir == 8
        assert ta.target_lesion_measurement_date is None

    def test_multiple_rows(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("baseline_tumor_assessment")

        ta = h.patient_data["multiple_rows"].tumor_assessment_baseline
        assert ta.target_lesion_size == 9
        assert ta.target_lesion_nadir == 9
        assert ta.target_lesion_measurement_date == dt.date(2020, 1, 1)


class TestPreviousTreatmentsRunOne:
    def test_empty_returns_empty_tuple(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("previous_treatments")

        assert h.patient_data["empty"].previous_treatments == ()
        assert h.patient_data["missing_treatment"].previous_treatments == ()

    def test_has_treatment(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("previous_treatments")

        pt = h.patient_data["has_treatment"].previous_treatments[0]
        assert pt.patient_id == "has_treatment"
        assert pt.treatment == "abc"
        assert pt.treatment_code == 2
        assert pt.start_date == dt.date(1900, 1, 1)
        assert pt.end_date == dt.date(1900, 1, 2)
        assert pt.additional_treatment == "def"

    def test_missing_partial(self, previous_treatment_fixture):
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        h._create_patients()
        h.run_one("previous_treatments")

        pts = h.patient_data["missing_partial"].previous_treatments
        assert len(pts) == 2

        assert pts[0].patient_id == "missing_partial"
        assert pts[0].treatment == "abc"
        assert pts[0].treatment_code is None
        assert pts[0].start_date == dt.date(1900, 1, 1)
        assert pts[0].end_date is None
        assert pts[0].additional_treatment is None

        assert pts[1].patient_id == "missing_partial"
        assert pts[1].treatment == "def"
        assert pts[1].treatment_code is None
        assert pts[1].start_date == dt.date(1900, 1, 3)
        assert pts[1].end_date is None
        assert pts[1].additional_treatment is None


class TestTreatmentCyclesRunOne:
    def test_drop_no_name(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_cycle")

        assert h.patient_data["drop_no_name"].treatment_cycles == ()

    def test_iv_two_cycles(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_cycle")

        cycles = h.patient_data["iv_two_cycles"].treatment_cycles
        assert len(cycles) == 2

        cycle_1, cycle_2 = cycles[0], cycles[1]
        assert cycle_1.cycle_type == "IV" and cycle_2.cycle_type == "IV"
        assert cycle_1.start_date == dt.date(1900, 1, 1)
        assert cycle_1.end_date == dt.date(1900, 1, 9)
        assert cycle_1.was_total_dose_delivered is True
        assert cycle_1.iv_dose_prescribed == "100" and cycle_1.iv_dose_prescribed_unit == "mg"

        assert cycle_2.start_date == dt.date(1900, 1, 10)
        assert cycle_2.end_date is None
        assert cycle_2.was_total_dose_delivered is False

    def test_oral_single(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_cycle")

        cycles = h.patient_data["oral_single"].treatment_cycles
        cycle = cycles[0]
        assert cycle.cycle_type == "oral"
        assert cycle.start_date == dt.date(1900, 1, 1)
        assert cycle.end_date == dt.date(1900, 1, 20)
        assert cycle.was_dose_administered_to_spec is True
        assert cycle.was_tablet_taken_to_prescription_in_previous_cycle is False
        assert cycle.oral_dose_prescribed_per_day == 200
        assert cycle.oral_dose_unit == "mg"
        assert cycle.number_of_days_tablet_not_taken == 3
        assert cycle.reason_tablet_not_taken == "nausea"

    def test_both_modalities(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_cycle")

        cycles = h.patient_data["both_modalities"].treatment_cycles
        assert len(cycles) == 2
        iv_cycle, oral_cycle = cycles[0], cycles[1]
        assert iv_cycle.cycle_type == "IV"
        assert oral_cycle.cycle_type == "oral"
        assert iv_cycle.end_date is None
        assert oral_cycle.end_date == dt.date(1900, 3, 30)

    def test_both_in_row_oral_precedence(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_cycle")

        cycles = h.patient_data["both_in_row"].treatment_cycles
        cycle = cycles[0]
        assert cycle.cycle_type == "oral"
        assert cycle.end_date == dt.date(1900, 1, 10)


class TestConcomitantMedicationRunOne:
    def test_null_name_filtered(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        h._create_patients()
        h.run_one("concomitant_medication")

        assert h.patient_data["drop_null_name"].concomitant_medications == ()
        assert h.patient_data["name_is_na"].concomitant_medications == ()

    def test_all_fields(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        h._create_patients()
        h.run_one("concomitant_medication")

        (cm,) = h.patient_data["all_fields"].concomitant_medications
        assert cm.medication_name == "Paracetamol"
        assert cm.was_taken_due_to_medical_history_event is True
        assert cm.was_taken_due_to_adverse_event is True
        assert cm.medication_ongoing is True
        assert cm.is_adverse_event_ongoing is True
        assert cm.start_date == dt.date(1900, 1, 1)
        assert cm.end_date == dt.date(1900, 1, 10)
        assert cm.sequence_id == 2

    def test_ordering(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        h._create_patients()
        h.run_one("concomitant_medication")

        cms = h.patient_data["ordering"].concomitant_medications
        assert [cm.sequence_id for cm in cms] == [1, 1, 2]
        assert [cm.start_date for cm in cms[:2]] == [dt.date(1900, 1, 1), dt.date(1900, 2, 1)]
        assert cms[2].medication_name == "Drug B"

    def test_ongoing_none(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        h._create_patients()
        h.run_one("concomitant_medication")

        (cm,) = h.patient_data["ongoing_none"].concomitant_medications
        assert cm.medication_ongoing is None
        assert cm.is_adverse_event_ongoing is None
        assert cm.was_taken_due_to_medical_history_event is None
        assert cm.was_taken_due_to_adverse_event is None
        assert cm.start_date is None and cm.end_date is None
        assert cm.sequence_id == 3


class TestHasAnyAdverseEventsRunOne:
    def test_none_all_empty(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        h._create_patients()
        h.run_one("has_any_adverse_events")

        assert h.patient_data["none_all_empty"].has_any_adverse_events is False
        assert h.patient_data["none_whitespace_only"].has_any_adverse_events is False

    def test_has_adverse_events(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        h._create_patients()
        h.run_one("has_any_adverse_events")

        assert h.patient_data["text_only"].has_any_adverse_events is True
        assert h.patient_data["date_only"].has_any_adverse_events is True
        assert h.patient_data["grade_only"].has_any_adverse_events is True
        assert h.patient_data["mixed"].has_any_adverse_events is True

    def test_multirow(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        h._create_patients()
        h.run_one("has_any_adverse_events")

        assert h.patient_data["multirow_any_true"].has_any_adverse_events is True
        assert h.patient_data["multirow_all_false"].has_any_adverse_events is False


class TestClinicalBenefitRunOne:
    def test_recist_le3(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["recist_le3"].has_clinical_benefit_at_week16 is True

    def test_recist_gt3(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["recist_gt3"].has_clinical_benefit_at_week16 is False

    def test_irecist_le3(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["irecist_le3"].has_clinical_benefit_at_week16 is True

    def test_rano_le3(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["rano_le3"].has_clinical_benefit_at_week16 is True

    def test_both_present(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["both_present"].has_clinical_benefit_at_week16 is True

    def test_v03_no_codes(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["v03_no_codes"].has_clinical_benefit_at_week16 is False

    def test_not_v03(self, clinical_benefit_fixture):
        h = ImpressHarmonizer(data=clinical_benefit_fixture, trial_id="T")
        h._create_patients()
        h.run_one("clinical_benefit")

        assert h.patient_data["not_v03"].has_clinical_benefit_at_week16 is None


class TestBestOverallResponseRunOne:
    def test_recist_only(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["recist_only"].best_overall_response
        assert bor.response == "PR"
        assert bor.code == 20
        assert bor.date == dt.date(1900, 1, 10)

    def test_irecist_only(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["irecist_only"].best_overall_response
        assert bor.response == "CR"
        assert bor.code == 4
        assert bor.date == dt.date(1900, 1, 5)

    def test_both_pick_irecist(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["both_pick_irecist"].best_overall_response
        assert bor.response == "iCR"
        assert bor.code == 4
        assert bor.date == dt.date(1900, 2, 1)

    def test_irecist_unconfirmed_drop(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["irecist_unconfirmed_drop"].best_overall_response
        assert bor.response == "PD"
        assert bor.code == 40
        assert bor.date == dt.date(1900, 3, 1)

    def test_rano_only(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["rano_only"].best_overall_response
        assert bor.response == "RANO-PR"
        assert bor.code == 15
        assert bor.date == dt.date(1900, 1, 20)

    def test_multi_best(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["multi_best"].best_overall_response
        assert bor.response == "PR"
        assert bor.code == 20
        assert bor.date == dt.date(1900, 2, 1)

    def test_irecist_ne_maps_96(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        h._create_patients()
        h.run_one("best_overall_response")

        bor = h.patient_data["irecist_ne_maps_96"].best_overall_response
        assert bor.response == "SD"
        assert bor.code == 30
        assert bor.date == dt.date(1900, 4, 1)


class TestTumorAssessmentsRunOneExtended:
    def test_irecist_full(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["irecist_full"].tumor_assessments[0]
        assert ta.assessment_type == "irecist"
        assert ta.target_lesion_change_from_baseline == 0
        assert ta.irecist_response == "iCR"
        assert ta.irecist_date_of_progression == dt.date(1900, 2, 10)
        assert ta.date == dt.date(1900, 1, 15)
        assert ta.event_id == "V02"

    def test_rano_full(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["rano_full"].tumor_assessments[0]
        assert ta.assessment_type == "rano"
        assert ta.target_lesion_change_from_baseline == -0.30
        assert ta.target_lesion_change_from_nadir == -0.10
        assert ta.was_new_lesions_registered_after_baseline is False
        assert ta.rano_response == "RANO-PR"
        assert ta.date == dt.date(1900, 1, 20)
        assert ta.event_id == "V03"

    def test_collision_irecist_wins(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["collision_irecist_wins"].tumor_assessments[0]
        assert ta.assessment_type == "irecist"

    def test_recist_bad_date(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["recist_bad_date"].tumor_assessments[0]
        assert ta.assessment_type == "recist"
        assert ta.date is None
        assert ta.recist_response == "SD"
        assert ta.event_id == "V06"

    def test_event_from_rnrsp(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_assessments")

        ta = h.patient_data["event_from_rnrsp"].tumor_assessments[0]
        assert ta.assessment_type == "rano"
        assert ta.date == dt.date(1900, 4, 1)
        assert ta.event_id == "V05"
        assert ta.was_new_lesions_registered_after_baseline is True


class TestEcogBaselineRunOneExtended:
    def test_no_event_id_returns_none(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        assert h.patient_data["no_event_id"].ecog_baseline is None

    def test_eventid_no_code(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        ins = h.patient_data["eventid_no_code"].ecog_baseline
        assert ins.description == "no code"
        assert ins.grade is None
        assert ins.date == dt.date(1900, 7, 1)

    def test_eventid_no_desc(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        ins = h.patient_data["eventid_no_desc"].ecog_baseline
        assert ins.description is None
        assert ins.grade == 2
        assert ins.date == dt.date(1900, 1, 15)

    def test_partial_data(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        ins = h.patient_data["partial_data"].ecog_baseline
        assert ins.description is None
        assert ins.grade == 1
        assert ins.date == dt.date(1900, 7, 15)

    def test_wrong_date(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        ins = h.patient_data["wrong_date"].ecog_baseline
        assert ins.description == "code"
        assert ins.grade == 4
        assert ins.date is None


class TestLostToFollowupRunOneExtended:
    def test_death_valid(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        h._create_patients()
        h.run_one("lost_to_followup")

        ins = h.patient_data["death_valid"].lost_to_followup
        assert ins.lost_to_followup is False
        assert ins.date_lost_to_followup is None

    def test_alive_lowercase_code_missing(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        h._create_patients()
        h.run_one("lost_to_followup")

        ins = h.patient_data["alive_lowercase_code_missing"].lost_to_followup
        assert ins.lost_to_followup is False
        assert ins.date_lost_to_followup is None

    def test_invalid_dates(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        h._create_patients()
        h.run_one("lost_to_followup")

        ins = h.patient_data["invalid_dates"].lost_to_followup
        assert ins.lost_to_followup is False
        assert ins.date_lost_to_followup is None


class TestMedicalHistoriesRunOneExtended:
    def test_ended(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        mh = h.patient_data["ended"].medical_histories[0]
        assert mh.term == "hypertension"
        assert mh.sequence_id == 2
        assert mh.start_date == dt.date(1901, 10, 2)
        assert mh.end_date == dt.date(1901, 11, 2)
        assert mh.status == "Past"
        assert mh.status_code == 3

    def test_ended_term_mismatch(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        mh = h.patient_data["ended_term_mismatch"].medical_histories[0]
        assert mh.term == "pain"
        assert mh.sequence_id == 1
        assert mh.start_date == dt.date(1840, 2, 2)
        assert mh.end_date is None
        assert mh.status == "Past"
        assert mh.status_code == 3

    def test_ended_code_mismatch(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        mh = h.patient_data["ended_code_mismatch"].medical_histories[0]
        assert mh.term == "rigor mortis"
        assert mh.sequence_id == 1
        assert mh.start_date == dt.date(1740, 2, 2)
        assert mh.end_date == dt.date(1940, 2, 2)
        assert mh.status == "Past"
        assert mh.status_code == 1


class TestEotReasonRunOne:
    def test_reason_trim(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        h._create_patients()
        h.run_one("eot_reason")

        assert h.patient_data["reason_trim"].end_of_treatment_reason == "Progression"

    def test_reason_empty_string(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        h._create_patients()
        h.run_one("eot_reason")

        assert h.patient_data["reason_empty_string"].end_of_treatment_reason is None

    def test_reason_whitespace_only(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        h._create_patients()
        h.run_one("eot_reason")

        assert h.patient_data["reason_whitespace_only"].end_of_treatment_reason is None

    def test_reason_none(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        h._create_patients()
        h.run_one("eot_reason")

        assert h.patient_data["reason_none"].end_of_treatment_reason is None

    def test_reason_multi_overwrite(self, eot_fixture):
        h = ImpressHarmonizer(data=eot_fixture, trial_id="T")
        h._create_patients()
        h.run_one("eot_reason")

        assert h.patient_data["reason_multi_overwrite"].end_of_treatment_reason == "Patient decision"


class TestTreatmentEndDateRunOne:
    def test_empty(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["empty"].end_of_treatment_date is None

    def test_missing_treatment_empty_str(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["missing_treatment_empty_str"].end_of_treatment_date is None

    def test_missing_treatment_eot_empty_str(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["missing_treatment_eot_empty_str"].end_of_treatment_date is None

    def test_multirow(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["multirow"].end_of_treatment_date == dt.date(1900, 1, 1)

    def test_eot_precedence(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["eot_precedence"].end_of_treatment_date == dt.date(1900, 1, 2)

    def test_invalid_row_doesnt_count(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        h._create_patients()
        h.run_one("end_of_treatment_date")

        assert h.patient_data["invalid_row_doesnt_count"].end_of_treatment_date == dt.date(1900, 1, 1)


class TestTreatmentStartDateRunOne:
    def test_empty(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_date")

        assert h.patient_data["empty"].treatment_start_date is None

    def test_missing_treatment_none(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_date")

        assert h.patient_data["missing_treatment_none"].treatment_start_date is None

    def test_missing_treatment_empty_str(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_date")

        assert h.patient_data["missing_treatment_empty_str"].treatment_start_date is None

    def test_multirow(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_date")

        assert h.patient_data["multirow"].treatment_start_date == dt.date(1900, 1, 1)

    def test_single_row(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_date")

        assert h.patient_data["single_row"].treatment_start_date == dt.date(1900, 1, 2)


class TestTreatmentStartLastCycleRunOne:
    def test_empty(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_last_cycle")

        assert h.patient_data["empty"].treatment_start_last_cycle is None

    def test_two_rows_both_valid(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_last_cycle")

        assert h.patient_data["two_rows_both_valid"].treatment_start_last_cycle == dt.date(1900, 1, 2)

    def test_one_invalid(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        h._create_patients()
        h.run_one("treatment_start_last_cycle")

        # not enforcing valid cycles so includes invalid starts as well
        assert h.patient_data["one_invalid"].treatment_start_last_cycle == dt.date(1900, 1, 2)


class TestImpressSpecContracts:
    """
    Contract-level test for ImpressHarmonizer specs to hydration to Patient.

    For each spec in `ImpressHarmonizer.SPECS`, runs the spec on its matching
    per-processor fixture via `run_one()` and asserts each declared spec
    executes through run_one() on its intended fixture and
    populates the declared target on at least one patient.
    """

    # maps each spec name to the conftest.py
    # fixture that provides input for that processor
    SPEC_TO_FIXTURE: ClassVar[dict[str, str | None]] = {
        # scalars
        "cohort_name": "cohort_name_fixture",
        "sex": "gender_fixture",
        "date_of_birth": "age_fixture",
        "age": "age_fixture",
        "date_of_death": "date_of_death_fixture",
        "has_any_adverse_events": "adverse_events_flag_fixture",
        "number_of_adverse_events": "adverse_event_number_fixture",
        "number_of_serious_adverse_events": "serious_adverse_event_number_fixture",
        "treatment_start_last_cycle": "last_treatment_start_fixture",
        "treatment_start_date": "treatment_start_fixture",
        "evaluable_for_efficacy_analysis": "evaluability_fixture",
        "clinical_benefit": "clinical_benefit_fixture",
        "eot_reason": "eot_fixture",
        "end_of_treatment_date": "treatment_stop_fixture",
        # singletons
        "tumor_type": "tumor_type_fixture",
        "study_drugs": "study_drugs_fixture",
        "biomarkers": "biomarkers_fixture",
        "lost_to_followup": "lost_to_followup_fixture",
        "ecog_baseline": "ecog_fixture",
        "baseline_tumor_assessment": "baseline_tumor_assessment_fixture",
        "best_overall_response": "best_overall_response_fixture",
        # collections
        "medical_histories": "medical_history_fixture",
        "previous_treatments": "previous_treatment_fixture",
        "treatment_cycle": "treatment_cycle_fixture",
        "concomitant_medication": "concomitant_medication_fixture",
        "adverse_events": "adverse_events_fixture",
        "tumor_assessments": "tumor_assessments_fixture",
        # TODO: c30 and eq5d have no per-processor fixtures yet:
        "c30": None,
        "eq5d": None,
    }

    @pytest.mark.parametrize("spec", ImpressHarmonizer.SPECS, ids=lambda s: s.name)
    def test_spec_populates_target(self, spec, request):
        if spec.name not in self.SPEC_TO_FIXTURE:
            pytest.fail(
                f"Spec {spec.name!r} has no entry in TestPipelineImpressIntegration.SPEC_TO_FIXTURE. Add an entry mapping it to a per-processor fixture"
            )

        fixture_name = self.SPEC_TO_FIXTURE[spec.name]
        if fixture_name is None:
            pytest.skip(f"Spec {spec.name!r} has no per-processor fixture (TODO)")

        df = request.getfixturevalue(fixture_name)
        h = ImpressHarmonizer(data=df, trial_id="T")
        h._create_patients()
        h.run_one(spec.name)

        assert h.patient_data, f"{fixture_name} created no patients"

        if isinstance(spec, ScalarSpec):
            target = spec.target_attr
            ok = any(getattr(p, target) is not None for p in h.patient_data.values())
        elif isinstance(spec, SingletonSpec):
            target = Patient.get_attr_for_type(spec.target_domain)
            ok = any(getattr(p, target) is not None for p in h.patient_data.values())
        elif isinstance(spec, CollectionSpec):
            target = Patient.get_attr_for_type(spec.target_domain)
            ok = any(len(getattr(p, target) or ()) > 0 for p in h.patient_data.values())
        else:
            raise AssertionError(f"Unknown spec type: {type(spec).__name__}")

        assert ok, (
            f"Spec {spec.name!r} did not populate {target!r} on any patient when "
            f"run on {fixture_name}. Either the processor produced empty output "
            f"for this fixture, or the spec is wired to the wrong target."
        )
