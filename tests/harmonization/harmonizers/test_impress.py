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


class TestProcessCohortName:
    def test_returns_expected_columns(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        assert set(df.columns) == {"SubjectId", "cohort_name"}

    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("cohort_hit_1", "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"),
            ("cohort_hit_2", "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"),
            pytest.param("cohort_empty_1", None, id="explicit None"),
            pytest.param("cohort_empty_2", None, id="empty string"),
            pytest.param("cohort_empty_3", None, id="missing value"),
        ],
    )
    def test_cohort_name_values(self, cohort_name_fixture, sid, expected):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "cohort_name")
        assert actual == expected


class TestProcessSex:
    def test_returns_expected_columns(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        assert set(df.columns) == {"SubjectId", "sex"}

    def test_normalizes_to_lowercase(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        values = set(df["sex"].to_list())
        assert values <= {"male", "female"}, "all values should be 'male' or 'female'"

    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("female_titlecase", "female"),
            ("male_titlecase", "male"),
            ("female_short_f", "female"),
            ("male_short_m", "male"),
            ("female_lowercase", "female"),
            ("male_lowercase", "male"),
            pytest.param("invalid_value", None, id="invalid value filtered out"),
            pytest.param("empty_value", None, id="empty string filtered out"),
        ],
    )
    def test_sex_values(self, gender_fixture, sid, expected):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "sex")
        assert actual == expected


class TestProcessAge:
    def test_returns_expected_columns(self, age_fixture):
        h = ImpressHarmonizer(data=age_fixture, trial_id="T")
        df = h._process_age()

        assert "SubjectId" in df.columns
        assert "age" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("birth_full_tx_full", 89),
            ("birth_year_tx_full", 39),
            ("birth_full_tx_full_recent", 20),
            ("birth_year_tx_year", 30),
            ("birth_full_tx_year_month", 9),
        ],
    )
    def test_age_values(self, age_fixture, sid, expected):
        h = ImpressHarmonizer(data=age_fixture, trial_id="T")
        df = h._process_age()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "age")
        assert actual == expected


class TestProcessTumorType:
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
        assert row.item(0, "date") == dt.date(2020, 3, 15)

        # crc_subtype_slot2
        row = df.filter(pl.col("SubjectId") == "crc_subtype_slot2")
        assert row.item(0, "icd10_code") == "C40.50"
        assert row.item(0, "icd10_description") == "CRC"
        assert row.item(0, "main_tumor_type") == "CRC_subtype"
        assert row.item(0, "main_tumor_type_code") == 40
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") is None
        assert row.item(0, "date") == dt.date(2019, 7, 15)

        # tumor2_dual_subtypes
        row = df.filter(pl.col("SubjectId") == "tumor2_dual_subtypes")
        assert row.item(0, "icd10_code") == "C07"
        assert row.item(0, "icd10_description") == "tumor2"
        assert row.item(0, "main_tumor_type") == "tumor2_subtype1"
        assert row.item(0, "main_tumor_type_code") == 70
        assert row.item(0, "cohort_tumor_type") == "tumor2_subtype2"
        assert row.item(0, "other_tumor_type") is None
        assert row.item(0, "date") is None

        # tumor3_sp_subtype
        row = df.filter(pl.col("SubjectId") == "tumor3_sp_subtype")
        assert row.item(0, "icd10_code") == "C70.1"
        assert row.item(0, "icd10_description") == "tumor3"
        assert row.item(0, "main_tumor_type") == "tumor3_subtype1"
        assert row.item(0, "main_tumor_type_code") == 10
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") == "tumor3_subtype2"
        assert row.item(0, "date") is None

        # tumor4_slot2_and_sp
        row = df.filter(pl.col("SubjectId") == "tumor4_slot2_and_sp")
        assert row.item(0, "icd10_code") == "C23.20"
        assert row.item(0, "icd10_description") == "tumor4"
        assert row.item(0, "main_tumor_type") == "tumor4_subtype1"
        assert row.item(0, "main_tumor_type_code") == 30
        assert row.item(0, "cohort_tumor_type") is None
        assert row.item(0, "other_tumor_type") == "tumor4_subtype2"
        assert row.item(0, "date") == dt.date(2021, 6, 1)


class TestProcessStudyDrugs:
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


class TestProcessDateOfDeath:
    def test_returns_expected_columns(self, date_of_death_fixture):
        h = ImpressHarmonizer(data=date_of_death_fixture, trial_id="T")
        df = h._process_date_of_death()

        assert set(df.columns) == {"SubjectId", "date_of_death"}

    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("both_partial_nk", dt.date(1990, 7, 2)),
            ("eos_valid_fu_partial_nk", dt.date(2016, 9, 15)),
            ("fu_valid_only", dt.date(1900, 1, 1)),
            ("eos_valid_fu_partial_upper_nk", dt.date(1999, 9, 9)),
            pytest.param("both_invalid", None, id="both source dates invalid -> filtered out"),
        ],
    )
    def test_date_of_death_values(self, date_of_death_fixture, sid, expected):
        h = ImpressHarmonizer(data=date_of_death_fixture, trial_id="T")
        df = h._process_date_of_death()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "date_of_death")
        assert actual == expected


class TestProcessBiomarkers:
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


class TestProcessLostToFollowup:
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

        # death -> not lost, no LTFU date
        r = get_row("death_valid")
        assert r.item(0, "lost_to_followup") is False
        assert r.item(0, "date_lost_to_followup") is None

        # ltfu -> lost with date
        r = get_row("ltfu_valid")
        assert r.item(0, "lost_to_followup") is True
        assert r.item(0, "date_lost_to_followup") == dt.date(1900, 1, 1)

    def test_alive_lowercase_code_missing_treated_as_not_lost(self, lost_to_followup_fixture):
        """A subject reporting as alive (status code missing or non-LTFU) should
        produce lost_to_followup=False with no LTFU date."""
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        df = h._process_lost_to_followup()

        row = df.filter(pl.col("SubjectId") == "alive_lowercase_code_missing")
        assert row.item(0, "lost_to_followup") is False
        assert row.item(0, "date_lost_to_followup") is None

    def test_invalid_dates_treated_as_not_lost(self, lost_to_followup_fixture):
        """Invalid date strings should not flag the subject as LTFU."""
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        df = h._process_lost_to_followup()

        row = df.filter(pl.col("SubjectId") == "invalid_dates")
        assert row.item(0, "lost_to_followup") is False
        assert row.item(0, "date_lost_to_followup") is None


class TestProcessEvaluability:
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
    def test_evaluability_values(self, evaluability_fixture, patient_id, expected):
        h = ImpressHarmonizer(data=evaluability_fixture, trial_id="T")
        df = h._process_evaluable_for_efficacy_analysis()

        row = df.filter(pl.col("SubjectId") == patient_id)
        assert row.item(0, "evaluable_for_efficacy_analysis") is expected


class TestProcessEcogBaseline:
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

        # subjects without a valid baseline event_id should be filtered out
        # entirely (so the eventual hydrated singleton is None on the patient).
        subject_ids = set(df["SubjectId"].to_list())
        assert "wrong_event_id" not in subject_ids
        assert "no_event_id" not in subject_ids

    def test_partial_data_subjects(self, ecog_fixture):
        """
        Subjects with partial baseline data should still appear in the
        with whatever fields can be parsed and null for the rest.
        """
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        df = h._process_ecog_baseline()

        # eventid_no_code: description present, grade missing, date present
        row = df.filter(pl.col("SubjectId") == "eventid_no_code")
        assert row.item(0, "description") == "no code"
        assert row.item(0, "grade") is None
        assert row.item(0, "date") == dt.date(1900, 7, 1)

        # eventid_no_desc: description missing, grade present, date present
        row = df.filter(pl.col("SubjectId") == "eventid_no_desc")
        assert row.item(0, "description") is None
        assert row.item(0, "grade") == 2
        assert row.item(0, "date") == dt.date(1900, 1, 15)

        # partial_data: description missing, grade present, date present
        row = df.filter(pl.col("SubjectId") == "partial_data")
        assert row.item(0, "description") is None
        assert row.item(0, "grade") == 1
        assert row.item(0, "date") == dt.date(1900, 7, 15)

        # wrong_date: description + grade present, date unparseable -> None
        row = df.filter(pl.col("SubjectId") == "wrong_date")
        assert row.item(0, "description") == "code"
        assert row.item(0, "grade") == 4
        assert row.item(0, "date") is None


class TestProcessMedicalHistories:
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

        # `pain` row: dates are MH_MHSTDAT="1900-09-NK" / MH_MHENDAT=""
        # so end_date should be None and start_date is parsed by the
        # NK-handling logic in PolarsParsers.
        pain = rows.filter(pl.col("term") == "pain")
        assert pain.item(0, "sequence_id") == 1
        assert pain.item(0, "start_date") == dt.date(1900, 9, 15)
        assert pain.item(0, "end_date") is None
        assert pain.item(0, "status") == "Current/active"
        assert pain.item(0, "status_code") == 1

        # `something` row: MH_MHSTDAT="1900-nk-02" / MH_MHENDAT="1990-01-01"
        something = rows.filter(pl.col("term") == "something")
        assert something.item(0, "sequence_id") == 5
        assert something.item(0, "start_date") == dt.date(1900, 7, 2)
        assert something.item(0, "end_date") == dt.date(1990, 1, 1)
        assert something.item(0, "status") == "Past"
        assert something.item(0, "status_code") == 3

    def test_missing_data_returns_no_rows(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        missing = df.filter(pl.col("SubjectId") == "missing")
        assert missing.height == 0

    def test_ended_subject_full_fields(self, medical_history_fixture):
        """`ended` has hypertension with both start and end dates."""
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        row = df.filter(pl.col("SubjectId") == "ended")
        assert row.item(0, "term") == "hypertension"
        assert row.item(0, "sequence_id") == 2
        assert row.item(0, "start_date") == dt.date(1901, 10, 2)
        assert row.item(0, "end_date") == dt.date(1901, 11, 2)
        assert row.item(0, "status") == "Past"
        assert row.item(0, "status_code") == 3

    def test_ended_term_mismatch_status_past_no_end(self, medical_history_fixture):
        """
        `ended_term_mismatch` has status="Past" with empty MH_MHENDAT, the
        end_date should be None despite the past status.
        """
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        row = df.filter(pl.col("SubjectId") == "ended_term_mismatch")
        assert row.item(0, "term") == "pain"
        assert row.item(0, "sequence_id") == 1
        assert row.item(0, "start_date") == dt.date(1840, 2, 2)
        assert row.item(0, "end_date") is None
        assert row.item(0, "status") == "Past"
        assert row.item(0, "status_code") == 3

    def test_ended_code_mismatch_keeps_status_code_one(self, medical_history_fixture):
        """
        `ended_code_mismatch` has status="Past" but status_code=1 (Current/active).
        The processor preserves both as-is, the mismatch is not corrected at this layer.
        """
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        df = h._process_medical_histories()

        row = df.filter(pl.col("SubjectId") == "ended_code_mismatch")
        assert row.item(0, "term") == "rigor mortis"
        assert row.item(0, "sequence_id") == 1
        assert row.item(0, "start_date") == dt.date(1740, 2, 2)
        assert row.item(0, "end_date") == dt.date(1940, 2, 2)
        assert row.item(0, "status") == "Past"
        assert row.item(0, "status_code") == 1


class TestProcessAdverseEventNumber:
    def test_returns_expected_columns(self, adverse_event_number_fixture):
        h = ImpressHarmonizer(data=adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_adverse_events()

        assert "SubjectId" in df.columns
        assert "number_of_adverse_events" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("2_events", 2),
            ("3_events", 3),
            ("1_event_code_only", 1),
            ("1_event_term_only", 1),
            ("missing_data", 0),
        ],
    )
    def test_number_of_adverse_events_values(self, adverse_event_number_fixture, sid, expected):
        h = ImpressHarmonizer(data=adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_adverse_events()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "number_of_adverse_events")
        assert actual == expected


class TestProcessSeriousAdverseEventNumber:
    @pytest.mark.parametrize(
        "sid, expected",
        [
            ("1_event_two_rows", 1),
            ("2_events_with_missing_fields", 2),
            ("1_event_missing_date", 1),
            ("0_events_missing_date", 0),
            ("0_events_no_data", 0),
        ],
    )
    def test_number_of_serious_adverse_events_values(self, serious_adverse_event_number_fixture, sid, expected):
        h = ImpressHarmonizer(data=serious_adverse_event_number_fixture, trial_id="T")
        df = h._process_number_of_serious_adverse_events()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "number_of_serious_adverse_events")
        assert actual == expected


class TestProcessBaselineTumorAssessment:
    def test_returns_expected_columns(self, baseline_tumor_assessment_fixture):
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline

        expected_cols = {"SubjectId"} | set(TumorAssessmentBaseline.data_fields())
        assert set(df.columns) == expected_cols

    def test_filters_subjects_without_baseline(self, baseline_tumor_assessment_fixture):
        """
        Subjects without enough baseline signal to construct an assessment
        should be filtered out at the processor level. After hydration the
        Patient's tumor_assessment_baseline ends up None for these.
        """
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        subject_ids = set(df["SubjectId"].to_list())
        for sid in (
            "missing_data",
            "vi_none",
            "no_ntl",
            "rntmnt_ntl_wrong_event_id",
            "rcnt_invalid_int",
            "missing_baseline_size",
        ):
            assert sid not in subject_ids, f"{sid!r} should be filtered out"

    def test_extracts_assessment_type_from_vituma(self, baseline_tumor_assessment_fixture):
        """
        Assessment type and date come from one of two source columns:
        VI_VITUMA (preferred) or VI_VITUMA__2 (fallback). `vi_no_date` is
        a vituma source with the assessment_date missing.
        """
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        # vituma_only: VI_VITUMA primary source
        row = df.filter(pl.col("SubjectId") == "vituma_only")
        assert row.item(0, "assessment_type") == "PD"
        assert row.item(0, "assessment_date") == dt.date(2020, 1, 2)

        # vituma__2_only: VI_VITUMA__2 fallback source (note double underscore)
        row = df.filter(pl.col("SubjectId") == "vituma__2_only")
        assert row.item(0, "assessment_type") == "CR"
        assert row.item(0, "assessment_date") == dt.date(2020, 1, 3)

        # vi_no_date: assessment_type populated, assessment_date null
        row = df.filter(pl.col("SubjectId") == "vi_no_date")
        assert row.item(0, "assessment_type") == "SD"

    def test_extracts_off_target_lesions_from_multiple_sources(self, baseline_tumor_assessment_fixture):
        """
        Off-target lesion fields can come from RNTMNT_RNTMNTNOB, RCNT_RCNTNOB,
        or both. The processor picks the appropriate source per subject.
        `ntl_no_date` has the count but no measurement date.
        """
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        # both_ntl_cols: both sources present, picks one
        row = df.filter(pl.col("SubjectId") == "both_ntl_cols")
        assert row.item(0, "off_target_lesions_number") == 5
        assert row.item(0, "off_target_lesion_measurement_date") == dt.date(2020, 2, 1)

        # rntmnt_only: only RNTMNT source
        row = df.filter(pl.col("SubjectId") == "rntmnt_only")
        assert row.item(0, "off_target_lesions_number") == 4
        assert row.item(0, "off_target_lesion_measurement_date") == dt.date(2020, 2, 2)

        # rcnt_only: only RCNT source
        row = df.filter(pl.col("SubjectId") == "rcnt_only")
        assert row.item(0, "off_target_lesions_number") == 3
        assert row.item(0, "off_target_lesion_measurement_date") == dt.date(2020, 2, 4)

        # ntl_no_date: count present, date missing
        row = df.filter(pl.col("SubjectId") == "ntl_no_date")
        assert row.item(0, "off_target_lesions_number") == 6
        assert row.item(0, "off_target_lesion_measurement_date") is None

    def test_extracts_target_lesions_from_multiple_sources(self, baseline_tumor_assessment_fixture):
        """
        Target lesion size/nadir/measurement_date can come from RA_* or
        RNRSP_* source fields. `ra_no_date` and `rnrsp_no_date` have
        size/nadir but a missing measurement_date.
        """
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        # ra_valid: RA source, full data
        row = df.filter(pl.col("SubjectId") == "ra_valid")
        assert row.item(0, "target_lesion_size") == 12
        assert row.item(0, "target_lesion_nadir") == 12
        assert row.item(0, "target_lesion_measurement_date") == dt.date(2018, 7, 27)

        # rnrsp_valid: RNRSP source, full data
        row = df.filter(pl.col("SubjectId") == "rnrsp_valid")
        assert row.item(0, "target_lesion_size") == 20
        assert row.item(0, "target_lesion_nadir") == 18
        assert row.item(0, "target_lesion_measurement_date") == dt.date(2019, 1, 1)

        # ra_no_date: RA source, missing date
        row = df.filter(pl.col("SubjectId") == "ra_no_date")
        assert row.item(0, "target_lesion_size") == 8
        assert row.item(0, "target_lesion_nadir") == 7
        assert row.item(0, "target_lesion_measurement_date") is None

        # rnrsp_no_date: RNRSP source, missing date
        row = df.filter(pl.col("SubjectId") == "rnrsp_no_date")
        assert row.item(0, "target_lesion_size") == 9
        assert row.item(0, "target_lesion_nadir") == 8
        assert row.item(0, "target_lesion_measurement_date") is None

    def test_multiple_rows_subject(self, baseline_tumor_assessment_fixture):
        """
        `multiple_rows` has more than one input row; the processor picks
        consistent values for the singleton's target lesion fields.
        """
        h = ImpressHarmonizer(data=baseline_tumor_assessment_fixture, trial_id="T")
        df = h._process_baseline_tumor_assessment()

        row = df.filter(pl.col("SubjectId") == "multiple_rows")
        assert row.item(0, "target_lesion_size") == 9
        assert row.item(0, "target_lesion_nadir") == 9
        assert row.item(0, "target_lesion_measurement_date") == dt.date(2020, 1, 1)


class TestProcessPreviousTreatments:
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

    def test_missing_partial_keeps_both_rows_with_partial_values(self, previous_treatment_fixture):
        """
        `missing_partial` has two rows with treatment="abc" and "def". Both
        have CT_CTSPID set but no CT_CTTYPECD, CT_CTENDAT, or CT_CTTYPESP.
        The processor should emit both rows; the missing fields should be
        None on the resulting DataFrame.
        """
        h = ImpressHarmonizer(data=previous_treatment_fixture, trial_id="T")
        df = h._process_previous_treatments()

        rows = df.filter(pl.col("SubjectId") == "missing_partial")
        assert rows.height == 2

        abc = rows.filter(pl.col("treatment") == "abc")
        assert abc.item(0, "treatment_code") is None
        assert abc.item(0, "start_date") == dt.date(1900, 1, 1)
        assert abc.item(0, "end_date") is None
        assert abc.item(0, "additional_treatment") is None

        defrow = rows.filter(pl.col("treatment") == "def")
        assert defrow.item(0, "treatment_code") is None
        assert defrow.item(0, "start_date") == dt.date(1900, 1, 3)
        assert defrow.item(0, "end_date") is None
        assert defrow.item(0, "additional_treatment") is None


class TestProcessTreatmentStartDate:
    def test_returns_expected_columns(self, treatment_start_fixture):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_date()

        assert "SubjectId" in df.columns
        assert "treatment_start_date" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("multirow", dt.date(1900, 1, 1), id="picks earliest of multiple rows"),
            ("single_row", dt.date(1900, 1, 2)),
            pytest.param("missing_treatment_none", None, id="TR_TRNAME=None -> filtered"),
            pytest.param("missing_treatment_empty_str", None, id="TR_TRNAME='' -> filtered"),
            pytest.param("empty", None, id="no rows -> filtered"),
        ],
    )
    def test_treatment_start_date_values(self, treatment_start_fixture, sid, expected):
        h = ImpressHarmonizer(data=treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_date()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "treatment_start_date")
        assert actual == expected


class TestProcessEndOfTreatmentDate:
    def test_returns_expected_columns(self, treatment_stop_fixture):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        df = h._process_end_of_treatment_date()

        assert "SubjectId" in df.columns
        assert "end_of_treatment_date" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("eot_precedence", dt.date(1900, 1, 2), id="EOT date wins over treatment-stop date"),
            pytest.param("multirow", dt.date(1900, 1, 1), id="picks max date over multiple rows"),
            pytest.param(
                "invalid_row_doesnt_count",
                dt.date(1900, 1, 1),
                id="invalid row's date ignored even though later",
            ),
            pytest.param("empty", None, id="no rows -> filtered"),
            pytest.param("missing_treatment_empty_str", None, id="empty TR_TROSTPDT -> filtered"),
            pytest.param("missing_treatment_eot_empty_str", None, id="empty EOT_EOTDAT -> filtered"),
        ],
    )
    def test_end_of_treatment_date_values(self, treatment_stop_fixture, sid, expected):
        h = ImpressHarmonizer(data=treatment_stop_fixture, trial_id="T")
        df = h._process_end_of_treatment_date()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "end_of_treatment_date")
        assert actual == expected


class TestProcessTreatmentStartLastCycle:
    def test_returns_expected_columns(self, last_treatment_start_fixture):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_last_cycle()

        assert "SubjectId" in df.columns
        assert "treatment_start_last_cycle" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("two_rows_both_valid", dt.date(1900, 1, 2), id="picks latest cycle start"),
            pytest.param(
                "one_invalid",
                dt.date(1900, 1, 2),
                id="invalid cycle still counts (no enforcement)",
            ),
            pytest.param("empty", None, id="no rows -> filtered"),
        ],
    )
    def test_treatment_start_last_cycle_values(self, last_treatment_start_fixture, sid, expected):
        h = ImpressHarmonizer(data=last_treatment_start_fixture, trial_id="T")
        df = h._process_treatment_start_last_cycle()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "treatment_start_last_cycle")
        assert actual == expected


class TestProcessTreatmentCycle:
    def test_returns_expected_columns(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent

        expected_cols = {"SubjectId"} | set(TreatmentCycleComponent.data_fields())
        assert set(df.columns) == expected_cols

    def test_extracts_iv_cycle(self, treatment_cycle_fixture):
        """
        `iv_two_cycles` has 2 IV rows. The processor computes end_date for
        cycle 1 as the day before cycle 2's start; cycle 2's end_date is None
        because there's no next cycle.
        """
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        rows = df.filter(pl.col("SubjectId") == "iv_two_cycles")
        assert rows.height == 2
        assert set(rows["cycle_type"].to_list()) == {"iv"}

        cycle_1 = rows.filter(pl.col("start_date") == dt.date(1900, 1, 1))
        assert cycle_1.item(0, "end_date") == dt.date(1900, 1, 9)
        assert cycle_1.item(0, "was_total_dose_delivered") is True
        assert cycle_1.item(0, "iv_dose_prescribed") == 100.0
        assert cycle_1.item(0, "iv_dose_prescribed_unit") == "mg"

        cycle_2 = rows.filter(pl.col("start_date") == dt.date(1900, 1, 10))
        assert cycle_2.item(0, "end_date") is None
        assert cycle_2.item(0, "was_total_dose_delivered") is False

    def test_extracts_oral_cycle(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        row = df.filter(pl.col("SubjectId") == "oral_single")
        assert row.item(0, "cycle_type") == "oral"
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 20)
        assert row.item(0, "was_dose_administered_to_spec") is True
        assert row.item(0, "was_tablet_taken_to_prescription_in_previous_cycle") is False
        assert row.item(0, "oral_dose_prescribed_per_day") == 200
        assert row.item(0, "oral_dose_unit") == "mg"
        assert row.item(0, "number_of_days_tablet_not_taken") == 3
        assert row.item(0, "reason_tablet_not_taken") == "nausea"

    def test_both_modalities_subject_has_iv_and_oral_rows(self, treatment_cycle_fixture):
        """
        `both_modalities` has 2 rows under different TR_TRTNO values: one IV
        cycle and one oral cycle. Both should appear in the processor output.
        The IV cycle has no end_date (single IV cycle, no next-start to
        derive from); the oral cycle's end_date comes from TR_TROSTPDT.
        """
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        rows = df.filter(pl.col("SubjectId") == "both_modalities")
        assert rows.height == 2
        assert set(rows["cycle_type"].to_list()) == {"iv", "oral"}

        iv = rows.filter(pl.col("cycle_type") == "iv")
        assert iv.item(0, "end_date") is None

        oral = rows.filter(pl.col("cycle_type") == "oral")
        assert oral.item(0, "end_date") == dt.date(1900, 3, 30)

    def test_both_in_row_oral_takes_precedence(self, treatment_cycle_fixture):
        """
        `both_in_row` has a single row with BOTH IV and oral fields populated.
        The processor picks oral as the cycle_type.
        """
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        row = df.filter(pl.col("SubjectId") == "both_in_row")
        assert row.height == 1
        assert row.item(0, "cycle_type") == "oral"
        assert row.item(0, "end_date") == dt.date(1900, 1, 10)

    def test_null_name_filtered(self, treatment_cycle_fixture):
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_no_name" not in subject_ids

    def test_combination_equal_dose_splits_into_two_rows(self, treatment_cycle_fixture):
        """'Phesgo (Pertuzumab and Trastuzumab)' with '600/600' splits into two rows."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        rows = df.filter(pl.col("SubjectId") == "combo_equal_dose")
        assert rows.height == 2

        names = sorted(rows["ingredient_name"].to_list())
        assert names == ["Pertuzumab", "Trastuzumab"]

        doses = sorted(rows["iv_dose_prescribed"].to_list())
        assert doses == [600.0, 600.0]

        # both rows share the same cycle metadata
        assert rows["cycle_number"].unique().to_list() == [1]
        assert rows["start_date"].unique().to_list() == [dt.date(2023, 1, 1)]
        assert rows["iv_dose_prescribed_unit"].unique().to_list() == ["mg"]

        # brand and raw name preserved
        assert rows["brand_name"].unique().to_list() == ["Phesgo"]
        assert rows["source_treatment_name"].unique().to_list() == ["Phesgo (Pertuzumab and Trastuzumab)"]

        # component_index tracks position
        indices = sorted(rows["component_index"].to_list())
        assert indices == [0, 1]

    def test_combination_different_doses_maps_correctly(self, treatment_cycle_fixture):
        """'1200/600' maps first dose to first drug, second dose to second drug."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        rows = df.filter(pl.col("SubjectId") == "combo_diff_dose").sort("ingredient_name")
        assert rows.height == 2

        pertuzumab = rows.filter(pl.col("ingredient_name") == "Pertuzumab")
        trastuzumab = rows.filter(pl.col("ingredient_name") == "Trastuzumab")

        assert pertuzumab.item(0, "iv_dose_prescribed") == 1200.0
        assert pertuzumab.item(0, "component_index") == 0

        assert trastuzumab.item(0, "iv_dose_prescribed") == 600.0
        assert trastuzumab.item(0, "component_index") == 1

    def test_single_drug_with_brand_extracts_ingredient(self, treatment_cycle_fixture):
        """Non-combination 'Brand (Ingredient)' rows extract brand and ingredient."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        # iv_two_cycles uses "iv Drug" (no parenthetical): should have no brand
        rows = df.filter(pl.col("SubjectId") == "iv_two_cycles")
        assert rows[0, "brand_name"] is None
        assert rows[0, "ingredient_name"] is None
        assert rows[0, "source_treatment_name"] == "IV Drug"
        assert rows[0, "component_index"] is None

    def test_single_branded_drug_extracts_brand_and_ingredient(self, treatment_cycle_fixture):
        """'Tecentriq (Atezolizumab)' extracts brand and ingredient, no component_index."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        row = df.filter(pl.col("SubjectId") == "single_branded")
        assert row.height == 1
        assert row.item(0, "ingredient_name") == "Atezolizumab"
        assert row.item(0, "brand_name") == "Tecentriq"
        assert row.item(0, "source_treatment_name") == "Tecentriq (Atezolizumab)"
        assert row.item(0, "component_index") is None
        assert row.item(0, "iv_dose_prescribed") == 1200.0

    def test_no_parens_treatment_has_no_brand(self, treatment_cycle_fixture):
        """Plain treatment names without parenthetical have no brand, name unchanged."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        row = df.filter(pl.col("SubjectId") == "oral_single")
        assert row.item(0, "brand_name") is None
        assert row.item(0, "ingredient_name") is None
        assert row.item(0, "source_treatment_name") == "Oral Drug"
        assert row.item(0, "component_index") is None

    def test_iv_dose_prescribed_is_float(self, treatment_cycle_fixture):
        """All iv_dose_prescribed values are float after processing (not strings)."""
        h = ImpressHarmonizer(data=treatment_cycle_fixture, trial_id="T")
        df = h._process_treatment_cycle()

        assert df["iv_dose_prescribed"].dtype == pl.Float64


class TestProcessConcomitantMedication:
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
        assert row.item(0, "medication_ongoing") is True
        assert row.item(0, "is_adverse_event_ongoing") is True
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 10)
        assert row.item(0, "sequence_id") == 2

    def test_null_name_filtered(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_null_name" not in subject_ids
        # `name_is_na` has CMTRT="Na", treated as a null
        assert "name_is_na" not in subject_ids

    def test_ordering_subject_emits_all_rows(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        rows = df.filter(pl.col("SubjectId") == "ordering")
        assert rows.height == 3

        # two rows with sequence_id=1 (Drug A), one with sequence_id=2 (Drug B)
        seq1 = rows.filter(pl.col("sequence_id") == 1)
        assert seq1.height == 2
        assert set(seq1["medication_name"].to_list()) == {"Drug A"}
        assert set(seq1["start_date"].to_list()) == {dt.date(1900, 1, 1), dt.date(1900, 2, 1)}

        seq2 = rows.filter(pl.col("sequence_id") == 2)
        assert seq2.height == 1
        assert seq2.item(0, "medication_name") == "Drug B"
        assert seq2.item(0, "start_date") == dt.date(1899, 12, 12)

    def test_ongoing_none_subject_keeps_null_fields(self, concomitant_medication_fixture):
        h = ImpressHarmonizer(data=concomitant_medication_fixture, trial_id="T")
        df = h._process_concomitant_medication()

        row = df.filter(pl.col("SubjectId") == "ongoing_none")
        assert row.height == 1
        assert row.item(0, "medication_ongoing") is None
        assert row.item(0, "is_adverse_event_ongoing") is None
        assert row.item(0, "was_taken_due_to_medical_history_event") is None
        assert row.item(0, "was_taken_due_to_adverse_event") is None
        assert row.item(0, "start_date") is None
        assert row.item(0, "end_date") is None
        assert row.item(0, "sequence_id") == 3


class TestProcessHasAnyAdverseEvents:
    def test_returns_expected_columns(self, adverse_events_flag_fixture):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        df = h._process_has_any_adverse_events()

        assert "SubjectId" in df.columns
        assert "has_any_adverse_events" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("none_all_empty", False, id="negative: all empty"),
            pytest.param("none_whitespace_only", False, id="negative: whitespace only"),
            pytest.param("text_only", True, id="single signal: text"),
            pytest.param("date_only", True, id="single signal: date"),
            pytest.param("grade_only", True, id="single signal: grade"),
            pytest.param("mixed", True, id="multi signal"),
            pytest.param("multirow_any_true", True, id="multirow: any True wins"),
            pytest.param("multirow_all_false", False, id="multirow: all False -> False"),
        ],
    )
    def test_has_any_adverse_events_values(self, adverse_events_flag_fixture, sid, expected):
        h = ImpressHarmonizer(data=adverse_events_flag_fixture, trial_id="T")
        df = h._process_has_any_adverse_events()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "has_any_adverse_events")
        assert actual is expected


class TestProcessAdverseEvents:
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
        assert row.item(0, "turned_serious_date") is None
        assert row.item(0, "related_to_treatment_1_status") == "related"
        assert row.item(0, "related_to_treatment_2_status") == "unknown"
        assert row.item(0, "was_serious_grade_expected_treatment_1") is True
        assert row.item(0, "was_serious_grade_expected_treatment_2") is False
        assert row.item(0, "treatment_1_name") == "Drug A"
        assert row.item(0, "treatment_2_name") == "Drug B"
        assert row.item(0, "start_date") == dt.date(1900, 1, 1)
        assert row.item(0, "end_date") == dt.date(1900, 1, 3)

    def test_serious_event_fills_end_from_death(self, adverse_events_fixture):
        """
        For a serious AE without an explicit end date but with a death date,
        the processor should fill end_date from the death (turned_serious_date).
        """
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        row = df.filter(pl.col("SubjectId") == "serious_fill_end_from_death")
        assert row.height == 1
        assert row.item(0, "was_serious") is True
        assert row.item(0, "turned_serious_date") == dt.date(1900, 1, 12)
        assert row.item(0, "end_date") == dt.date(1900, 2, 1)
        assert row.item(0, "related_to_treatment_1_status") == "not_related"
        assert row.item(0, "related_to_treatment_2_status") is None
        assert row.item(0, "was_serious_grade_expected_treatment_1") is False
        assert row.item(0, "was_serious_grade_expected_treatment_2") is True

    def test_multiple_events_per_patient(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        rows = df.filter(pl.col("SubjectId") == "multi")
        assert rows.height == 2
        assert set(rows["term"].to_list()) == {"Nausea", "Vomiting"}

        nausea = rows.filter(pl.col("term") == "Nausea")
        assert nausea.item(0, "start_date") == dt.date(1900, 3, 1)
        assert nausea.item(0, "end_date") == dt.date(1900, 3, 2)
        assert nausea.item(0, "related_to_treatment_1_status") == "unknown"
        assert nausea.item(0, "related_to_treatment_2_status") == "related"
        assert nausea.item(0, "was_serious") is False

        vomiting = rows.filter(pl.col("term") == "Vomiting")
        assert vomiting.item(0, "start_date") == dt.date(1900, 3, 5)
        assert vomiting.item(0, "end_date") is None
        assert vomiting.item(0, "related_to_treatment_1_status") is None
        assert vomiting.item(0, "related_to_treatment_2_status") == "not_related"
        assert vomiting.item(0, "was_serious") is False

    def test_null_term_filtered(self, adverse_events_fixture):
        h = ImpressHarmonizer(data=adverse_events_fixture, trial_id="T")
        df = h._process_adverse_events()

        subject_ids = set(df["SubjectId"].to_list())
        assert "drop_null_term" not in subject_ids


class TestProcessTumorAssessments:
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
        assert row.item(0, "target_lesion_change_from_nadir") == 0
        assert row.item(0, "was_new_lesions_registered_after_baseline") is True
        assert row.item(0, "date") == dt.date(1900, 1, 10)
        assert row.item(0, "recist_response") == "PR"
        assert row.item(0, "recist_date_of_progression") == dt.date(1900, 2, 1)
        assert row.item(0, "event_id") == "V01"

    def test_extracts_rano_values(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "rano_full")
        assert row.item(0, "assessment_type") == "rano"
        assert row.item(0, "target_lesion_change_from_baseline") == -0.30
        assert row.item(0, "target_lesion_change_from_nadir") == -0.10
        assert row.item(0, "was_new_lesions_registered_after_baseline") is False
        assert row.item(0, "rano_response") == "RANO-PR"
        assert row.item(0, "date") == dt.date(1900, 1, 20)
        assert row.item(0, "event_id") == "V03"

    def test_extracts_irecist_values(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "irecist_full")
        assert row.item(0, "assessment_type") == "irecist"
        assert row.item(0, "target_lesion_change_from_baseline") == 0
        assert row.item(0, "irecist_response") == "iCR"
        assert row.item(0, "irecist_date_of_progression") == dt.date(1900, 2, 10)
        assert row.item(0, "date") == dt.date(1900, 1, 15)
        assert row.item(0, "event_id") == "V02"

    def test_collision_irecist_wins_over_recist(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "collision_irecist_wins")
        assert row.item(0, "assessment_type") == "irecist"

    def test_recist_with_invalid_date_keeps_response(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "recist_bad_date")
        assert row.item(0, "assessment_type") == "recist"
        assert row.item(0, "date") is None
        assert row.item(0, "recist_response") == "SD"
        assert row.item(0, "event_id") == "V06"

    def test_event_id_from_rnrsp_source_only(self, tumor_assessments_fixture):
        """For subjects with signlan only from RNRSP, the event_id and
        date should be picked up from the RNRSP fields."""
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        row = df.filter(pl.col("SubjectId") == "event_from_rnrsp")
        assert row.item(0, "assessment_type") == "rano"
        assert row.item(0, "date") == dt.date(1900, 4, 1)
        assert row.item(0, "event_id") == "V05"
        assert row.item(0, "was_new_lesions_registered_after_baseline") is True

    def test_no_signal_subject_filtered(self, tumor_assessments_fixture):
        h = ImpressHarmonizer(data=tumor_assessments_fixture, trial_id="T")
        df = h._process_tumor_assessments()

        subject_ids = set(df["SubjectId"].to_list())
        assert "no_signal" not in subject_ids


class TestProcessTumorAssessmentsAbsoluteSize:
    """Absolute target_lesion_size = baseline_size × (1 + change_from_baseline).

    Baseline is sourced via the shared `_subject_baseline_target_lesion_size`
    helper (earliest non-null RNRSP_TERNTBAS / RA_RARECBAS per subject); change
    is the fractional percent-change already computed on the row.
    """

    @staticmethod
    def _frame(rows):
        from tests.harmonization.conftest import TumorAssessmentRow
        from dataclasses import asdict, fields

        # all TumorAssessmentRow source columns are str/int-or-None and come from CSV in prod;
        # pin schema so all-None columns in small test frames don't infer Null dtype.
        schema = {}
        for f in fields(TumorAssessmentRow):
            if f.name in ("RA_RANLBASECD", "RNRSP_RNRSPNLCD"):
                schema[f.name] = pl.Int64
            else:
                schema[f.name] = pl.Utf8
        return pl.from_dicts([asdict(TumorAssessmentRow(**r)) for r in rows], schema=schema)

    def test_baseline_100_change_minus_50_pct_yields_50(self):
        df = self._frame(
            [
                # baseline row supplies the size only
                {"SubjectId": "s1", "RA_RARECBAS": "100", "RA_EventDate": "2020-01-01", "RA_EventId": "V00"},
                # assessment row with -50% change from baseline
                {
                    "SubjectId": "s1",
                    "RA_RARECCH": "-50",
                    "RA_RABASECH": "-50",
                    "RA_RAASSESS1": "RECIST",
                    "RA_EventDate": "2020-03-01",
                    "RA_EventId": "V02",
                },
            ],
        )
        h = ImpressHarmonizer(data=df, trial_id="T")
        out = h._process_tumor_assessments()

        row = out.filter((pl.col("SubjectId") == "s1") & (pl.col("event_id") == "V02"))
        assert row.item(0, "target_lesion_size") == 50.0

    def test_baseline_100_change_zero_yields_baseline(self):
        df = self._frame(
            [
                {"SubjectId": "s1", "RA_RARECBAS": "100", "RA_EventDate": "2020-01-01", "RA_EventId": "V00"},
                {
                    "SubjectId": "s1",
                    "RA_RABASECH": "0",
                    "RA_RAASSESS1": "RECIST",
                    "RA_EventDate": "2020-02-01",
                    "RA_EventId": "V01",
                },
            ],
        )
        h = ImpressHarmonizer(data=df, trial_id="T")
        out = h._process_tumor_assessments()

        row = out.filter((pl.col("SubjectId") == "s1") & (pl.col("event_id") == "V01"))
        assert row.item(0, "target_lesion_size") == 100.0

    def test_subject_without_baseline_yields_none(self):
        df = self._frame(
            [
                # no baseline size row for s1, only an assessment with a change value
                {
                    "SubjectId": "s1",
                    "RA_RABASECH": "-30",
                    "RA_RAASSESS1": "RECIST",
                    "RA_EventDate": "2020-02-01",
                    "RA_EventId": "V01",
                },
            ],
        )
        h = ImpressHarmonizer(data=df, trial_id="T")
        out = h._process_tumor_assessments()

        row = out.filter(pl.col("SubjectId") == "s1")
        assert row.item(0, "target_lesion_size") is None

    def test_change_is_none_yields_none(self):
        df = self._frame(
            [
                {"SubjectId": "s1", "RA_RARECBAS": "80", "RA_EventDate": "2020-01-01", "RA_EventId": "V00"},
                # assessment with signal but no change value
                {
                    "SubjectId": "s1",
                    "RA_RAASSESS1": "RECIST",
                    "RA_RATIMRES": "SD",
                    "RA_EventDate": "2020-02-01",
                    "RA_EventId": "V01",
                },
            ],
        )
        h = ImpressHarmonizer(data=df, trial_id="T")
        out = h._process_tumor_assessments()

        row = out.filter((pl.col("SubjectId") == "s1") & (pl.col("event_id") == "V01"))
        assert row.item(0, "target_lesion_size") is None

    def test_subjects_do_not_cross_contaminate(self):
        df = self._frame(
            [
                # s1 baseline 100, assessment -50%: 50.0
                {"SubjectId": "s1", "RA_RARECBAS": "100", "RA_EventDate": "2020-01-01", "RA_EventId": "V00"},
                {
                    "SubjectId": "s1",
                    "RA_RABASECH": "-50",
                    "RA_RAASSESS1": "RECIST",
                    "RA_EventDate": "2020-02-01",
                    "RA_EventId": "V01",
                },
                # s2 baseline 40, assessment +25%: 50.0 (same arithmetic, different subject)
                {"SubjectId": "s2", "RA_RARECBAS": "40", "RA_EventDate": "2020-01-05", "RA_EventId": "V00"},
                {
                    "SubjectId": "s2",
                    "RA_RABASECH": "25",
                    "RA_RAASSESS1": "RECIST",
                    "RA_EventDate": "2020-02-05",
                    "RA_EventId": "V01",
                },
            ],
        )
        h = ImpressHarmonizer(data=df, trial_id="T")
        out = h._process_tumor_assessments()

        s1 = out.filter((pl.col("SubjectId") == "s1") & (pl.col("event_id") == "V01"))
        s2 = out.filter((pl.col("SubjectId") == "s2") & (pl.col("event_id") == "V01"))
        assert s1.item(0, "target_lesion_size") == 50.0
        assert s2.item(0, "target_lesion_size") == 50.0


class TestProcessBestOverallResponse:
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
        assert row.item(0, "date") == dt.date(1900, 1, 20)

    def test_irecist_only_subject(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "irecist_only")
        assert row.item(0, "response") == "CR"
        assert row.item(0, "code") == 4
        assert row.item(0, "date") == dt.date(1900, 1, 5)

    def test_both_recist_and_irecist_picks_irecist(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "both_pick_irecist")
        assert row.item(0, "response") == "iCR"
        assert row.item(0, "code") == 4
        assert row.item(0, "date") == dt.date(1900, 2, 1)

    def test_irecist_unconfirmed_dropped_in_favor_of_recist(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "irecist_unconfirmed_drop")
        assert row.item(0, "response") == "PD"
        assert row.item(0, "code") == 40
        assert row.item(0, "date") == dt.date(1900, 3, 1)

    def test_multi_best_picks_best(self, best_overall_response_fixture):
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "multi_best")
        assert row.item(0, "response") == "PR"
        assert row.item(0, "code") == 20
        assert row.item(0, "date") == dt.date(1900, 2, 1)

    def test_irecist_ne_maps_to_96(self, best_overall_response_fixture):
        """iRECIST 'NE' (not evaluable) should map to code 96 / response 'SD'."""
        h = ImpressHarmonizer(data=best_overall_response_fixture, trial_id="T")
        df = h._process_best_overall_response()

        row = df.filter(pl.col("SubjectId") == "irecist_ne_maps_96")
        assert row.item(0, "response") == "SD"
        assert row.item(0, "code") == 30
        assert row.item(0, "date") == dt.date(1900, 4, 1)


class TestProcessClinicalBenefitAtWeek16:
    def test_returns_expected_columns(self, has_clinical_benefit_at_week_16_fixture):
        h = ImpressHarmonizer(data=has_clinical_benefit_at_week_16_fixture, trial_id="T")
        df = h._process_has_clinical_benefit_at_week_16()

        assert "SubjectId" in df.columns
        assert "has_clinical_benefit_at_week_16" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("recist_le3", True, id="single criterion: RECIST <=3"),
            pytest.param("recist_gt3", False, id="single criterion: RECIST >3"),
            pytest.param("irecist_le3", True, id="single criterion: iRECIST <=3"),
            pytest.param("rano_le3", True, id="single criterion: RANO <=3"),
            pytest.param("both_present", True, id="multi criterion present"),
            pytest.param("v03_no_codes", False, id="V03 visit but no benefit codes"),
            pytest.param("not_v03", None, id="non-V03 visit -> filtered out"),
        ],
    )
    def test_clinical_benefit__at_week_16_values(self, has_clinical_benefit_at_week_16_fixture, sid, expected):
        h = ImpressHarmonizer(data=has_clinical_benefit_at_week_16_fixture, trial_id="T")
        df = h._process_has_clinical_benefit_at_week_16()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "has_clinical_benefit_at_week_16")
        assert actual is expected


class TestProcessEotReason:
    def test_returns_expected_columns(self, end_of_treatment_reason_fixture):
        h = ImpressHarmonizer(data=end_of_treatment_reason_fixture, trial_id="T")
        df = h._process_end_of_treatment_reason()

        assert "SubjectId" in df.columns
        assert "end_of_treatment_reason" in df.columns

    @pytest.mark.parametrize(
        "sid, expected",
        [
            pytest.param("reason_trim", "Progression", id="whitespace trimmed"),
            pytest.param("reason_empty_string", None, id="empty string -> filtered"),
            pytest.param("reason_whitespace_only", None, id="whitespace only -> filtered"),
            pytest.param("reason_none", None, id="None -> filtered"),
        ],
    )
    def test_eot_reason_values(self, end_of_treatment_reason_fixture, sid, expected):
        h = ImpressHarmonizer(data=end_of_treatment_reason_fixture, trial_id="T")
        df = h._process_end_of_treatment_reason()

        row = df.filter(pl.col("SubjectId") == sid)
        actual = None if row.height == 0 else row.item(0, "end_of_treatment_reason")
        assert actual == expected

    def test_multi_row_subject_keeps_all_rows(self, end_of_treatment_reason_fixture):
        h = ImpressHarmonizer(data=end_of_treatment_reason_fixture, trial_id="T")
        df = h._process_end_of_treatment_reason()

        rows = df.filter(pl.col("SubjectId") == "reason_multi_overwrite")
        assert rows.height == 2

        reasons = rows["end_of_treatment_reason"].to_list()
        assert reasons == ["Toxicity", "Patient decision"]


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
        "has_clinical_benefit_at_week_16": "has_clinical_benefit_at_week_16_fixture",
        "end_of_treatment_reason": "end_of_treatment_reason_fixture",
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
        "c30": "c30_fixture",
        "eq5d": "eq5d_fixture",
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

    def test_eot_reason_uses_on_duplicate_last(self):
        """
        eot_reason is the only impress scalar with a non-default `on_duplicate`,
        since the scalar can have colissions across multiple sheets,
        source rows for the same subject are emitted in order where the last one wins.
        """
        spec = next((s for s in ImpressHarmonizer.SPECS if s.name == "end_of_treatment_reason"), None)
        assert spec is not None, "end_of_treatment_reason spec missing from ImpressHarmonizer.SPECS"
        assert isinstance(spec, ScalarSpec)
        assert spec.on_duplicate == "last", (
            f"eot_reason must use on_duplicate='last' (got {spec.on_duplicate!r}). "
            "If this is intentionally changing, update: "
            "TestProcessEotReason::test_multi_row_subject_keeps_all_rows too."
        )
