import datetime as dt
import pytest
import polars as pl

from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer


# =============================================================================
# Layer A: Processor unit tests (DataFrame-level assertions)
# =============================================================================


class TestProcessCohortNameDF:
    """Layer A: Test _process_cohort_name at DataFrame level."""

    def test_returns_expected_columns(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        assert set(df.columns) == {"SubjectId", "cohort_name"}

    def test_filters_null_values(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        # All rows should have non-null cohort_name (nulls filtered)
        assert df.filter(pl.col("cohort_name").is_null()).height == 0

    def test_parses_values_correctly(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        row1 = df.filter(pl.col("SubjectId") == "cohort_hit_1")
        assert row1.item(0, "cohort_name") == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"

        row2 = df.filter(pl.col("SubjectId") == "cohort_hit_2")
        assert row2.item(0, "cohort_name") == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"

    def test_empty_subjects_filtered_out(self, cohort_name_fixture):
        h = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        df = h._process_cohort_name()

        # Empty/null cohort names should be filtered out
        subject_ids = set(df["SubjectId"].to_list())
        assert "cohort_empty_1" not in subject_ids
        assert "cohort_empty_2" not in subject_ids
        assert "cohort_empty_3" not in subject_ids


class TestProcessSexDF:
    """Layer A: Test _process_sex at DataFrame level."""

    def test_returns_expected_columns(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        assert set(df.columns) == {"SubjectId", "sex"}

    def test_normalizes_to_lowercase(self, gender_fixture):
        h = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        df = h._process_sex()

        # All values should be "male" or "female" (lowercased)
        values = set(df["sex"].to_list())
        assert values <= {"male", "female"}

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
    """Layer A: Test _process_age at DataFrame level."""

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
    """Layer A: Test _process_tumor_type at DataFrame level."""

    def test_returns_expected_columns(self, tumor_type_fixture):
        h = ImpressHarmonizer(data=tumor_type_fixture, trial_id="T")
        df = h._process_tumor_type()

        from omop_etl.harmonization.models.domain.tumor_type import TumorType

        # Should have SubjectId + all TumorType data fields
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


class TestProcessStudyDrugsDF:
    """Layer A: Test _process_study_drugs at DataFrame level."""

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

    def test_collision_filtered_out(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        df = h._process_study_drugs()

        # Collision subject should be filtered out
        subject_ids = set(df["SubjectId"].to_list())
        assert "sd_collision" not in subject_ids


# =============================================================================
# Layer C: Spec-run wiring tests (using run_one, assert on Patient domains)
# =============================================================================


class TestAdverseEventsRunOne:
    """Layer C: Test adverse_events spec via run_one, asserting on Patient domain objects."""

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
    """Layer C: Test medical_histories spec via run_one."""

    def test_hydrates_multiple_histories(self, medical_history_fixture):
        h = ImpressHarmonizer(data=medical_history_fixture, trial_id="T")
        h._create_patients()
        h.run_one("medical_histories")

        mh_list = list(h.patient_data["two_rows"].medical_histories)
        assert len(mh_list) == 2

        # Ordered by start_date ascending: 1900-07-02 < 1900-09-15
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
    """Layer C: Test tumor_assessments spec via run_one."""

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


# =============================================================================
# Additional Layer A tests (scalars)
# =============================================================================


class TestProcessDateOfDeathDF:
    """Layer A: Test _process_date_of_death at DataFrame level."""

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
    """Layer A: Test _process_biomarkers at DataFrame level."""

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


# =============================================================================
# Layer C: Singleton specs via run_one
# =============================================================================


class TestTumorTypeRunOne:
    """Layer C: Test tumor_type singleton spec via run_one."""

    def test_hydrates_tumor_type(self, tumor_type_fixture):
        h = ImpressHarmonizer(data=tumor_type_fixture, trial_id="T")
        h._create_patients()
        h.run_one("tumor_type")

        tt = h.patient_data["tumor1_multi_subtypes"].tumor_type
        assert tt.icd10_code == "C30"
        assert tt.icd10_description == "tumor1"
        assert tt.main_tumor_type == "tumor1_subtype1"
        assert tt.main_tumor_type_code == 50
        assert tt.cohort_tumor_type == "tumor1_subtype2"
        assert tt.other_tumor_type == "tumor1_subtype3"


class TestStudyDrugsRunOne:
    """Layer C: Test study_drugs singleton spec via run_one."""

    def test_hydrates_study_drugs(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        h._create_patients()
        h.run_one("study_drugs")

        sd = h.patient_data["sd_from_alt_slots"].study_drugs
        assert sd.primary_treatment_drug == "Traztuzumab"
        assert sd.primary_treatment_drug_code == 31
        assert sd.secondary_treatment_drug == "Tafinlar"
        assert sd.secondary_treatment_drug_code == 10

    def test_collision_returns_none(self, study_drugs_fixture):
        h = ImpressHarmonizer(data=study_drugs_fixture, trial_id="T")
        h._create_patients()
        h.run_one("study_drugs")

        # Collision is filtered at processor level, so no domain object hydrated
        assert h.patient_data["sd_collision"].study_drugs is None


class TestLostToFollowupRunOne:
    """Layer C: Test lost_to_followup singleton spec via run_one."""

    def test_hydrates_ltfu(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        h._create_patients()
        h.run_one("lost_to_followup")

        ins = h.patient_data["ltfu_valid"].lost_to_followup
        assert ins.lost_to_followup is True
        assert ins.date_lost_to_followup == dt.date(1900, 1, 1)

    def test_alive_not_ltfu(self, lost_to_followup_fixture):
        h = ImpressHarmonizer(data=lost_to_followup_fixture, trial_id="T")
        h._create_patients()
        h.run_one("lost_to_followup")

        ins = h.patient_data["alive_valid"].lost_to_followup
        assert ins.lost_to_followup is False
        assert ins.date_lost_to_followup is None


class TestEcogBaselineRunOne:
    """Layer C: Test ecog_baseline singleton spec via run_one."""

    def test_hydrates_ecog(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        ins = h.patient_data["all_data"].ecog_baseline
        assert ins.description == "all"
        assert ins.grade == 1
        assert ins.date == dt.date(1900, 1, 1)

    def test_wrong_event_id_returns_none(self, ecog_fixture):
        h = ImpressHarmonizer(data=ecog_fixture, trial_id="T")
        h._create_patients()
        h.run_one("ecog_baseline")

        assert h.patient_data["wrong_event_id"].ecog_baseline is None


# =============================================================================
# Scalar specs via run_one (Layer C pattern for completeness)
# =============================================================================


class TestScalarSpecsRunOne:
    """Layer C: Test scalar specs via run_one."""

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
    """Layer C: Test evaluable_for_efficacy_analysis scalar spec."""

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
