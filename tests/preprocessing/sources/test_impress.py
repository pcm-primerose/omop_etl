import polars as pl

from omop_etl.preprocessing.sources.impress import (
    _filter_valid_cohort,
    _keep_ecog_v00_or_na,
    _add_trial,
    _prefix_subject,
    _aggregate_no_conflicts,
    preprocess_impress,
)
from omop_etl.preprocessing.core.models import EcrfConfig, RunOptions


class TestCohortFiltering:
    """Test cohort filtering logic"""

    def test_keeps_subjects_with_valid_cohort(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A001", "A002", "A002"],
                "COH_COHORTNAME": ["Valid Cohort", None, "Another Cohort", ""],
                "data": [1, 2, 3, 4],
            }
        )

        result = _filter_valid_cohort(df)

        # A001 and A002 should be kept
        assert result.height == 4
        subject_ids = result["SubjectId"].to_list()
        assert "A001" in subject_ids
        assert "A002" in subject_ids

    def test_removes_subjects_with_no_valid_cohort(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A001", "A002", "A002", "A003"],
                "COH_COHORTNAME": ["Valid Cohort", None, None, "", "NA"],
                "data": [1, 2, 3, 4, 5],
            }
        )

        result = _filter_valid_cohort(df)

        # only A001 should remain
        subject_ids = result["SubjectId"].unique().to_list()
        assert subject_ids == ["A001"]
        assert result.height == 2

    def test_handles_whitespace_cohort_names(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A002", "A003"],
                "COH_COHORTNAME": ["  ", "\t\n", "Valid Name"],
            }
        )

        result = _filter_valid_cohort(df)

        # only A003 should remain
        assert result.height == 1
        assert result["SubjectId"].item() == "A003"


class TestEcogFiltering:
    """Test ECOG filtering logic"""

    def test_keeps_v00_ecog_events(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A002"],
                "ECOG_EventId": ["V00", "V00"],
                "data": [1, 2],
            }
        )

        result = _keep_ecog_v00_or_na(df)
        assert result.height == 2

    def test_filters_out_other_ecog_events(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A002", "A003", "A004"],
                "ECOG_EventId": ["V00", None, "V01", "V02"],
                "data": [1, 2, 3, 4],
            }
        )

        result = _keep_ecog_v00_or_na(df)

        # only V00 and None should remain
        assert result.height == 2
        kept_events = result["ECOG_EventId"].to_list()
        assert "V01" not in kept_events
        assert "V02" not in kept_events


class TestAddTrial:
    """Test trial column addition and subject id prefixing"""

    def test_adds_trial_column(self):
        df = pl.DataFrame({"SubjectId": ["A001", "A002"], "data": [1, 2]})

        result = _add_trial(df, "impress")

        assert "Trial" in result.columns
        assert result["Trial"].unique().to_list() == ["IMPRESS"]

    def test_prefixes_subject_id(self):
        df = pl.DataFrame({"SubjectId": ["001", "002"], "data": [1, 2]})

        result = _prefix_subject(df, "TEST")
        expected_ids = ["TEST-001", "TEST-002"]
        assert result["SubjectId"].to_list() == expected_ids

    def test_converts_trial_to_uppercase(self):
        df = pl.DataFrame({"SubjectId": ["001"]})
        result = _prefix_subject(df, "lowercase")
        assert result["SubjectId"].item() == "LOWERCASE-001"

    def test_preserves_other_columns(self):
        df = pl.DataFrame({"SubjectId": ["001"], "other_col": ["unchanged"]})

        result = _prefix_subject(df, "TEST")
        assert result["other_col"].item() == "unchanged"


class TestConflictMerging:
    """Test conflict merging"""

    def test_converts_trial_to_uppercase(self):
        df = pl.DataFrame({"SubjectId": ["001"]})
        result = _prefix_subject(df, "lowercase")
        assert result["SubjectId"].item() == "LOWERCASE-001"

    def test_preserves_other_columns(self):
        df = pl.DataFrame({"SubjectId": ["001"], "other_col": ["unchanged"]})

        result = _prefix_subject(df, "TEST")
        assert result["other_col"].item() == "unchanged"

    def test_keeps_raw_data_for_conflicts(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A001", "A002", "A002"],
                "age": [25, 30, 35, None],  # age conflict in A001
                "sex": ["M", "M", "F", "F"],
            }
        )

        result = _aggregate_no_conflicts(df)
        a001_rows = result.filter(pl.col("SubjectId") == "A001")
        a002_rows = result.filter(pl.col("SubjectId") == "A002")

        assert a001_rows.height == 2, "A001 should have 2 rows from conflict"
        assert a002_rows.height == 1

    def test_ignores_null_values_in_conflict_detection(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["A001", "A001"],
                "age": [25, None],
                "sex": ["M", None],  # no conflict, null ignored
            }
        )

        result = _aggregate_no_conflicts(df)

        # should aggregate to 1 row
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["age"] == 25
        assert row["sex"] == "M"

    def test_empty_dataframe(self):
        df = pl.DataFrame(
            {"SubjectId": [], "age": []}, schema={"SubjectId": pl.Utf8, "age": pl.Int64}
        )

        result = _aggregate_no_conflicts(df)

        assert result.height == 0
        assert result.columns == ["SubjectId", "age"]

    def test_single_subject_single_row(self):
        df = pl.DataFrame({"SubjectId": ["A001"], "age": [25], "sex": ["M"]})

        result = _aggregate_no_conflicts(df)

        assert result.height == 1
        assert result["SubjectId"].item() == "A001"


class TestPreprocessImpressIntegration:
    """Test the main preprocess_impress function"""

    def test_full_pipeline_with_filtering(self):
        df = pl.DataFrame(
            {
                "SubjectId": ["001", "001", "002"],
                "COH_COHORTNAME": ["Valid Cohort", None, "Invalid"],
                "ECOG_EventId": [None, "V00", "V01"],
                "age": [25, None, 30],
            }
        )

        ecfg = EcrfConfig(trial="IMPRESS", configs=[])
        run_opts = RunOptions(filter_valid_cohort=True)

        result = preprocess_impress(df, ecfg, run_opts)

        # should filter out 002 (no valid cohort) and 001's V01 row
        assert "Trial" in result.columns
        assert "IMPRESS-001" in result["SubjectId"].to_list()
        assert "IMPRESS-002" not in result["SubjectId"].to_list()

        # check column order
        assert result.columns[0] == "SubjectId"
        assert result.columns[1] == "Trial"

    def test_defaults_to_impress_when_no_trial_name(self):
        df = pl.DataFrame(
            {"SubjectId": ["001"], "COH_COHORTNAME": ["Valid"], "ECOG_EventId": ["V00"]}
        )

        ecfg = EcrfConfig(trial=None, configs=[])
        run_opts = RunOptions(filter_valid_cohort=False)

        result = preprocess_impress(df, ecfg, run_opts)

        assert result["Trial"].item() == "IMPRESS"
        assert result["SubjectId"].item() == "IMPRESS-001"
