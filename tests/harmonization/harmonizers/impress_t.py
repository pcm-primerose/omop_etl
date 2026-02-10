import polars as pl
import pytest

from src.omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer

# todo:
# [ ] use helper class? if so, don't do anything with the results jsut collect and format (no logic)
# [ ] figure out if there is better way to do parametrization.
#       seems to be moving logic away from code and isntead writing assertions in a config,
#       but it's more easily extensible and don't need to change the code so that's nice
#       idk if i like it though, but thats more that it loks ugly and hard to read if you dont know what it is,
#       like its just strings in tuples, not obvious what it represents, but maybe thats just me


class Helpers:
    """Collection of helper methods"""

    @staticmethod
    def rows_where(df: pl.DataFrame, **eq) -> pl.DataFrame:
        """
        Filter df by equality predicates: rows_where(df, SubjectId="x", sex="male")
        Always returns a DataFrame (possibly empty).
        """
        if not eq:
            raise ValueError("Provide at least one predicate")
        expr = pl.lit(True)
        for col, val in eq.items():
            expr &= pl.col == val
        return df.filter(expr)

    @staticmethod
    def require_one(df: pl.DataFrame, **eq) -> pl.DataFrame:
        out = Helpers.rows_where(df, **eq)
        assert out.height == 1, f"Expected 1 row for {eq}, got {out.height}\n{out}"
        return out

    @staticmethod
    def value(df: pl.DataFrame, *, column: str, **eq):
        out = Helpers.rows_where(df, **eq)
        return out.item(0, column)

    @staticmethod
    def assert_absent(df: pl.DataFrame, **eq) -> None:
        out = Helpers.rows_where(df, **eq)
        assert out.height == 0, f"Expected 0 rows for {eq}, got {out.height}\n{out}"

    @staticmethod
    def values_for_subject(df: pl.DataFrame, subject_id: str, column: str) -> list:
        """
        Return all values for a subject+column, preserving duplicates.
        Useful when expecting multiple rows.
        """
        out = Helpers.rows_where(df, SubjectId=subject_id).select(column)
        return out.get_column(column).to_list()

    @staticmethod
    def _harmonizer_for_subject(fixture: pl.DataFrame, subject_id: str) -> ImpressHarmonizer:
        """Build a harmonizer over a single subject's RAW input rows"""
        df = fixture.filter(pl.col("SubjectId") == subject_id)
        harmonizer = ImpressHarmonizer(data=df, trial_id="T")
        harmonizer._create_patients()  # noqa
        return harmonizer


class TestCreatePatients:
    """Note: dedupe and callapsing logic not yet implemented"""

    @pytest.fixture(scope="class")
    def results(self, subject_id_fixture):
        harmonizer = ImpressHarmonizer(data=subject_id_fixture, trial_id="T")
        harmonizer._create_patients()
        return harmonizer

    def test_patient_id_processing(self, results):
        expected_ids = {
            "unique_1",
            "unique_2",
            "unique_3",
            "unique_4",
            "duplicate_id",
            "duplicate_id",
            "duplicate_variant",
        }
        assert len(results.patient_data) == 6
        assert expected_ids == set(results.patient_data)

        for patient_id, patient_data in results.patient_data.items():
            assert patient_data.patient_id == patient_id
            assert patient_data.trial_id == "T"


class TestProcessCohortName:
    @pytest.fixture(scope="class")
    def results(self, cohort_name_fixture):
        harmonizer = ImpressHarmonizer(data=cohort_name_fixture, trial_id="T")
        return harmonizer._process_cohort_name()

    def test_filter_null_values(self, results):
        """Correct cols and no null-results."""
        assert set(results.columns) == {"SubjectId", "cohort_name"}, "Should just return patient id col and cohort_name"
        assert results.filter(pl.col("cohort_name").is_null()).height == 0, "Should not contain rows without cohort_name"
        # assert get_rows(results, colname="cohort_name")
        assert results.height == 2

    def test_filter_missing_subject_id(self, results):
        """Empty cohorts not processed."""
        subject_ids = set(results["SubjectId"].to_list())
        assert "cohort_empty_1" not in subject_ids
        assert "cohort_empty_2" not in subject_ids
        assert "cohort_empty_3" not in subject_ids

    def test_correct_parse(self, results):
        """Valid cohort names parsed."""
        row1 = results.filter(pl.col("SubjectId") == "cohort_hit_1")
        row2 = results.filter(pl.col("SubjectId") == "cohort_hit_2")
        assert row1.item(0, "cohort_name") == "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib"
        assert row2.item(0, "cohort_name") == "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab"


class TestProcessSex:
    @pytest.fixture(scope="class")
    def processed(self, gender_fixture):
        harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        harmonizer._create_patients()
        harmonizer._ensure_patients_initialized()
        return harmonizer._process_sex()

    @pytest.mark.parametrize(
        "subject_id, expected",
        [
            ("female_titlecase", "female"),
            ("male_titlecase", "male"),
            ("female_lowercase", "female"),
            ("male_lowercase", "male"),
            ("female_short_f", "female"),
            ("male_short_m", "male"),
        ],
    )
    def test_valid_values(self, processed, subject_id, expected):
        """Assert processor parser to correct values"""
        out = processed.filter(pl.col("SubjectId") == subject_id)
        assert out.height == 1
        assert out.item(0, "sex") == expected

    @pytest.mark.parametrize("subject_id", ["invalid_value", "empty_value"])
    def test_invalid_values(self, processed, subject_id):
        """Assert processor filters out invalid values"""
        assert processed.filter(pl.col("SubjectId") == subject_id).height == 0

    @pytest.mark.parametrize(
        "subject_id, expected",
        [
            ("female_titlecase", "female"),
            ("male_titlecase", "male"),
            ("female_lowercase", "female"),
            ("male_lowercase", "male"),
            ("female_short_f", "female"),
            ("male_short_m", "male"),
        ],
    )
    def test_build_sets_sex_param(self, gender_fixture, subject_id, expected):
        # nice thing about fixtures is that 4 loc is all that's needed (+ fixture to process):
        harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="T")
        harmonizer.run_one("sex")

        # todo:
        # just use parametrization or not, doesn't matter too much really...

        # how to access Patient nicely? will encounter this problem in all build tests
        # so the Patient object here is set with sex from the gender fixture,
        # since the hydrators set the attrs on Patient, the hydrators don't return anything,
        # so how do I access the patient crated here?
        # I can access the patient dict storign the patient keys in harmonizer,
        # then i can index into the patient instance matching that key.
        # but how do i iterate over patients, or get a patient matching soem condition (Patient.some_attr == "some_value")
        # or access the set nesteed objects or scalar attrs, etc?
        # i'm confused about the scope, so in this test the Patient object is set with just the Sex field (+ patient_id, trial_id).
        # so accessing the patient data in this scope means the only patient data existing is that data?
        # this feels weird, i'm not used to methods not returning objeects but setting them...
        # wait so all the processors update self.patient_data with the processed data then?
        # so the Patient object lives inside the processor class, then ... but how is this updated during the processing?
        # and if the patients exist in a finalizer form in the processor, why not just return that final object?
        # i've confused myself with writing "OOP setter" code when i usually don't do stateful stuff...
        # todo: figure out how people deal with this!
        # todo: also maybe make hydrators return something, so it's explicit, and the last step is then something taking all this data and setting the Patient class?
        #       idk if that's worth the refactor, how big of change that would be, but this feels a bit "magic" and not explicit like i'm used to...
        # todo: also, maybe not dict is the best way to store the data?

        # so currently the only way (?) to access the data is:
        assert harmonizer.patient_data[subject_id].sex == expected
        assert harmonizer.patient_data[subject_id].trial_id == "T"

    # Integration-style: run spec + verify hydration onto Patient
    @pytest.mark.parametrize(
        "subject_id, expected",
        [
            ("female_titlecase", "female"),
            ("male_titlecase", "male"),
            ("female_short_f", "female"),
            ("male_short_m", "male"),
        ],
    )
    def test_build_sets_sex(self, gender_fixture, subject_id, expected):
        # h = self._harmonizer_for_subject(gender_fixture, subject_id)
        # h.run_one("sex")
        # assert h.patient_data[subject_id].sex == expected
        # assert h.patient_data[subject_id].trial_id == "T"
        pass

    @pytest.mark.parametrize("subject_id", ["invalid_value", "empty_value"])
    def test_build_invalid_does_not_set_sex(self, gender_fixture, subject_id):
        # h = self._harmonizer_for_subject(gender_fixture, subject_id)
        #
        # # run_one will early-return because processor output is empty for these subjects
        # h.run_one("sex")
        #
        # patient = h.patient_data[subject_id]  # patient exists because df had SubjectId row(s)
        # assert patient.sex is None
        pass


# class TestProcessSex:
#     @pytest.fixture(scope="class")
#     def results(self, gender_fixture):
#         harmonizer = ImpressHarmonizer(data=gender_fixture, trial_id="T")
#         return harmonizer._process_sex()
#
#     def test_titlecase(self, results):
#         assert results.filter(pl.col("SubjectId") == "female_titlecase").item(0, "sex") == "female"
#         assert results.filter(pl.col("SubjectId") == "male_titlecase").item(0, "sex") == "male"
#
#     def test_lowercase(self, results):
#         assert results.filter(pl.col("SubjectId") == "female_lowercase").item(0, "sex") == "female"
#         assert results.filter(pl.col("SubjectId") == "male_lowercase").item(0, "sex") == "male"
#
#     def test_short_to_long(self, results):
#         assert results.filter(pl.col("SubjectId") == "female_short_f").item(0, "sex") == "female"
#         assert results.filter(pl.col("SubjectId") == "male_short_m").item(0, "sex") == "male"
#
#     def test_invalid_values_to_none(self, results):
#         assert results.filter(pl.col("SubjectId") == "invalid_value").height == 0
#         assert results.filter(pl.col("SubjectId") == "empty_value").height == 0
#
#     @pytest.mark.parametrize("subject_id, expected", [
#         ("female_titlecase", "female"),
#         ("male_titlecase", "male"),
#         ("female_short_f", "female"),
#         ("male_short_m", "male")
#     ])
#     def test_build_sets_sex(self, results, subject_id, expected):
#         df = results.filter(pl.col("SubjectId") == subject_id)
#
#         harmonizer = ImpressHarmonizer(data=df, trial_id="T")
#         harmonizer._create_patients()
#         harmonizer.run_one("sex")
#
#         assert harmonizer.patient_data[subject_id].sex == expected
#
#     @pytest.mark.parametrize("subject_id", ["invalid_value", "empty_value"])
#     def test_build_invalid_does_not_set_sex(self, results, subject_id):
#         df = results.filter(pl.col("SubjectId") == subject_id)
#
#         harmonizer = ImpressHarmonizer(data=df, trial_id="T")
#         harmonizer._create_patients()
#         harmonizer.run_one("sex")
#
#         patient = harmonizer.patient_data[subject_id]
#         assert patient.sex is None


# parametrizing is ugly...
# @pytest.mark.parametrize(
#     "subject_id, expected",
#     [
#         ("female_titlecase", "female"),
#         ("male_titlecase", "male"),
#         ("female_lowercase", "female"),
#         ("male_lowercase", "male"),
#         ("female_short_f", "female"),
#         ("male_short_m", "male"),
#     ],
# )
# def test_sex_mappings(sex_results, subject_id, expected):
#     assert value(sex_results, SubjectId=subject_id, column="sex") == expected
#
# @pytest.mark.parametrize("subject_id", ["invalid_value", "empty_value"])
# def test_sex_invalid_filtered_out(sex_results, subject_id):
#     assert_absent(sex_results, SubjectId=subject_id)
