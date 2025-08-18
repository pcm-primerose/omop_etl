from types import SimpleNamespace
import pytest
import polars as pl

from omop_etl.preprocessing.core.combine import combine
from omop_etl.preprocessing.sources.impress import _aggregate_no_conflicts


@pytest.fixture
def df_subjects():
    return pl.DataFrame(
        {
            "SubjectId": [
                "A_001_1",
                "A_002_1",
                "A_003_1",
                "B_999_1",
                "A_004_1",
                "A_003_2",
            ],
            "sex": ["F", "M", "M", "F", "M", "F"],
        }
    )


@pytest.fixture
def df_demographics():
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_1", "B_999_1"],
            "age": [34, 51, 28, 99],
        }
    )


@pytest.fixture
def df_icd10():
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_1"],
            "icd10": ["C70.0", "C34.0", "C71.0"],
        }
    )


@pytest.fixture
def df_extra_subjectid():
    # A_003_2 is currently processed as separate entry
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_2", "extra_id"],
            "cancer_type": ["CRC", "NSCLC", "HNSCC", "something"],
        }
    )


def sheet(key: str, df: pl.DataFrame):
    return SimpleNamespace(key=key, data=df)


def ecfg(*sheets):
    return SimpleNamespace(data=list(sheets))


@pytest.fixture
def demographics_merged_subjects(df_subjects, df_demographics) -> pl.DataFrame:
    e = ecfg(
        sheet("subjects", df_subjects),
        sheet("demographics", df_demographics),
    )
    combined = combine(e, on="SubjectId")

    aggregated = _aggregate_no_conflicts(combined)
    return aggregated


@pytest.fixture
def all_merged(
    df_subjects, df_demographics, df_icd10, df_extra_subjectid
) -> pl.DataFrame:
    e = ecfg(
        sheet("subjects", df_subjects),
        sheet("demographics", df_demographics),
        sheet("icd10", df_icd10),
        sheet("extra", df_extra_subjectid),
    )
    combined = combine(e, on="SubjectId")

    # apply aggregations
    aggregated = _aggregate_no_conflicts(combined)
    return aggregated


def test_key_column_preserved_without_prefix(demographics_merged_subjects):
    assert "SubjectId" in demographics_merged_subjects.columns
    assert "subjects_SubjectId" not in demographics_merged_subjects.columns
    assert "demographics_SubjectId" not in demographics_merged_subjects.columns


def test_non_key_columns_get_prefixed(demographics_merged_subjects):
    assert "demographics_age" in demographics_merged_subjects.columns
    assert "subjects_sex" in demographics_merged_subjects.columns
    assert "demographics_age" in demographics_merged_subjects.columns


def test_original_unprefixed_columns_removed(demographics_merged_subjects):
    assert "age" not in demographics_merged_subjects.columns
    assert "sex" not in demographics_merged_subjects.columns


def test_all_sheets_contribute_columns(all_merged):
    expected_columns = {
        "SubjectId",
        "demographics_age",
        "subjects_sex",
        "icd10_icd10",
        "extra_cancer_type",
    }
    assert expected_columns.issubset(set(all_merged.columns))


def test_non_matching_keys_result_in_null_values(all_merged):
    row = all_merged.filter(pl.col("SubjectId") == "B_999_1").row(0, named=True)
    assert row["demographics_age"] == 99, "Should be merged with existing matched data"
    assert row["icd10_icd10"] is None, "Should be None when no data for that subject"


def test_matching_keys_get_combined_data(all_merged):
    row = all_merged.filter(pl.col("SubjectId") == "A_001_1").row(0, named=True)
    assert row["subjects_sex"] == "F"
    assert row["demographics_age"] == 34
    assert row["icd10_icd10"] == "C70.0"
    assert row["extra_cancer_type"] == "CRC"


def test_unique_keys_create_separate_rows(all_merged):
    only_in_subjects_df = all_merged.filter(pl.col("SubjectId") == "A_004_1").row(
        0, named=True
    )
    assert only_in_subjects_df["subjects_sex"] == "M"
    assert only_in_subjects_df["extra_cancer_type"] is None

    only_in_extra_df = all_merged.filter(pl.col("SubjectId") == "A_003_2").row(
        0, named=True
    )
    assert only_in_extra_df["demographics_age"] is None
    assert only_in_extra_df["extra_cancer_type"] == "HNSCC"


def test_results_sorted_by_key_column(all_merged):
    keys = all_merged.select("SubjectId").to_series().to_list()
    expected_sorted = sorted(keys, key=lambda x: (x is None, x))
    assert keys == expected_sorted


def test_sorting_handles_null_keys():
    df_with_nulls = pl.DataFrame(
        {"SubjectId": ["B_001", None, "A_001"], "value": [1, 2, 3]}
    )

    e = ecfg(sheet("test", df_with_nulls))
    out = combine(e, on="SubjectId")

    _keys = out.select("SubjectId").to_series().to_list()
    assert _keys == [
        "A_001",
        "B_001",
        None,
    ], "should be: A_001, B_001, None (nulls last)"


def test_total_row_count_matches_unique_keys(all_merged):
    assert len(all_merged) == 7


def test_no_duplicate_rows_for_same_key(df_subjects):
    e = ecfg(sheet("subjects", df_subjects))
    out = combine(e, on="SubjectId")
    subject_ids = out.select("SubjectId").to_series()

    assert len(out) == len(df_subjects)
    assert subject_ids.n_unique() == len(subject_ids)


def test_missing_key_column_raises_error():
    df_no_key = pl.DataFrame({"other_col": [1, 2, 3]})
    e = ecfg(sheet("no_key", df_no_key))

    with pytest.raises(ValueError, match="'SubjectId' not in sheet no_key"):
        combine(e, on="SubjectId")


def test_empty_ecrf_config_raises_error():
    e = ecfg()  # no sheets
    with pytest.raises(ValueError, match="No eCRF config data loaded"):
        combine(e, on="SubjectId")


def test_custom_key_column(self, df_subjects):
    df_renamed = df_subjects.rename({"SubjectId": "PatientId"})
    e = ecfg(sheet("patients", df_renamed))
    out = combine(e, on="PatientId")

    assert "PatientId" in out.columns
    assert "patients_sex" in out.columns
    assert len(out) == 6
