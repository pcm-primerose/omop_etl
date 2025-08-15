from types import SimpleNamespace

import pytest
import polars as pl

from omop_etl.preprocessing.core.combine import combine


@pytest.fixture
def df_subjects():
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_1"],
            "age": [34, 51, 28],
            "sex": ["F", "M", "M"],
        }
    )


@pytest.fixture
def df_demographics():
    """
    Should not be merged to one row, age conflicts with age in df_subjects
    """
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_1"],
            "age": [34, 51, 28],
        }
    )


@pytest.fixture
def df_icd10():
    """
    Should merge to one row, no conflicts
    """
    return pl.DataFrame(
        {"SubjectId": ["A_001", "A_002", "A_003"], "icd10": ["C70.0", "C34.0", "C71.0"]}
    )


@pytest.fixture
def df_additional_subjectid():
    """
    Should be merged becasue no conflicts, A_003_2 is currently processed as separate entry
    """
    return pl.DataFrame(
        {
            "SubjectId": ["A_001_1", "A_002_1", "A_003_2"],
            "cancer_type": ["CRC", "NSCLC", "HNSCC"],
        }
    )


def sheet(key: str, df: pl.DataFrame):
    return SimpleNamespace(key=key, data=df)


def ecfg(*sheets):
    return SimpleNamespace(data=list(sheets))


def test_combine_prefix_union_and_sort(
    df_subjects, df_demographics, df_icd10, df_additional_subjectid
):
    e = ecfg(
        sheet("subjects", df_subjects),
        sheet("demographics", df_demographics),
        sheet("icd10", df_icd10),
        sheet("extra", df_additional_subjectid),
    )
    out = combine(e, on="SubjectId")  # type: ignore

    # key kept, everything else prefixed
    assert "SubjectId" in out.columns
    assert {
        "subjects_age",
        "subjects_sex",
        "demographics_age",
        "icd10_icd10",
        "extra_cancer_type",
    }.issubset(set(out.columns))
    assert "age" not in out.columns and "sex" not in out.columns, "not left un-prefixed"

    # values are None for non-matching keys
    r1 = out.filter(pl.col("SubjectId") == "A_001_1").row(0, named=True)
    assert r1["subjects_age"] == 34
    assert r1["icd10_icd10"] is None, "No march due to different key format"

    # A_003_2 as its own row
    r3_1 = out.filter(pl.col("SubjectId") == "A_003_1").row(0, named=True)
    assert r3_1["extra_cancer_type"] is None

    r3_2 = out.filter(pl.col("SubjectId") == "A_003_2").row(0, named=True)
    assert r3_2["extra_cancer_type"] == "HNSCC"

    # sorted by key, first few keys are ascending
    keys = out.select("SubjectId").to_series().to_list()
    assert keys == sorted(keys, key=lambda x: (x is None, x))
