import datetime as dt
from typing import Optional, List

import polars as pl
import pytest

from omop_etl.harmonization.core.serialize import (
    build_nested_schema,
    build_nested_df,
    to_wide,
    to_normalized,
)
from omop_etl.infra.io.types import SerializeTypes


class BestOverallResponse:
    # leaf class, use annotations for field types
    code: Optional[int]
    date: Optional[dt.date]

    def __init__(self, code: Optional[int], date: Optional[dt.date]):
        self._code = code
        self._date = date

    @property
    def code(self) -> Optional[int]:
        return self._code

    @property
    def date(self) -> Optional[dt.date]:
        return self._date


class AdverseEvent:
    term: str
    grade: Optional[int]

    def __init__(self, term: str, grade: Optional[int]):
        self._term = term
        self._grade = grade

    @property
    def term(self) -> str:
        return self._term

    @property
    def grade(self) -> Optional[int]:
        return self._grade


class Patient:
    """
    Serializer only looks at public @property descriptors on the class.
    Return types must be annotated to build the schema.
    """

    def __init__(
        self,
        patient_id: str,
        trial_id: str,
        age: Optional[int],
        best_overall_response: Optional[BestOverallResponse],
        adverse_events: List[AdverseEvent],
    ):
        self._patient_id = patient_id
        self._trial_id = trial_id
        self._age = age
        self._best_overall_response = best_overall_response
        self._adverse_events = adverse_events

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def trial_id(self) -> str:
        return self._trial_id

    @property
    def age(self) -> Optional[int]:
        return self._age

    @property
    def best_overall_response(self) -> Optional[BestOverallResponse]:
        return self._best_overall_response

    @property
    def adverse_events(self) -> List[AdverseEvent]:
        return self._adverse_events


@pytest.fixture
def patients() -> list[Patient]:
    p1 = Patient(
        patient_id="P1",
        trial_id="T",
        age=60,
        best_overall_response=BestOverallResponse(code=2, date=dt.date(2024, 1, 19)),
        adverse_events=[
            AdverseEvent("Headache", 1),
            AdverseEvent("Nausea", None),
        ],
    )
    p2 = Patient(
        patient_id="P2",
        trial_id="T",
        age=None,
        best_overall_response=None,
        adverse_events=[],
    )
    return [p1, p2]


@pytest.fixture
def nested_df(patients) -> pl.DataFrame:
    return build_nested_df(patients, Patient)


def test_build_nested_schema_shapes_structs_and_lists(patients):
    schema = build_nested_schema(patients, Patient)

    # id columns
    assert schema["patient_id"] == pl.Utf8
    assert schema["trial_id"] == pl.Utf8

    # scalar
    assert schema["age"] == pl.Int64

    # singleton struct
    bor = schema["best_overall_response"]
    assert isinstance(bor, pl.Struct)

    bor_schema = bor.to_schema()
    assert bor_schema["code"] == pl.Int64
    assert bor_schema["date"] == pl.Date

    aes = schema["adverse_events"]
    assert isinstance(aes, pl.List)
    inner = aes.inner
    assert isinstance(inner, pl.Struct)
    inner_schema = inner.to_schema()
    assert inner_schema["term"] == pl.Utf8
    assert inner_schema["grade"] == pl.Int64


def test_build_nested_df_matches_schema(patients):
    df = build_nested_df(patients, Patient)
    # ensure nested types are present
    assert df.schema["best_overall_response"] == pl.Struct({"code": pl.Int64, "date": pl.Date})
    assert isinstance(df.schema["adverse_events"], pl.List)
    assert df.height == 2


def test_to_wide_filters_ids_only_rows(nested_df):
    wide = to_wide(nested_df, prefix_sep=".")
    # p2 excluded (just ID rows)
    base_rows = wide.filter(pl.col("row_type") == "base")
    assert base_rows.height == 1
    assert set(base_rows["patient_id"].to_list()) == {"P1"}

    coll_rows = wide.filter(pl.col("row_type") != "base")
    assert coll_rows.height == 2, "collection rows (adverse_events) present"
    assert all(c in coll_rows.columns for c in ["row_index", "row_type"])
    assert any(c.startswith("adverse_events.") for c in coll_rows.columns), "prefixed columns exist"


def test_to_wide_no_nested_types(nested_df):
    wide = to_wide(nested_df, prefix_sep=".")
    assert all(not isinstance(tp, pl.List) and tp != pl.Struct for tp in wide.schema.values()), "ensure final frame is flat"


def test_to_normalized_tables_and_filters(nested_df):
    tables = to_normalized(nested_df)
    # patients table keeps both rows, IDs + primitives
    assert "patients" in tables
    assert tables["patients"].height == 2
    assert all(c in tables["patients"].columns for c in SerializeTypes.ID_COLUMNS)

    # singleton table is filtered, no ids-only rows
    assert "best_overall_response" in tables
    bor = tables["best_overall_response"]
    assert bor.height == 1
    assert "best_overall_response.code" in bor.columns
    assert "best_overall_response.date" in bor.columns

    # collection table explodes into rows with row_index
    assert "adverse_events" in tables
    ae = tables["adverse_events"]
    assert ae.height == 2
    assert all(c in ae.columns for c in [*SerializeTypes.ID_COLUMNS, "row_index"])
    assert any(c.startswith("adverse_events.") for c in ae.columns)
