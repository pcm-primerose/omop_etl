import datetime as dt
from typing import Optional, List
import polars as pl
import pytest

from omop_etl.infra.io.types import SerializeTypes
from omop_etl.harmonization.core.serialize import (
    build_nested_schema,
    build_nested_df,
    to_wide,
    to_normalized,
)


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


class SerializeTestPatient:
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
def patients() -> list[SerializeTestPatient]:
    p1 = SerializeTestPatient(
        patient_id="P1",
        trial_id="T",
        age=60,
        best_overall_response=BestOverallResponse(code=2, date=dt.date(2024, 1, 19)),
        adverse_events=[
            AdverseEvent("Headache", 1),
            AdverseEvent("Nausea", None),
        ],
    )
    p2 = SerializeTestPatient(
        patient_id="P2",
        trial_id="T",
        age=None,
        best_overall_response=None,
        adverse_events=[],
    )
    return [p1, p2]


@pytest.fixture
def nested_df(patients) -> pl.DataFrame:
    return build_nested_df(patients, SerializeTestPatient)


def test_build_nested_schema_shapes_structs_and_lists(patients):
    schema = build_nested_schema(patients, SerializeTestPatient)

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
    df = build_nested_df(patients, SerializeTestPatient)
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


class TestDynamicPropertySerialization:
    """Tests for classes with dynamically generated properties (C30, EQ5D)."""

    def test_c30_dynamic_properties_included_in_schema(self):
        """C30's dynamically generated q1-q30 and q1_code-q30_code properties are in schema."""
        from omop_etl.harmonization.models.domain.c30 import C30
        from omop_etl.harmonization.models.patient import Patient

        p = Patient(patient_id="P001", trial_id="TEST")
        c = C30("P001")
        c.event_name = "Baseline"
        c.q1 = "test_value"
        c.q1_code = 1
        c.q15 = "another_value"
        c.q15_code = 15
        p.c30_collection = [c]

        schema = build_nested_schema([p], Patient)

        c30_dtype = schema.get("c30_collection")
        assert isinstance(c30_dtype, pl.List)
        inner = c30_dtype.inner
        assert isinstance(inner, pl.Struct)
        inner_schema = inner.to_schema()

        # static properties
        assert "date" in inner_schema
        assert "event_name" in inner_schema

        # dynamic properties should be included
        assert "q1" in inner_schema
        assert "q1_code" in inner_schema
        assert "q15" in inner_schema
        assert "q15_code" in inner_schema
        # all 30 questions and codes should be present
        for i in range(1, 31):
            assert f"q{i}" in inner_schema, f"q{i} missing from schema"
            assert f"q{i}_code" in inner_schema, f"q{i}_code missing from schema"

    def test_eq5d_dynamic_properties_included_in_schema(self):
        """EQ5D's dynamically generated q1-q5 and q1_code-q5_code properties are in schema."""
        from omop_etl.harmonization.models.domain.eq5d import EQ5D
        from omop_etl.harmonization.models.patient import Patient

        p = Patient(patient_id="P001", trial_id="TEST")
        e = EQ5D("P001")
        e.event_name = "Baseline"
        e.q1 = "test"
        e.q1_code = 1
        e.qol_metric = 75
        p.eq5d_collection = [e]

        schema = build_nested_schema([p], Patient)

        eq5d_dtype = schema.get("eq5d_collection")
        assert isinstance(eq5d_dtype, pl.List)
        inner = eq5d_dtype.inner
        assert isinstance(inner, pl.Struct)
        inner_schema = inner.to_schema()

        # static properties
        assert "date" in inner_schema
        assert "event_name" in inner_schema
        assert "qol_metric" in inner_schema

        # dynamic properties should be included
        for i in range(1, 6):
            assert f"q{i}" in inner_schema, f"q{i} missing from schema"
            assert f"q{i}_code" in inner_schema, f"q{i}_code missing from schema"

    def test_c30_wide_output_includes_dynamic_columns(self):
        """to_wide includes C30's dynamic properties as columns."""
        from omop_etl.harmonization.models.domain.c30 import C30
        from omop_etl.harmonization.models.patient import Patient

        p = Patient(patient_id="P001", trial_id="TEST")
        c = C30("P001")
        c.event_name = "Baseline"
        c.q1 = "value1"
        c.q1_code = 1
        p.c30_collection = [c]

        df = build_nested_df([p], Patient)
        wide = to_wide(df)

        c30_cols = [col for col in wide.columns if "c30_collection" in col]
        assert "c30_collection.event_name" in c30_cols
        assert "c30_collection.q1" in c30_cols
        assert "c30_collection.q1_code" in c30_cols

    def test_c30_normalized_output_includes_dynamic_columns(self):
        """to_normalized creates c30_collection table with dynamic properties."""
        from omop_etl.harmonization.models.domain.c30 import C30
        from omop_etl.harmonization.models.patient import Patient

        p = Patient(patient_id="P001", trial_id="TEST")
        c = C30("P001")
        c.event_name = "Baseline"
        c.q1 = "value1"
        c.q1_code = 1
        c.q2 = "value2"
        c.q2_code = 2
        p.c30_collection = [c]

        df = build_nested_df([p], Patient)
        tables = to_normalized(df)

        assert "c30_collection" in tables
        c30_table = tables["c30_collection"]

        # check columns exist
        assert "c30_collection.event_name" in c30_table.columns
        assert "c30_collection.q1" in c30_table.columns
        assert "c30_collection.q1_code" in c30_table.columns

        # check data is correct
        row = c30_table.row(0, named=True)
        assert row["c30_collection.event_name"] == "Baseline"
        assert row["c30_collection.q1"] == "value1"
        assert row["c30_collection.q1_code"] == 1
        assert row["c30_collection.q2"] == "value2"
        assert row["c30_collection.q2_code"] == 2

    def test_c30_preserves_static_property_types(self):
        """Static properties with type hints preserve their types during enrichment."""
        from omop_etl.harmonization.models.domain.c30 import C30
        from omop_etl.harmonization.models.patient import Patient

        p = Patient(patient_id="P001", trial_id="TEST")
        c = C30("P001")
        c.date = dt.date(2024, 1, 15)
        c.event_name = "Baseline"
        p.c30_collection = [c]

        schema = build_nested_schema([p], Patient)
        inner_schema = schema["c30_collection"].inner.to_schema()  # type: ignore

        # static properties should retain their annotated types
        assert inner_schema["date"] == pl.Date
        assert inner_schema["event_name"] == pl.Utf8
