import polars as pl
import datetime as dt
from src.omop_etl.harmonization.parsing.core import PolarsParsers as pp


def eval_expr(expr, values, col_name="x", dtype=None):
    s = pl.Series(name=col_name, values=values, dtype=dtype) if dtype is not None else pl.Series(name=col_name, values=values)
    df = pl.DataFrame([s])
    return df.select(expr.alias("out"))["out"].to_list()


def test_to_optional_utf8_na_and_strip():
    vals = ["  na ", "n/a", "", None, "unknown", "none", "nk", "ok ", " Foo"]
    out = eval_expr(pp.to_optional_utf8("x"), vals)
    assert out == [None, None, None, None, None, None, None, "ok", "Foo"]


def test_to_optional_bool_strings_only():
    vals = ["yes", "Y", " true ", "False", "n", "maybe", None]
    out = eval_expr(pp.to_optional_bool("x"), vals, dtype=pl.Utf8)
    assert out == [True, True, True, False, False, None, None]


def test_to_optional_bool_bools_only():
    vals = [True, False, True, False, None]
    out = eval_expr(pp.to_optional_bool("x"), vals, dtype=pl.Boolean)
    assert out == [True, False, True, False, None]


def test_to_optional_int64_basics_and_truncation():
    vals = ["1", "2.0", "3.7", None, "foo"]
    out = eval_expr(pp.to_optional_int64("x"), vals)
    assert out == [1, 2, 3, None, None]


def test_to_optional_float64():
    vals = ["1", "2.5", None, "foo"]
    out = eval_expr(pp.to_optional_float64("x"), vals)
    assert out == [1.0, 2.5, None, None]


def test_int_to_bool_defaults_and_custom_labels():
    vals = [1, 0, 2, None, -1]
    out = eval_expr(pp.int_to_bool("x"), vals)
    assert out == [True, False, None, None, None]

    vals2 = [2, -1, 0, None]
    out2 = eval_expr(pp.int_to_bool("x", true_int=2, false_int=-1), vals2)
    assert out2 == [True, False, None, None]


def test_to_optional_date_partial_and_nk():
    vals = ["2020", "2020-05", "2020-05-07", "2020-NK-NK", "2020-NK-03", "2020-07-NK", None, "n/a"]
    out = eval_expr(pp.to_optional_date("x", default_day=15, default_month=7), vals, dtype=pl.Utf8)
    assert out == [
        dt.date(2020, 7, 15),
        dt.date(2020, 5, 15),
        dt.date(2020, 5, 7),
        dt.date(2020, 7, 15),
        dt.date(2020, 7, 3),
        dt.date(2020, 7, 15),
        None,
        None,
    ]


def test__as_expr_variants_equivalence():
    df = pl.DataFrame({"x": [1, 2, 3]})
    a = df.select(pp._as_expr("x").alias("out"))["out"].to_list()
    b = df.select(pp._as_expr(pl.col("x")).alias("out"))["out"].to_list()
    c = df.with_columns(pp._as_expr(5).alias("out"))["out"].to_list()
    assert a == [1, 2, 3]
    assert b == [1, 2, 3]
    assert c == [5, 5, 5]
