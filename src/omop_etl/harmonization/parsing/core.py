import polars as pl


class PolarsParsers:
    """
    Polars-specific parsing utilities for vectorized operations over columns, rows or expressions.
    """

    NA_VALUES = {"na", "n/a", "null", "", "unknown", "none", "nk"}
    TRUE = {"true", "yes", "y"}
    FALSE = {"false", "no", "n"}
    StrOrExprOrScalar = str | pl.Expr | int | float | bool | None

    @staticmethod
    def _as_expr(x: StrOrExprOrScalar) -> pl.Expr:
        if isinstance(x, pl.Expr):
            return x
        if isinstance(x, str):
            return pl.col(x)
        return pl.lit(x)

    @staticmethod
    def to_optional_utf8(x: StrOrExprOrScalar) -> pl.Expr:
        """Str-like to Utf8 or None"""
        expr = PolarsParsers._as_expr(x)
        expr = expr.cast(pl.Utf8, strict=False).str.strip_chars()
        lc = expr.str.to_lowercase()
        miss = expr.is_null() | lc.is_in(PolarsParsers.NA_VALUES)
        return pl.when(miss).then(None).otherwise(expr)

    @staticmethod
    def to_optional_int64(x: StrOrExprOrScalar) -> pl.Expr:
        expr = PolarsParsers._as_expr(x)
        _int = expr.cast(pl.Float64, strict=False)
        return pl.when(_int.is_null()).then(None).otherwise(_int.cast(pl.Int64, strict=False))

    @staticmethod
    def to_optional_float64(x: StrOrExprOrScalar) -> pl.Expr:
        expr = PolarsParsers._as_expr(x)
        return expr.cast(pl.Float64, strict=False)

    @staticmethod
    def to_optional_bool(x: StrOrExprOrScalar) -> pl.Expr:
        """
        Boolish strings to optional booleans:
        yes|y|true -> True
        no|n|false -> False
        """
        expr = PolarsParsers._as_expr(x)
        s = expr.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
        return (
            pl.when(s.is_in(list(PolarsParsers.TRUE)))
            .then(True)
            .when(s.is_in(list(PolarsParsers.FALSE)))
            .then(False)
            .otherwise(None)
            .cast(pl.Boolean)
        )

    @staticmethod
    def int_to_bool(x: StrOrExprOrScalar, true_int: int = 1, false_int: int = 0) -> pl.Expr:
        """Int to bool or None"""
        expr = PolarsParsers._as_expr(x)
        e = expr.cast(pl.Int64)
        return pl.when(e.is_null()).then(None).when(e == true_int).then(True).when(e == false_int).then(False).otherwise(None).cast(pl.Boolean)

    @staticmethod
    def to_optional_date(x: StrOrExprOrScalar, default_day: int = 15, default_month: int = 7) -> pl.Expr:
        """Vectorized date parser for Polars columns"""
        col = PolarsParsers._as_expr(x)
        col = pl.when(col.is_in(PolarsParsers.NA_VALUES)).then(None).otherwise(col)

        standardized = (
            col.str.replace("'´`˙", "")
            .str.replace_all("(?i)nk", "NK")
            # YYYY-NK-NK to YYYY-MM-DD
            .str.replace("NK-NK$", f"{default_month:02d}-{default_day:02d}")
            # YYYY-MM-NK to YYYY-MM-DD
            .str.replace("-NK$", f"-{default_day:02d}")
            # YYYY-NK-DD to YYYY-MM-DD
            .str.replace("-NK-", f"-{default_month:02d}-")
        )

        # partial dates
        result = (
            # YYYY
            pl.when(standardized.str.len_chars() == 4)
            .then(pl.concat_str([standardized, pl.lit(f"-{default_month:02d}-{default_day:02d}")]))
            # YYYY-MM
            .when(standardized.str.len_chars() == 7)
            .then(pl.concat_str([standardized, pl.lit(f"-{default_day:02d}")]))
            # already YYYY-MM-DD
            .otherwise(standardized)
        )

        return result.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
