import polars as pl
import datetime as dt
from typing import Optional, List, Union

# TODO make these more general (i.e., not hardcoded names, parametrized, more errer handling,


def date_parser_helper(col_name: pl.col) -> pl.col:
    """Safely parses missing dates using polars expressions API, given col name containing dates"""
    col_expr = pl.col(col_name) if isinstance(col_name, str) else col_name

    return (
        pl.when(col_expr.str.len_chars() == 4)
        .then(col_expr + "-01-01")
        .when(col_expr.str.len_chars() == 7)
        .then(col_expr + "-01")
        .otherwise(col_expr)
    )


def na_to_none(value):
    return None if value == "NA" else value


def safe_convert_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


def safe_get(value, default=None):
    return value if value != "NA" else default


def safe_int(value):
    if value == "NA":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_date(date_str):
    if date_str == "NA" or not date_str:
        return None
    try:
        # parse to datetime obj
        return dt.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        # use logger later
        return None


def parse_flexible_date(
    date_str: str, default_day: Optional[int] = 15, default_month: Optional[int] = 7
) -> None | dt.datetime:
    """Takes any date string and allows partial parsing to datetime objects"""
    if date_str == "NA" or not date_str:
        return None

    elif isinstance(date_str, str):
        try:
            return dt.datetime.strptime(date_str, "%Y-%m-%d")

        except ValueError:
            try:
                date = dt.datetime.strptime(date_str, "%Y-%m")
                # default to middle of month
                return date.replace(day=default_day)

            except ValueError:
                try:
                    year = int(date_str)
                    if 1900 <= year <= 2100:
                        # default to middle of year
                        return dt.datetime(year, default_month, 1)
                    return None
                except (ValueError, TypeError):
                    return None
    else:
        print(f"Must pass str, got {type(date_str)}")


def parse_date_column(
    column: Union[str, pl.Expr],
    default_day: int = 15,
    default_month: int = 7,
    na_values=None,
) -> pl.Expr:
    """
    Vectorized date parser that handles partial dates in Polars.
    """
    if na_values is None:
        na_values = ["NA", ""]
    if isinstance(column, str):
        column = pl.col(column)

    for na_value in na_values:
        column = pl.when(column == na_value).then(None).otherwise(column)

    return (
        pl.when(column.str.len_chars() == 4)
        .then(
            pl.concat_str(
                column, pl.lit(f"-{default_month:02d}-{default_day:02d}")
            ).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
        )
        .when(column.str.len_chars() == 7)
        .then(
            pl.concat_str(column, pl.lit(f"-{default_day:02d}")).str.strptime(
                pl.Datetime, "%Y-%m-%d", strict=False
            )
        )
        .when(column.str.len_chars() == 10)
        .then(column.str.strptime(pl.Datetime, "%Y-%m-%d", strict=False))
        # every other format becomes None
        .otherwise(None)
    )
