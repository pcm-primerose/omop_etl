import polars as pl
import datetime as dt
from typing import Optional, List, Union, Any
from src.harmonization.parsing.coercion import TypeCoercion

# todo:
#   fix remaining static methods (see specific todos)
#   fix calls from implementation to use this class instead


class CoreParsers:
    """Domain-agnostic parsers that handle common data patterns"""

    @staticmethod
    def parse_date_flexible(value: Any) -> Optional[dt.date]:
        """Parse dates from various common formats"""
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        if isinstance(value, dt.date):
            return value
        if isinstance(value, dt.datetime):
            return value.date()

        # todo include NK formats
        #   add as static set
        formats = [
            "%Y-%m-%d",  # 2023-12-25
            "%m/%d/%Y",  # 12/25/2023
            "%d/%m/%Y",  # 25/12/2023
            "%Y%m%d",  # 20231225
            "%d-%b-%Y",  # 25-Dec-2023
            "%B %d, %Y",  # December 25, 2023
        ]

        for fmt in formats:
            try:
                return dt.datetime.strptime(str_value, fmt).date()
            except ValueError:
                continue

        raise ValueError(f"Cannot parse date from: '{value}'")

    @staticmethod
    def parse_numeric_range(
        value: Any, min_val: float, max_val: float
    ) -> Optional[float]:
        float_value = TypeCoercion.to_optional_float(value)
        if float_value is None:
            return None

        if not (min_val <= float_value <= max_val):
            raise ValueError(
                f"Value {float_value} outside valid range [{min_val}, {max_val}]"
            )

        return float_value

    @staticmethod
    def parse_categorical(
        value: Any, valid_values: set, case_sensitive: bool = False
    ) -> Optional[str]:
        """Parse categorical value against allowed list"""
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        # normalize
        check_value = str_value if case_sensitive else str_value.lower()
        valid_set = (
            valid_values if case_sensitive else {v.lower() for v in valid_values}
        )

        if check_value not in valid_set:
            raise ValueError(f"Value {value} not in valid options: {valid_values}")

        # return original case from valid_values if found
        if not case_sensitive:
            for original_value in valid_values:
                if original_value.lower() == check_value:
                    return original_value

        return str_value

    @staticmethod
    def parse_text_normalized(
        value: Any, max_length: int = None, title_case: bool = False
    ) -> Optional[str]:
        """Parse text with normalization options"""
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        if title_case:
            str_value = str_value.title()

        if max_length and len(str_value) > max_length:
            raise ValueError(f"Text exceeds maximum length {max_length}: '{str_value}'")

        return str_value

    # todo: fix this
    #   add date handling for YYYY-NK-NK & YYYY-mm-NK as well (used in IMPRESS)
    @staticmethod
    def parse_optional_date(
        date_str: str, default_day: Optional[int] = 15, default_month: Optional[int] = 7
    ) -> Optional[dt.datetime]:
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
                            return dt.datetime(year, default_month, default_day)
                        return None
                    except (ValueError, TypeError):
                        return None
        else:
            return None

    # todo: fix this and rename
    @staticmethod
    def parse_safe_get(value, default=None):
        return value if value != "NA" else default

    # todo: fix this and rename
    @staticmethod
    def safe_int(value):
        if value == "NA":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # todo: fix this and rename
    #   add date handling for YYYY-NK-NK & YYYY-mm-NK as well (used in IMPRESS)
    @staticmethod
    def parse_optional_date_column(
        column: Union[str, pl.Expr],
        default_day: int = 15,
        default_month: int = 7,
        na_values: Optional[List[str]] = None,
    ) -> Optional[pl.expr]:
        """
        Vectorized date parser that handles partial dates in Polars.
        """
        if na_values is None:
            na_values = ["NA"]

        if isinstance(column, str):
            column = pl.col(column)

        for na_value in na_values:
            column = pl.when(column == na_value).then(None).otherwise(column)

        return (
            # year only: YYYY
            pl.when(column.str.len_chars() == 4)
            .then(
                pl.concat_str(
                    column, pl.lit(f"-{default_month:02d}-{default_day:02d}")
                ).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
            )
            # year and month: YYYY-MM
            .when(column.str.len_chars() == 7)
            .then(
                pl.concat_str(column, pl.lit(f"-{default_day:02d}")).str.strptime(
                    pl.Datetime, "%Y-%m-%d", strict=False
                )
            )
            # full date: YYYY-MM-DD
            .when(column.str.len_chars() == 10)
            .then(column.str.strptime(pl.Datetime, "%Y-%m-%d", strict=False))
            # everything else becomes None
            .otherwise(None)
        )
