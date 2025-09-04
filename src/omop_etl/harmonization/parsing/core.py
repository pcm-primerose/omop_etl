import logging

import polars as pl
import datetime as dt
from typing import Optional, Union, Any, Set
from omop_etl.harmonization.parsing.coercion import TypeCoercion


class CoreParsers:
    """Domain-agnostic parsers that handle common data patterns"""

    STANDARD_DATE_FORMATS = (
        "%Y-%m-%d",  # 2023-12-25
        "%m/%d/%Y",  # 12/25/2023
        "%d/%m/%Y",  # 25/12/2023
        "%Y%m%d",  # 20231225
        "%d-%b-%Y",  # 25-Dec-2023
        "%B %d, %Y",  # December 25, 2023
        "%Y-%m",  # 2023-12 (partial)
        "%Y",  # 2023 (year only)
    )

    @staticmethod
    def parse_date_flexible(
        value: Any, default_day: int = 15, default_month: int = 7
    ) -> Optional[dt.date]:
        """Parse dates from various formats including partial dates"""
        # coerce
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        # early ret on date obj
        if isinstance(value, dt.date):
            return value
        if isinstance(value, dt.datetime):
            return value.date()

        # process NK formats (YYYY-NK-NK, YYYY-MM-NK)
        if "-nk" in str_value.lower():
            return CoreParsers._parse_nk_date(str_value, default_day, default_month)

        # try to parse partial dates
        for fmt in CoreParsers.STANDARD_DATE_FORMATS:
            try:
                parsed_date = dt.datetime.strptime(str_value, fmt)
                if fmt == "%Y":
                    return parsed_date.replace(
                        month=default_month, day=default_day
                    ).date()
                elif fmt == "%Y-%m":
                    return parsed_date.replace(day=default_day).date()
                else:
                    return parsed_date.date()

            except ValueError:
                continue

        if not isinstance(value, dt.date):
            logging.warning(f"Cannot parse date from: {str(value)}, returning as None")
            return None

    @staticmethod
    def _parse_nk_date(
        date_str: str, default_day: int, default_month: int
    ) -> Optional[dt.date]:
        """Parse clinical dates with NK (Not Known) components"""
        parts = date_str.upper().split("-")
        if len(parts) != 3:
            raise ValueError(f"Invalid NK date format: {date_str}")

        year_str, month_str, day_str = parts

        try:
            year = int(year_str)
        except ValueError:
            raise ValueError(f"Invalid year in NK date: {year_str}")

        # NK month
        if month_str == "NK":
            month = default_month
        else:
            try:
                month = int(month_str)
            except ValueError:
                raise ValueError(f"Invalid month in NK date: {month_str}")

        # NK day
        if day_str == "NK":
            day = default_day
        else:
            try:
                day = int(day_str)
            except ValueError:
                raise ValueError(f"Invalid day in NK date: {day_str}")

        try:
            return dt.date(year, month, day)

        except ValueError as e:
            raise ValueError(f"Invalid date components: {e}")

    @staticmethod
    def parse_categorical(
        value: Any, valid_values: Set[str], case_sensitive: bool = False
    ) -> Optional[str]:
        """Parse categorical value against allowed list"""
        # coerce
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        # normalize
        check_value = str_value if case_sensitive else str_value.lower()
        valid_set = (
            valid_values if case_sensitive else {v.lower() for v in valid_values}
        )

        if check_value not in valid_set:
            raise ValueError(f"Value '{value}' not in valid options: {valid_values}")

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
        # coerce
        str_value = TypeCoercion.to_optional_string(value)
        if str_value is None:
            return None

        # normalize
        if title_case:
            str_value = str_value.title()

        # check length constraint
        if max_length and len(str_value) > max_length:
            raise ValueError(f"Text exceeds maximum length {max_length}: '{str_value}'")

        return str_value

    @staticmethod
    def parse_int_range(value: Any, min_val: int, max_val: int) -> Optional[int]:
        """Parse integer value and validate range"""
        # coerce
        int_value = TypeCoercion.to_optional_int(value)
        if int_value is None:
            return None

        # validate range
        if not (min_val <= int_value <= max_val):
            raise ValueError(
                f"Value {int_value} outside valid range [{min_val}, {max_val}]"
            )

        return int_value

    @staticmethod
    def parse_numeric_range(
        value: Any, min_val: float, max_val: float
    ) -> Optional[float]:
        """Parse numeric value to float and validate range"""
        # coerce
        float_value = TypeCoercion.to_optional_float(value)
        if float_value is None:
            return None

        # validate range
        if not (min_val <= float_value <= max_val):
            raise ValueError(
                f"Value {float_value} outside valid range [{min_val}, {max_val}]"
            )

        return float_value


class PolarsParsers:
    """
    Polars-specific parsing utilities for vectorized operations over columns, rows or expressions.
    """

    NA_VALUES = {"na", "n/a", "null", "", "unknown", "none", "nk"}

    @staticmethod
    def parse_date_column(
        column: Union[str, pl.Expr], default_day: int = 15, default_month: int = 7
    ) -> pl.Expr:
        """Vectorized date parser for Polars columns"""
        c = pl.col(column) if isinstance(column, str) else column

        # NA values to None
        c = pl.when(c.is_in(PolarsParsers.NA_VALUES)).then(None).otherwise(c)

        standardized = (
            c
            # replace NK patterns (case-insensitive) with default values
            .str.replace_all("(?i)nk", "NK")  # Normalize to uppercase first
            .str.replace(
                "NK-NK$", f"{default_month:02d}-{default_day:02d}"
            )  # YYYY-NK-NK to YYYY-MM-DD
            .str.replace("-NK$", f"-{default_day:02d}")  # YYYY-MM-NK to YYYY-MM-DD
            .str.replace("-NK-", f"-{default_month:02d}-")  # YYYY-NK-DD to YYYY-MM-DD
        )

        # process partial dates
        result = (
            pl.when(standardized.str.len_chars() == 4)  # YYYY
            .then(
                pl.concat_str(
                    [standardized, pl.lit(f"-{default_month:02d}-{default_day:02d}")]
                )
            )
            .when(standardized.str.len_chars() == 7)  # YYYY-MM
            .then(pl.concat_str([standardized, pl.lit(f"-{default_day:02d}")]))
            .otherwise(standardized)  # already YYYY-MM-DD format or other
        )

        # try to parse as date with error handling
        return result.str.strptime(pl.Date, "%Y-%m-%d", strict=False)

    @staticmethod
    def safe_numeric_conversion(
        column: Union[str, pl.Expr], target_type: str = "int"
    ) -> pl.Expr:
        """Safely convert column to numeric type with NA handling"""
        if isinstance(column, str):
            column = pl.col(column)

        for na_value in PolarsParsers.NA_VALUES:
            column = pl.when(column == na_value).then(None).otherwise(column)

        if target_type == "int":
            return column.cast(pl.Int64, strict=True)
        elif target_type == "float":
            return column.cast(pl.Float64, strict=True)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")

    @staticmethod
    def null_if_na(expr: str | pl.Expr) -> pl.Expr:
        e = pl.col(expr) if isinstance(expr, str) else expr
        raw = e.cast(pl.Utf8, strict=False).str.strip_chars()
        return (
            pl.when(raw.str.to_lowercase().is_in(list(PolarsParsers.NA_VALUES)))
            .then(None)
            .otherwise(raw)
        )

    @staticmethod
    def yes_no_to_bool(expr: str | pl.Expr) -> pl.Expr:
        e = pl.col(expr) if isinstance(expr, str) else expr
        s = e.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
        return (
            pl.when(s.is_in(["yes", "y", "true", "1"]))
            .then(True)
            .when(s.is_in(["no", "n", "false", "0"]))
            .then(False)
            .otherwise(None)
            .cast(pl.Boolean)
        )

    @staticmethod
    def int_to_bool(
        expr: pl.Expr, true_int: Optional[int] = None, false_int: Optional[int] = None
    ) -> pl.Expr:
        e = pl.col(expr) if isinstance(expr, str) else expr
        return (
            pl.when(e == true_int)
            .then(True)
            .when(e == false_int)
            .then(False)
            .otherwise(None)
            .cast(pl.Boolean)
        )
