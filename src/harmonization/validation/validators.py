from typing import Optional, Any
import datetime as dt


class StrictValidators:
    """
    Strict validators to validate harmonized data models
    """

    @staticmethod
    def validate_optional_str(value: Optional[str], field_name: str) -> Optional[str]:
        """Strict validation - only accepts string or None (already parsed)"""
        if value is None:
            return None

        if not isinstance(value, str):
            raise TypeError(
                f"{field_name} must be string or None, got {type(value)}: {value}"
            )

        return value

    @staticmethod
    def validate_optional_int(value: Optional[int], field_name: str) -> Optional[int]:
        """Strict validation - only accepts int or None (already parsed)"""
        if value is None:
            return None

        if not isinstance(value, int):
            raise TypeError(
                f"{field_name} must be int or None, got {type(value)}: {value}"
            )

        return value

    @staticmethod
    def validate_optional_float(
        value: Optional[float], field_name: str
    ) -> Optional[float]:
        """Strict validation - only accepts int or None (already parsed)"""
        if value is None:
            return None

        if not isinstance(value, float):
            raise TypeError(
                f"{field_name} must be int or None, got {type(value)}: {value}"
            )

        return value

    @staticmethod
    def validate_optional_date(
        value: Optional[dt.date], field_name: str
    ) -> Optional[dt.date]:
        """Strict validation - only accepts date or None (already parsed)"""
        if value is None:
            return None

        if not isinstance(value, dt.date):
            raise TypeError(
                f"{field_name} must be date or None, got {type(value)}: {value}"
            )

        return value

    @staticmethod
    def validate_optional_positive_int(value: Any, field_name: str) -> Optional[int]:
        if value is None:
            return None

        if isinstance(value, int):
            if value < 0:
                raise ValueError(
                    f"Exepected positive integer for {field_name}, got {value}"
                )
            return value

        raise TypeError(
            f"Expected int or None for field {field_name}, got {type(value)} with value {value}"
        )

    @staticmethod
    def validate_optional_negative_int(value: Any, field_name: str) -> Optional[int]:
        if value is None:
            return None

        if isinstance(value, int):
            if value > 0:
                raise ValueError(
                    f"Exepected positive integer for {field_name}, got {value}"
                )
            return value

        raise TypeError(
            f"Expected int or None for field {field_name}, got {type(value)} with value {value}"
        )

    @staticmethod
    def validate_optional_date_range(
        value: Any, field_name: str, min_date: dt.date, max_date: dt.date
    ) -> Optional[dt.date]:
        if value is None:
            return None

        if isinstance(value, dt.date):
            if not (min_date <= value <= max_date):
                raise ValueError(
                    f"Date {value} is outside min date: {min_date} or max date: {max_date}"
                )
            return value

        raise TypeError(
            f"Excpected datetime.date or None for field {field_name}, got {type(value)} with value {value}"
        )

    @staticmethod
    def validate_optional_bool(value: Any, field_name: str) -> Optional[bool]:
        if value is None:
            return None

        elif isinstance(value, bool):
            return value

        else:
            raise TypeError(
                f"Excpected bool or None for field {field_name}, got {type(value)} with value {value}"
            )

    @staticmethod
    def validate_optional_int_range(
        value: Optional[int], field_name: str, min_val: int, max_val: int
    ) -> Optional[int]:
        """Strict validation with range checking"""
        if value is None:
            return None

        if not isinstance(value, int):
            raise TypeError(
                f"{field_name} must be int or None, got {type(value)}: {value}"
            )

        if not (min_val <= value <= max_val):
            raise ValueError(
                f"{field_name} must be between {min_val} and {max_val}, got {value}"
            )

        return value
