import logging
from typing import Optional, Any, List
import datetime as dt


class CoreValidators:
    def __init__(self, logger: Optional[logging.Logger]):
        self.logger = logger or logging.getLogger(__name__)

    MISSING = {"", "na", "n/a", "none", "unknown", "missing", "null"}

    def validate_optional_str(self, value: Any, field_name: str) -> Optional[str]:
        if value is None:
            return None

        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in self.MISSING:
                if self.logger:
                    self.logger.debug(f"Field {field_name} has missing value: {value}")
                return None

            return cleaned

        raise TypeError(
            f"Unsupported type for str coercion for field {field_name} with type {type(value)} and value {value}"
        )

    @staticmethod
    def validate_optional_int(value: Any, field_name: str) -> Optional[int]:
        if value is None:
            return None

        if isinstance(value, int):
            return value

        raise TypeError(
            f"Expected int or None for field {field_name}, got {type(value)} with value {value}"
        )

    @staticmethod
    def validate_optional_float(value: Any, field_name: str) -> Optional[float]:
        if value is None:
            return None

        if isinstance(value, float):
            return value

        raise TypeError(
            f"Excpected float or None for field {field_name}, got {type(value)} with value {value}"
        )

    @staticmethod
    def validate_optional_date(value: Any, field_name: str) -> Optional[dt.date]:
        if value is None:
            return None

        if isinstance(value, dt.date):
            return value

        raise TypeError(
            f"Excpected datetime.date or None for field {field_name}, got {type(value)} with value {value}"
        )

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
