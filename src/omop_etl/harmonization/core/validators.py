import datetime as dt
from enum import Enum
from typing import TypeVar

E = TypeVar("E", bound=Enum)


class StrictValidators:
    @staticmethod
    def validate_optional_str(value, field_name):
        if value is None:
            return None
        if type(value) is not str:
            raise TypeError(f"{field_name} must be string or None, got {type(value)}: {value}")
        return value

    @staticmethod
    def validate_optional_int(value, field_name):
        if value is None:
            return None
        if type(value) is not int:
            raise TypeError(f"{field_name} must be int or None, got {type(value)}: {value}")
        return value

    @staticmethod
    def validate_optional_float(value, field_name):
        if value is None:
            return None
        if type(value) is not float:
            raise TypeError(f"{field_name} must be float or None, got {type(value)}: {value}")
        return value

    @staticmethod
    def validate_optional_date(value, field_name):
        if value is None:
            return None
        if type(value) is not dt.date:
            raise TypeError(f"{field_name} must be date or None, got {type(value)}: {value}")
        return value

    @staticmethod
    def validate_optional_bool(value, field_name):
        if value is None:
            return None
        if type(value) is bool:
            return value
        raise TypeError(f"Expected bool or None for field {field_name}, got {type(value)} with value {value}")

    @staticmethod
    def validate_optional_str_tuple(value, field_name):
        """
        Normalize a sequence of strings to a tuple. None: ().
        Rejects a bare str/bytes.
        """
        if value is None:
            return ()
        if isinstance(value, (str, bytes)):
            raise TypeError(f"{field_name} must be a sequence of str (not a bare string), got {type(value)}: {value}")
        try:
            items = list(value)
        except TypeError:
            raise TypeError(f"{field_name} must be a sequence of str or None, got {type(value)}: {value}")
        for x in items:
            if type(x) is not str:
                raise TypeError(f"{field_name} elements must be str, got {type(x)}: {x}")
        return tuple(items)

    @staticmethod
    def validate_optional_enum(*, value, field_name: str, enum_type: type[E]) -> E | None:
        if value is None:
            return None
        if isinstance(value, enum_type):
            return value
        if isinstance(value, str):
            try:
                return enum_type(value)
            except ValueError:
                valid = [e.value for e in enum_type]
                raise ValueError(f"{field_name}: '{value}' is not a valid {enum_type.__name__}. Valid values: {valid}")
        raise TypeError(f"{field_name} must be {enum_type.__name__} or None, got {type(value)}: {value}")
