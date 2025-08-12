from typing import Optional, Any


class TypeCoercion:
    """Helper class to handle basic type conversion with None handling"""

    MISSING_VALUES = {"", "na", "n/a", "null", "none", "unknown", "missing", "nan"}

    @staticmethod
    def to_optional_string(value: Any) -> Optional[str]:
        """Convert any value to string or None"""
        if value is None:
            return None

        str_val = str(value).strip()
        if str_val.lower() in TypeCoercion.MISSING_VALUES:
            return None

        return str_val

    @staticmethod
    def to_optional_int(value: Any) -> Optional[int]:
        """Convert any value to int or None"""
        if value is None:
            return None

        # handle string values
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in TypeCoercion.MISSING_VALUES:
                return None

            try:
                # try to convert decimal strings
                if "." in cleaned:
                    float_val = float(cleaned)
                    if float_val.is_integer():
                        return int(float_val)
                    else:
                        raise ValueError(f"Cannot convert decimal to int {cleaned}")

                return int(cleaned)

            except ValueError:
                raise ValueError(f"Cannot convert string {value} to integer")

        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            else:
                raise ValueError(f"Cannot convert decimal float {value} to integer")

        if isinstance(value, int):
            return value

        raise TypeError(f"Cannot convert {type(value)} to integer")

    @staticmethod
    def to_optional_float(value: Any) -> Optional[float]:
        """Convert any value to float or None"""
        if value is None:
            return None

        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in TypeCoercion.MISSING_VALUES:
                return None

            try:
                return float(cleaned)
            except ValueError:
                raise ValueError(f"Cannot convert string {value} to float")

        if isinstance(value, (int, float)):
            return float(value)

        raise TypeError(f"Cannot convert {type(value)} to float")

    @staticmethod
    def to_optional_bool(value: Any) -> Optional[bool]:
        """Convert any value to bool or None"""
        if value is None:
            return None

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned in TypeCoercion.MISSING_VALUES:
                return None

            if cleaned in {"true", "yes", "y", "1", "on"}:
                return True
            elif cleaned in {"false", "no", "n", "0", "off"}:
                return False
            else:
                raise ValueError(f"Cannot convert string {value} to boolean")

        if isinstance(value, (int, float)):
            return bool(value)

        raise TypeError(f"Cannot convert {type(value)} to boolean")
