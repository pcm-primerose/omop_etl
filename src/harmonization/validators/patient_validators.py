from typing import Optional, Any


class PatientValidator:
    @staticmethod
    def validate_age(value: None | str | float | int) -> Optional[int]:
        # none case
        if value is None:
            return None

        # string case
        if isinstance(value, str):
            value = value.strip()
            if value.lower() in ["na", "", "n/a", "null", "none", "unknown"]:
                return None

            try:
                int(value)
            except ValueError:
                raise ValueError(f"Invalid string for age: {value}")

        # float case
        if isinstance(value, float):
            return int(value)

        # int case
        if isinstance(value, int):
            return value

    @staticmethod
    def validate_sex(value: str | None) -> Optional[str]:
        if value is None:
            return None

        # normalize string
        if value.lower() in ["male", "m"]:
            return "M"
        elif value.lower() in ["female", "f"]:
            return "F"

        else:
            raise ValueError(f"Invalid input value for sex: {value}")
