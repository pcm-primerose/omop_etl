import datetime as dt


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
        if type(value) is not int:  # disallow bool
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
