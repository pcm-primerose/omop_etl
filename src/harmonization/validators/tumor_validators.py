from typing import Optional


class TumorTypeValidator:
    @staticmethod
    def validate_icd10_code(value: None | str) -> Optional[str]:
        # format is e.g. C30.0 or B38.79 or K20.102 etc
        # none case
        if value is None:
            return None

        # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                str(value)
            except ValueError:
                raise ValueError(f"ICD10 code not valid str: {value}")

            return value

    @staticmethod
    def validate_icd10_description(value: str | None) -> Optional[str]:
        # format is just free text description
        if value is None:
            return None

            # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                str(value)
            except ValueError:
                raise ValueError(f"ICD10 description not valid str: {value}")

            return value

    @staticmethod
    def validate_tumor_type(value: str | None) -> Optional[str]:
        # format is just str
        if value is None:
            return None

        # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                str(value)
            except ValueError:
                raise ValueError(f"Tumor type not valid str: {value}")

            return value

    @staticmethod
    def validate_tumor_type_code(value: str | int | float | None) -> Optional[int]:
        # format should be integer
        if value is None:
            return None

        # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                int(value)
            except ValueError:
                raise ValueError(
                    f"Tumor type code not valid int after coerding from str: {value}"
                )

        # float case
        if isinstance(value, float):
            try:
                int(value)
            except ValueError:
                raise ValueError(
                    f"Tumor type code not valid int after coercing from float: {value}"
                )

        return value

    @staticmethod
    def validate_cohort_tumor_type(value: str | None) -> Optional[str]:
        # format is just str
        if value is None:
            return None

        # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                str(value)
            except ValueError:
                raise ValueError(f"Cohort tumor type not valid str: {value}")

            return value

    @staticmethod
    def validate_other_tumor_type(value: str | None) -> Optional[str]:
        # format is just str
        if value is None:
            return None

        # str case
        if isinstance(value, str):
            if value.lower() in ["", "na", "n/a"]:
                return None

            try:
                str(value)
            except ValueError:
                raise ValueError(f"Other tumor type not valid str: {value}")

            return value
