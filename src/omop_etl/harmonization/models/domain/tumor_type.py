from typing import Set

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TumorType(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._icd10_code: str | None = None
        self._icd10_description: str | None = None
        self._main_tumor_type: str | None = None
        self._main_tumor_type_code: int | None = None
        self._cohort_tumor_type: str | None = None
        self._other_tumor_type: str | None = None
        self.updated_fields: Set[str] = set()

    @property
    def icd10_code(self) -> str | None:
        return self._icd10_code

    @icd10_code.setter
    def icd10_code(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.icd10_code,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )
        self.updated_fields.add(self.__class__.icd10_code.fset.__name__)

    @property
    def icd10_description(self) -> str | None:
        return self._icd10_description

    @icd10_description.setter
    def icd10_description(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.icd10_description,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def main_tumor_type(self) -> str | None:
        return self._main_tumor_type

    @main_tumor_type.setter
    def main_tumor_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.main_tumor_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def main_tumor_type_code(self) -> int | None:
        return self._main_tumor_type_code

    @main_tumor_type_code.setter
    def main_tumor_type_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.main_tumor_type_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def cohort_tumor_type(self) -> str | None:
        return self._cohort_tumor_type

    @cohort_tumor_type.setter
    def cohort_tumor_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cohort_tumor_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def other_tumor_type(self) -> str | None:
        return self._other_tumor_type

    @other_tumor_type.setter
    def other_tumor_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.other_tumor_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"icd10_code={self.icd10_code!r}{delim} "
            f"icd10_description={self.icd10_description!r}{delim} "
            f"main_tumor_type={self.main_tumor_type!r}{delim} "
            f"main_tumor_type_code={self.main_tumor_type_code!r}{delim} "
            f"other_tumor_type={self.other_tumor_type!r}{delim} "
            f"cohort_tumor_type={self.cohort_tumor_type!r}"
            f")"
        )
