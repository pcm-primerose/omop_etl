from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TumorType(DomainBase):
    class Cols:
        ICD10_CODE = "icd10_code"
        ICD10_DESCRIPTION = "icd10_description"
        MAIN_TUMOR_TYPE = "main_tumor_type"
        MAIN_TUMOR_TYPE_CODE = "main_tumor_type_code"
        COHORT_TUMOR_TYPE = "cohort_tumor_type"
        OTHER_TUMOR_TYPE = "other_tumor_type"
        START_DATE = "start_date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._icd10_code: str | None = None
        self._icd10_description: str | None = None
        self._main_tumor_type: str | None = None
        self._main_tumor_type_code: int | None = None
        self._cohort_tumor_type: str | None = None
        self._other_tumor_type: str | None = None
        self._start_date: dt.date | None = None
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

    @property
    def start_date(self) -> dt.date | None:
        return self._start_date

    @start_date.setter
    def start_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.start_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"icd10_code={self.icd10_code!r}{delim} "
            f"icd10_description={self.icd10_description!r}{delim} "
            f"main_tumor_type={self.main_tumor_type!r}{delim} "
            f"main_tumor_type_code={self.main_tumor_type_code!r}{delim} "
            f"other_tumor_type={self.other_tumor_type!r}{delim} "
            f"cohort_tumor_type={self.cohort_tumor_type!r}{delim}"
            f"start_date={self.start_date!r}"
            f")"
        )
