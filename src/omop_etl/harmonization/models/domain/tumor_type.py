from typing import Set
import datetime as dt

from omop_etl.harmonization.core.track_validated import setter_name
from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TumorType(DomainBase):
    """
    A patient's tumor type (singleton: one per patient).

    Identity (NATURAL_KEY_FIELDS) = (main_tumor_type,): the singleton's linkage
    anchor for the primary-cancer condition.
    Validity (REQUIRED_FIELDS) = (main_tumor_type,): every processor must
    populate a main cancer type, a record without one isn't usable.

    Fields:
    - main_tumor_type: Main tumor type for the patient.
    - main_tumor_type_code: Source-specific integer-code for main_tumor_type.
    - icd10_code: ICD 10 code of the tumor type (e.g. C70.2).
    - icd10_description: Free-text description of the ICD10 code.
    - cohort_tumor_type: The tumor type defined from the cohort the patient is placed in.
    - other_tumor_type: Additional tumor type for the patient.
    - date: Date of tumor type registration.
    """

    class Fields:
        MAIN_TUMOR_TYPE = "main_tumor_type"
        ICD10_CODE = "icd10_code"
        ICD10_DESCRIPTION = "icd10_description"
        MAIN_TUMOR_TYPE_CODE = "main_tumor_type_code"
        COHORT_TUMOR_TYPE = "cohort_tumor_type"
        OTHER_TUMOR_TYPE = "other_tumor_type"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._main_tumor_type: str | None = None
        self._icd10_code: str | None = None
        self._icd10_description: str | None = None
        self._main_tumor_type_code: int | None = None
        self._cohort_tumor_type: str | None = None
        self._other_tumor_type: str | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    REQUIRED_FIELDS = (Fields.MAIN_TUMOR_TYPE,)
    NATURAL_KEY_FIELDS = (Fields.MAIN_TUMOR_TYPE,)

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
    def icd10_code(self) -> str | None:
        return self._icd10_code

    @icd10_code.setter
    def icd10_code(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.icd10_code,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )
        self.updated_fields.add(setter_name(self.__class__.icd10_code))

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
    def date(self) -> dt.date | None:
        return self._date

    @date.setter
    def date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"main_tumor_type={self.main_tumor_type!r}{delim} "
            f"icd10_code={self.icd10_code!r}{delim} "
            f"icd10_description={self.icd10_description!r}{delim} "
            f"main_tumor_type_code={self.main_tumor_type_code!r}{delim} "
            f"other_tumor_type={self.other_tumor_type!r}{delim} "
            f"cohort_tumor_type={self.cohort_tumor_type!r}{delim} "
            f"date={self.date!r}"
            f")"
        )
