from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class StudyDrugs(DomainBase):
    """
    A patient's study treatment: the primary (and optional secondary) study
    drug(s) and allocation date (one per patient).

    Identity (NATURAL_KEY_FIELDS) = (date,): the singleton's linkage anchor.
    Validity (REQUIRED_FIELDS) = (primary_study_drug,): every patient has a primary study drug.

    Fields:
    - primary_treatment_drug: Primary study drug.
    - primary_treatment_drug_code: Source-specific code for primary treatment drug.
    - secondary_treatment_drug: Secondary study drug.
    - secondary_treatment_drug_code: Source-specific code for secondary treatment drug.
    - date: date of study drug record from source.
    """

    class Fields:
        PRIMARY_TREATMENT_DRUG = "primary_treatment_drug"
        PRIMARY_TREATMENT_DRUG_CODE = "primary_treatment_drug_code"
        SECONDARY_TREATMENT_DRUG = "secondary_treatment_drug"
        SECONDARY_TREATMENT_DRUG_CODE = "secondary_treatment_drug_code"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._primary_treatment_drug: str | None = None
        self._primary_treatment_drug_code: int | None = None
        self._secondary_treatment_drug: str | None = None
        self._secondary_treatment_drug_code: int | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    NATURAL_KEY_FIELDS = (Fields.DATE,)
    REQUIRED_FIELDS = (Fields.PRIMARY_TREATMENT_DRUG,)

    @property
    def primary_treatment_drug(self) -> str | None:
        return self._primary_treatment_drug

    @primary_treatment_drug.setter
    def primary_treatment_drug(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.primary_treatment_drug,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def secondary_treatment_drug(self) -> str | None:
        return self._secondary_treatment_drug

    @secondary_treatment_drug.setter
    def secondary_treatment_drug(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.secondary_treatment_drug,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def primary_treatment_drug_code(self) -> int | None:
        return self._primary_treatment_drug_code

    @primary_treatment_drug_code.setter
    def primary_treatment_drug_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.primary_treatment_drug_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def secondary_treatment_drug_code(self) -> int | None:
        return self._secondary_treatment_drug_code

    @secondary_treatment_drug_code.setter
    def secondary_treatment_drug_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.secondary_treatment_drug_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
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

    def __repr__(self, delim=","):
        return (
            f"{self.__class__.__name__}("
            f"primary_treatment_drug={self.primary_treatment_drug!r}{delim} "
            f"primary_treatment_drug_code={self.primary_treatment_drug_code!r}{delim} "
            f"secondary_treatment_drug={self.secondary_treatment_drug!r}{delim} "
            f"secondary_treatment_drug_code={self.secondary_treatment_drug_code!r}{delim}"
            f"date={self.date!r}"
            f")"
        )
