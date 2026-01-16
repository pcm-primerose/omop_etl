from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class PreviousTreatments(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._treatment: str | None = None
        self._treatment_code: int | None = None
        self._treatment_sequence_number: int | None = None
        self._start_date: dt.date | None = None
        self._end_date: dt.date | None = None
        self._additional_treatment: str | None = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def treatment(self) -> str | None:
        return self._treatment

    @treatment.setter
    def treatment(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def treatment_code(self) -> int | None:
        return self._treatment_code

    @treatment_code.setter
    def treatment_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def treatment_sequence_number(self) -> int | None:
        return self._treatment_sequence_number

    @treatment_sequence_number.setter
    def treatment_sequence_number(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_sequence_number,
            value=value,
            validator=StrictValidators.validate_optional_int,
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

    @property
    def end_date(self) -> dt.date | None:
        return self._end_date

    @end_date.setter
    def end_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.end_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def additional_treatment(self) -> str | None:
        return self._additional_treatment

    @additional_treatment.setter
    def additional_treatment(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.additional_treatment,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"treatment={self.treatment!r}{delim}"
            f" treatment_code={self.treatment_code!r}{delim}"
            f" treatment_sequence_number={self.treatment_sequence_number!r}{delim}"
            f" start_date={self.start_date!r}{delim}"
            f" end_date={self.end_date!r}{delim}"
            f" additional_treatment={self.additional_treatment!r})"
        )
