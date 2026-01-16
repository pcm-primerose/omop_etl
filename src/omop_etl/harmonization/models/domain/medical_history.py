from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class MedicalHistory(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._term: str | None = None
        self._sequence_id: int | None = None
        self._start_date: dt.date | None = None
        self._end_date: dt.date | None = None
        self._status: str | None = None
        self._status_code: int | None = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def term(self) -> str | None:
        return self._term

    @term.setter
    def term(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.term,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def sequence_id(self) -> int | None:
        return self._sequence_id

    @sequence_id.setter
    def sequence_id(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.sequence_id,
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
    def status(self) -> str | None:
        return self._status

    @status.setter
    def status(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.status,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def status_code(self) -> int | None:
        return self._status_code

    @status_code.setter
    def status_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.status_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"term={self.term!r}{delim} "
            f"seq={self.sequence_id!r}{delim} "
            f"start={self.start_date!r}{delim} "
            f"end={self.end_date!r}{delim} "
            f"status={self.status!r}{delim} "
            f"code={self.status_code!r})"
        )
