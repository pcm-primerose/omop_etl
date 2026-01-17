from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class BestOverallResponse(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._response: str | None = None
        self._code: int | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    MATERIAL_COLS = ("response",)

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def response(self) -> str | None:
        return self._response

    @response.setter
    def response(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.response,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def code(self) -> int | None:
        return self._code

    @code.setter
    def code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.code,
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

    def __repr__(self, delim=",") -> str:
        return f"{self.__class__.__name__}(response={self.response!r}{delim} code={self.code!r}, date={self.date!r})"
