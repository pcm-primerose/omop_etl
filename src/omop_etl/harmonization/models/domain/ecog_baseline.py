from typing import Set
import datetime as dt

from omop_etl.harmonization.core.track_validated import TrackedValidated
from omop_etl.harmonization.core.validators import StrictValidators


class EcogBaseline(TrackedValidated):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._description: str | None = None
        self._grade: int | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def description(self) -> str | None:
        return self._description

    @description.setter
    def description(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.description,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def grade(self) -> int | None:
        return self._grade

    @grade.setter
    def grade(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.grade,
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
        return f"{self.__class__.__name__}(description={self.description!r}{delim} grade={self.grade!r}, date={self.date!r})"
