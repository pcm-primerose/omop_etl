from typing import Set
import datetime as dt

from omop_etl.harmonization.core.make_validated_property import make_validated_property
from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class C30(DomainBase):
    Q_COUNT = 30

    def __init__(self, patient_id: str):
        self.updated_fields: Set[str] = set()
        self._patient_id = patient_id
        self._date: dt.date | None = None
        self._event_name: str | None = None
        # question fields default to None

    @property
    def patient_id(self) -> str:
        return self._patient_id

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

    @property
    def event_name(self) -> str | None:
        return self._event_name

    @event_name.setter
    def event_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.event_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    def __repr__(self) -> str:
        base = [
            f"patient_id={self.patient_id!r}",
            f"date={self.date!r}",
            f"event_name={self.event_name!r}",
        ]
        qs = [f"q{i}={getattr(self, f'q{i}')!r}" for i in range(1, self.Q_COUNT + 1) if getattr(self, f"q{i}", None) is not None]
        qcs = [f"q{i}_code={getattr(self, f'q{i}_code')!r}" for i in range(1, self.Q_COUNT + 1) if getattr(self, f"q{i}_code", None) is not None]
        return f"{self.__class__.__name__}({', '.join(base + qs + qcs)})"


# generate q1-q30 and q1_code-q30_code properties at class definition time
for _i in range(1, C30.Q_COUNT + 1):
    setattr(C30, f"q{_i}", make_validated_property(f"q{_i}", StrictValidators.validate_optional_str))
    setattr(C30, f"q{_i}_code", make_validated_property(f"q{_i}_code", StrictValidators.validate_optional_int))
