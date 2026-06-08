from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class Visit(DomainBase):
    """
    A single patient visit (one date per instance).

    Identity (NATURAL_KEY_COLUMNS) = (date,): one visit equals one date.
    Validitiy (REQUIRED_COLUMNS) = (date,): a dateless VI row is not a usable visit.

    Fields:
    - date: date of the visit.
    - event_id: ID of that visit (e.g. "V02").
    """

    class Fields:
        DATE = "date"
        EVENT_ID = "event_id"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._date: dt.date | None = None
        self._event_id: str | None = None
        self.updated_fields: Set[str] = set()

    REQUIRED_FIELDS = (Fields.DATE,)
    NATURAL_KEY_FIELDS = (Fields.DATE,)

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
    def event_id(self) -> str | None:
        return self._event_id

    @event_id.setter
    def event_id(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.event_id,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    def __repr__(self, delim=",") -> str:
        return f"{self.__class__.__name__}(date={self.date!r}{delim} event_id={self.event_id!r})"
