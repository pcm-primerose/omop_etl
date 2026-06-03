from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class FollowUp(DomainBase):
    """
    If a patient was lost to follow-up, and when (one per patient).

    lost_to_followup is True when the follow-up status is anything other than
    alive/death (actually lost). date_lost_to_followup is the loss date (null
    when not lost).

    Identity (NATURAL_KEY_FIELDS) = (lost_to_followup,): singleton anchors on lost_to_followup status.
    Validity (REQUIRED_FIELDS) = (lost_to_followup,): record must carry a status.

    Fields:
    - lost_to_followup: True if patient is lost to followup, false otherwise.
    - date_lost_to_followup: Date when patient was lost to followup.
    """

    class Fields:
        LOST_TO_FOLLOWUP = "lost_to_followup"
        DATE_LOST_TO_FOLLOWUP = "date_lost_to_followup"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._lost_to_followup: bool | None = None
        self._date_lost_to_followup: dt.datetime | None = None
        self.updated_fields: Set[str] = set()

    NATURAL_KEY_FIELDS = (Fields.LOST_TO_FOLLOWUP,)
    REQUIRED_FIELDS = (Fields.LOST_TO_FOLLOWUP,)

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def lost_to_followup(self) -> bool | None:
        return self._lost_to_followup

    @lost_to_followup.setter
    def lost_to_followup(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.lost_to_followup,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def date_lost_to_followup(self) -> dt.date | None:
        return self._date_lost_to_followup

    @date_lost_to_followup.setter
    def date_lost_to_followup(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.date_lost_to_followup,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=","):
        return f"{self.__class__.__name__}(lost_to_followup={self.lost_to_followup!r}{delim} date_lost_to_followup={self.date_lost_to_followup!r})"
