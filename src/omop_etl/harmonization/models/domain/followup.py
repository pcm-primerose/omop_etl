from typing import Set
import datetime as dt

from omop_etl.harmonization.core.track_validated import TrackedValidated
from omop_etl.harmonization.core.validators import StrictValidators


class FollowUp(TrackedValidated):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._lost_to_followup: bool | None = None
        self._date_lost_to_followup: dt.datetime | None = None
        self.updated_fields: Set[str] = set()

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
        return (
            f"{self.__class__.__name__}("
            f"lost_to_followup={self.lost_to_followup!r}{delim} "
            f"date_lost_to_followup={self.date_lost_to_followup!r})"
        )
