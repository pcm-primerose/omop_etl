from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class ConcomitantMedication(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._medication_name: str | None = None
        self._medication_ongoing: bool | None = None
        self._was_taken_due_to_medical_history_event: bool | None = None
        self._was_taken_due_to_adverse_event: bool | None = None
        self._is_adverse_event_ongoing: bool | None = None
        self._start_date: dt.date | None = None
        self._end_date: dt.date | None = None
        self._sequence_id: int | None = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def medication_name(self) -> str | None:
        return self._medication_name

    @medication_name.setter
    def medication_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.medication_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def medication_ongoing(self) -> bool | None:
        return self._medication_ongoing

    @medication_ongoing.setter
    def medication_ongoing(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.medication_ongoing,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def was_taken_due_to_medical_history_event(self) -> bool | None:
        return self._was_taken_due_to_medical_history_event

    @was_taken_due_to_medical_history_event.setter
    def was_taken_due_to_medical_history_event(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_taken_due_to_medical_history_event,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def was_taken_due_to_adverse_event(self) -> bool | None:
        return self._was_taken_due_to_adverse_event

    @was_taken_due_to_adverse_event.setter
    def was_taken_due_to_adverse_event(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_taken_due_to_adverse_event,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def is_adverse_event_ongoing(self) -> bool | None:
        return self._is_adverse_event_ongoing

    @is_adverse_event_ongoing.setter
    def is_adverse_event_ongoing(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.is_adverse_event_ongoing,
            value=value,
            validator=StrictValidators.validate_optional_bool,
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
    def sequence_id(self) -> int | None:
        return self._sequence_id

    @sequence_id.setter
    def sequence_id(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.sequence_id,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}{delim} "
            f"medication_name={self.medication_name!r}{delim} "
            f"was_taken_due_to_medical_history_event={self.was_taken_due_to_medical_history_event!r}{delim} "
            f"was_taken_due_to_adverse_event={self.was_taken_due_to_adverse_event!r}{delim} "
            f"is_adverse_event_ongoing={self.is_adverse_event_ongoing!r}{delim} "
            f"start_date={self.start_date!r}{delim} "
            f"end_date={self.end_date!r}{delim} "
            f"sequence_id={self.sequence_id!r}"
            f")"
        )
