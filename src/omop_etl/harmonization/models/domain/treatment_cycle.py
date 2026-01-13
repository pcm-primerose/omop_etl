from typing import Set
import datetime as dt

from omop_etl.harmonization.core.track_validated import TrackedValidated
from omop_etl.harmonization.core.validators import StrictValidators


class TreatmentCycle(TrackedValidated):
    def __init__(self, patient_id: str):
        # core
        self._patient_id = patient_id
        self._treatment_name: str | None = None
        self._cycle_type: str | None = None
        self._treatment_number: int | None = None
        self._cycle_number: int | None = None
        self._start_date: dt.date | None = None
        self._end_date: dt.date | None = None
        self._recieved_treatment_this_cycle: bool | None = None

        # iv only
        self._was_total_dose_delivered: bool | None = None
        self._iv_dose_prescribed: str | None = None
        self._iv_dose_prescribed_unit: str | None = None

        # oral only
        self._was_dose_administered_to_spec: bool | None = None
        self._reason_not_administered_to_spec: str | None = None
        self._oral_dose_prescribed_per_day: float | None = None
        self._oral_dose_unit: str | None = None
        self._other_dose_unit: str | None = None
        self._number_of_days_tablet_not_taken: int | None = None
        self._reason_tablet_not_taken: str | None = None
        self._was_tablet_taken_to_prescription_in_previous_cycle: bool | None = None

        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def treatment_name(self) -> str:
        return self._treatment_name

    @treatment_name.setter
    def treatment_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def cycle_type(self) -> str | None:
        return self._cycle_type

    @cycle_type.setter
    def cycle_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cycle_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def treatment_number(self) -> int | None:
        return self._treatment_number

    @treatment_number.setter
    def treatment_number(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_number,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def cycle_number(self) -> int | None:
        return self._cycle_number

    @cycle_number.setter
    def cycle_number(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cycle_number,
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
    def recieved_treatment_this_cycle(self) -> bool | None:
        return self._recieved_treatment_this_cycle

    @recieved_treatment_this_cycle.setter
    def recieved_treatment_this_cycle(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.recieved_treatment_this_cycle,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def iv_dose_prescribed_unit(self) -> str | None:
        return self._iv_dose_prescribed_unit

    @iv_dose_prescribed_unit.setter
    def iv_dose_prescribed_unit(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.iv_dose_prescribed_unit,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def iv_dose_prescribed(self) -> str | None:
        return self._iv_dose_prescribed

    @iv_dose_prescribed.setter
    def iv_dose_prescribed(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.iv_dose_prescribed,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def was_total_dose_delivered(self) -> str | None:
        return self._was_total_dose_delivered

    @was_total_dose_delivered.setter
    def was_total_dose_delivered(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_total_dose_delivered,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    # oral only
    @property
    def was_dose_administered_to_spec(self) -> bool | None:
        return self._was_dose_administered_to_spec

    @was_dose_administered_to_spec.setter
    def was_dose_administered_to_spec(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_dose_administered_to_spec,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def reason_not_administered_to_spec(self) -> str | None:
        return self._reason_not_administered_to_spec

    @reason_not_administered_to_spec.setter
    def reason_not_administered_to_spec(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.reason_not_administered_to_spec,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def oral_dose_prescribed_per_day(self) -> float | None:
        return self._oral_dose_prescribed_per_day

    @oral_dose_prescribed_per_day.setter
    def oral_dose_prescribed_per_day(self, value: float | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.oral_dose_prescribed_per_day,
            value=value,
            validator=StrictValidators.validate_optional_float,
        )

    @property
    def oral_dose_unit(self) -> str | None:
        return self._oral_dose_unit

    @oral_dose_unit.setter
    def oral_dose_unit(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.oral_dose_unit,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def other_dose_unit(self) -> str | None:
        return self._other_dose_unit

    @other_dose_unit.setter
    def other_dose_unit(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.other_dose_unit,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def number_of_days_tablet_not_taken(self) -> int | None:
        return self._number_of_days_tablet_not_taken

    @number_of_days_tablet_not_taken.setter
    def number_of_days_tablet_not_taken(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.number_of_days_tablet_not_taken,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def reason_tablet_not_taken(self) -> str | None:
        return self._reason_tablet_not_taken

    @reason_tablet_not_taken.setter
    def reason_tablet_not_taken(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.reason_tablet_not_taken,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def was_tablet_taken_to_prescription_in_previous_cycle(self) -> bool | None:
        return self._was_tablet_taken_to_prescription_in_previous_cycle

    @was_tablet_taken_to_prescription_in_previous_cycle.setter
    def was_tablet_taken_to_prescription_in_previous_cycle(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_tablet_taken_to_prescription_in_previous_cycle,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}{delim} "
            f"cycle_type={self.cycle_type!r}{delim} "
            f"treatment_number={self.treatment_number!r}{delim} "
            f"cycle_number={self.cycle_number!r}{delim} "
            f"start_date={self.start_date!r}{delim} "
            f"end_date={self.end_date!r}{delim} "
            f"recieved_treatment_this_cycle={self.recieved_treatment_this_cycle!r}{delim} "
            f"iv_dose_prescribed={self.iv_dose_prescribed!r}{delim} "
            f"iv_dose_prescribed_unit={self.iv_dose_prescribed_unit!r}{delim} "
            f"was_total_dose_delivered={self.was_total_dose_delivered!r}{delim} "
            f"was_dose_administered_to_spec={self.was_dose_administered_to_spec!r}{delim} "
            f"reason_not_administered_to_spec={self.reason_not_administered_to_spec!r}{delim} "
            f"oral_dose_prescribed_per_day={self.oral_dose_prescribed_per_day!r}{delim} "
            f"oral_dose_unit={self.oral_dose_unit!r}{delim} "
            f"other_dose_unit={self.other_dose_unit!r}{delim} "
            f"number_of_days_tablet_not_taken={self.number_of_days_tablet_not_taken!r}{delim} "
            f"reason_tablet_not_taken={self.reason_tablet_not_taken!r}{delim} "
            f"was_tablet_taken_to_prescription_in_previous_cycle={self.was_tablet_taken_to_prescription_in_previous_cycle!r}"
            f")"
        )
