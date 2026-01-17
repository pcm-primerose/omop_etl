from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TumorAssessmentBaseline(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._assessment_type: str | None = None
        self._assessment_date: dt.date | None = None
        self._target_lesion_size: int | None = None
        self._target_lesion_nadir: int | None = None
        self._target_lesion_measurement_date: dt.date | None = None
        self._off_target_lesions_number: int | None = None
        self._off_target_lesion_measurement_date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def assessment_type(self) -> str | None:
        return self._assessment_type

    @assessment_type.setter
    def assessment_type(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.assessment_type,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def assessment_date(self) -> dt.date | None:
        return self._assessment_date

    @assessment_date.setter
    def assessment_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.assessment_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def target_lesion_size(self) -> int | None:
        return self._target_lesion_size

    @target_lesion_size.setter
    def target_lesion_size(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_size,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def target_lesion_nadir(self) -> int | None:
        return self._target_lesion_nadir

    @target_lesion_nadir.setter
    def target_lesion_nadir(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_nadir,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def target_lesion_measurement_date(self) -> dt.date | None:
        return self._target_lesion_measurement_date

    @target_lesion_measurement_date.setter
    def target_lesion_measurement_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_measurement_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def off_target_lesions_number(self) -> int | None:
        return self._off_target_lesions_number

    @off_target_lesions_number.setter
    def off_target_lesions_number(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.off_target_lesions_number,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def off_target_lesion_measurement_date(self) -> dt.date | None:
        return self._off_target_lesion_measurement_date

    @off_target_lesion_measurement_date.setter
    def off_target_lesion_measurement_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.off_target_lesion_measurement_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"assessment_type={self.assessment_type!r}{delim} "
            f"assessment_date={self.assessment_date!r}{delim} "
            f"target_lesion_size={self.target_lesion_size!r}{delim} "
            f"target_lesion_nadir={self.target_lesion_nadir!r}{delim} "
            f"target_lesion_measurement_date={self.target_lesion_measurement_date!r}{delim} "
            f"off_target_lesion_size={self.off_target_lesions_number!r}{delim} "
            f"off_target_lesion_measurement_date={self.off_target_lesion_measurement_date!r}"
            f")"
        )
