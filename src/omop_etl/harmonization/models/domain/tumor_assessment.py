from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TumorAssessment(DomainBase):
    class Fields:
        ASSESSMENT_TYPE = "assessment_type"
        TARGET_LESION_SIZE = "target_lesion_size"
        TARGET_LESION_CHANGE_FROM_BASELINE = "target_lesion_change_from_baseline"
        TARGET_LESION_CHANGE_FROM_NADIR = "target_lesion_change_from_nadir"
        WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE = "was_new_lesions_registered_after_baseline"
        DATE = "date"
        RECIST_RESPONSE = "recist_response"
        IRECIST_RESPONSE = "irecist_response"
        RANO_RESPONSE = "rano_response"
        RECIST_DATE_OF_PROGRESSION = "recist_date_of_progression"
        IRECIST_DATE_OF_PROGRESSION = "irecist_date_of_progression"
        EVENT_ID = "event_id"
        WAS_BASELINE = "was_baseline"
        BASELINE_OFF_TARGET_LESIONS_NUMBER = "baseline_off_target_lesions_number"
        BASELINE_OFF_TARGET_LESION_MEASUREMENT_DATE = "baseline_off_target_lesion_measurement_date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._assessment_type: str | None = None
        self._target_lesion_size: float | None = None
        self._target_lesion_change_from_baseline: float | None = None
        self._target_lesion_change_from_nadir: float | None = None
        self._was_new_lesions_registered_after_baseline: bool | None = None
        self._date: dt.date | None = None
        self._recist_response: str | None = None
        self._irecist_response: str | None = None
        self._rano_response: str | None = None
        self._recist_date_of_progression: dt.date | None = None
        self._irecist_date_of_progression: dt.date | None = None
        self._event_id: str | None = None
        # baseline-only fields: populated where was_baseline is True, else None.
        # they describe the baseline visit, not each assessment.
        self._was_baseline: bool | None = None
        self._baseline_off_target_lesions_number: int | None = None
        self._baseline_off_target_lesion_measurement_date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    INVARIANT_FIELDS = (Fields.ASSESSMENT_TYPE,)
    NATURAL_KEY_FIELDS = (Fields.ASSESSMENT_TYPE, Fields.DATE, Fields.EVENT_ID)

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
    def target_lesion_size(self) -> float | None:
        return self._target_lesion_size

    @target_lesion_size.setter
    def target_lesion_size(self, value: float | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_size,
            value=value,
            validator=StrictValidators.validate_optional_float,
        )

    @property
    def target_lesion_change_from_baseline(self) -> float | None:
        return self._target_lesion_change_from_baseline

    @target_lesion_change_from_baseline.setter
    def target_lesion_change_from_baseline(self, value: float | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_change_from_baseline,
            value=value,
            validator=StrictValidators.validate_optional_float,
        )

    @property
    def target_lesion_change_from_nadir(self) -> float | None:
        return self._target_lesion_change_from_nadir

    @target_lesion_change_from_nadir.setter
    def target_lesion_change_from_nadir(self, value: float | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_lesion_change_from_nadir,
            value=value,
            validator=StrictValidators.validate_optional_float,
        )

    @property
    def was_new_lesions_registered_after_baseline(self) -> bool | None:
        return self._was_new_lesions_registered_after_baseline

    @was_new_lesions_registered_after_baseline.setter
    def was_new_lesions_registered_after_baseline(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_new_lesions_registered_after_baseline,
            value=value,
            validator=StrictValidators.validate_optional_bool,
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

    @property
    def recist_response(self) -> str | None:
        return self._recist_response

    @recist_response.setter
    def recist_response(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.recist_response,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def irecist_response(self) -> str | None:
        return self._irecist_response

    @irecist_response.setter
    def irecist_response(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.irecist_response,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def rano_response(self) -> str | None:
        return self._rano_response

    @rano_response.setter
    def rano_response(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.rano_response,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def recist_date_of_progression(self) -> dt.date | None:
        return self._recist_date_of_progression

    @recist_date_of_progression.setter
    def recist_date_of_progression(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.recist_date_of_progression,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def irecist_date_of_progression(self) -> dt.date | None:
        return self._irecist_date_of_progression

    @irecist_date_of_progression.setter
    def irecist_date_of_progression(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.irecist_date_of_progression,
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

    @property
    def was_baseline(self) -> bool | None:
        return self._was_baseline

    @was_baseline.setter
    def was_baseline(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_baseline,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def baseline_off_target_lesions_number(self) -> int | None:
        return self._baseline_off_target_lesions_number

    @baseline_off_target_lesions_number.setter
    def baseline_off_target_lesions_number(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.baseline_off_target_lesions_number,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def baseline_off_target_lesion_measurement_date(self) -> dt.date | None:
        return self._baseline_off_target_lesion_measurement_date

    @baseline_off_target_lesion_measurement_date.setter
    def baseline_off_target_lesion_measurement_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.baseline_off_target_lesion_measurement_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=","):
        return (
            f"{self.__class__.__name__}("
            f"assessment_type={self.assessment_type!r}{delim} "
            f"target_lesion_size={self.target_lesion_size!r}{delim} "
            f"target_lesion_change_from_baseline={self.target_lesion_change_from_baseline!r}{delim} "
            f"target_lesion_change_from_nadir={self.target_lesion_change_from_nadir!r}{delim} "
            f"was_new_lesions_registered_after_baseline={self.was_new_lesions_registered_after_baseline!r}{delim} "
            f"date={self.date!r}{delim} "
            f"recist_response={self.recist_response!r}{delim} "
            f"irecist_response={self.irecist_response!r}{delim} "
            f"rano_response={self.rano_response!r}{delim} "
            f"recist_date_of_progression={self.recist_date_of_progression!r}{delim} "
            f"irecist_date_of_progression={self.irecist_date_of_progression!r}{delim} "
            f"event_id={self.event_id!r}{delim} "
            f"was_baseline={self.was_baseline!r}{delim} "
            f"baseline_off_target_lesions_number={self.baseline_off_target_lesions_number!r}{delim} "
            f"baseline_off_target_lesion_measurement_date={self.baseline_off_target_lesion_measurement_date!r}"
            f")"
        )
