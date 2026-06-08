from enum import Enum
from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class RelatedStatus(str, Enum):
    RELATED = "related"  # code 4
    NOT_RELATED = "not_related"  # code 1
    UNKNOWN = "unknown"  # code 2 & 3


class AdverseEvent(DomainBase):
    """
    An adverse event the patient experienced during the study.

    Identity (NATURAL_KEY_FIELDS) = (term, start_date, sequence_id): the event and
    when it started; sequence_id (source column, not a calculated index) is a
    nullable tiebreaker for the same term on the same date.
    Validity (REQUIRED_FIELDS) = (term,): AE without the term is not usable.

    Fields:
    - term: Adverse-event term (e.g. "Rash").
    - grade: CTCAE severity grade (1-5).
    - outcome: Event outcome (e.g. "Recovered/resolved").
    - start_date: Event start date.
    - end_date: Event end date.
    - sequence_id: Source sequence id, nullable same-date tiebreaker.
    - was_serious: If the event is a serious adverse event (SAE).
    - turned_serious_date: Date the event became serious.
    - related_to_treatment_1_status: Relatedness to study treatment 1 (related / not_related / unknown).
    - treatment_1_name: Name of study treatment 1.
    - related_to_treatment_2_status: Relatedness to study treatment 2.
    - treatment_2_name: Name of study treatment 2.
    - was_serious_grade_expected_treatment_1: If the serious-grade event was expected for treatment 1.
    - was_serious_grade_expected_treatment_2: If the serious-grade event was expected for treatment 2.
    """

    class Fields:
        TERM = "term"
        GRADE = "grade"
        OUTCOME = "outcome"
        START_DATE = "start_date"
        END_DATE = "end_date"
        SEQUENCE_ID = "sequence_id"
        WAS_SERIOUS = "was_serious"
        TURNED_SERIOUS_DATE = "turned_serious_date"
        RELATED_TO_TREATMENT_1_STATUS = "related_to_treatment_1_status"
        TREATMENT_1_NAME = "treatment_1_name"
        RELATED_TO_TREATMENT_2_STATUS = "related_to_treatment_2_status"
        TREATMENT_2_NAME = "treatment_2_name"
        WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_1 = "was_serious_grade_expected_treatment_1"
        WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_2 = "was_serious_grade_expected_treatment_2"

    REQUIRED_FIELDS = (Fields.TERM,)
    NATURAL_KEY_FIELDS = (Fields.TERM, Fields.START_DATE, Fields.SEQUENCE_ID)

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._term: str | None = None
        self._grade: int | None = None
        self._outcome: str | None = None
        self._start_date: dt.date | None = None
        self._end_date: dt.date | None = None
        self._sequence_id: int | None = None
        self._was_serious: bool | None = None
        self._turned_serious_date: dt.date | None = None
        self._related_to_treatment_1_status: RelatedStatus | None = None
        self._treatment_1_name: str | None = None
        self._related_to_treatment_2_status: RelatedStatus | None = None
        self._treatment_2_name: str | None = None
        self._was_serious_grade_expected_treatment_1: bool | None = None
        self._was_serious_grade_expected_treatment_2: bool | None = None
        self.updated_fields: Set[str] = set()

    @property
    def term(self) -> str | None:
        return self._term

    @term.setter
    def term(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.term,
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
    def outcome(self) -> str | None:
        return self._outcome

    @outcome.setter
    def outcome(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.outcome,
            value=value,
            validator=StrictValidators.validate_optional_str,
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

    @property
    def was_serious(self) -> bool | None:
        return self._was_serious

    @was_serious.setter
    def was_serious(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_serious,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def turned_serious_date(self) -> dt.date | None:
        return self._turned_serious_date

    @turned_serious_date.setter
    def turned_serious_date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.turned_serious_date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    @property
    def related_to_treatment_1_status(self) -> RelatedStatus | None:
        return self._related_to_treatment_1_status

    @related_to_treatment_1_status.setter
    def related_to_treatment_1_status(self, value: RelatedStatus | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.related_to_treatment_1_status,
            value=value,
            validator=StrictValidators.validate_optional_enum,
            enum_type=RelatedStatus,
        )

    @property
    def treatment_1_name(self) -> str | None:
        return self._treatment_1_name

    @treatment_1_name.setter
    def treatment_1_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_1_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def related_to_treatment_2_status(self) -> RelatedStatus | None:
        return self._related_to_treatment_2_status

    @related_to_treatment_2_status.setter
    def related_to_treatment_2_status(self, value: RelatedStatus | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.related_to_treatment_2_status,
            value=value,
            validator=StrictValidators.validate_optional_enum,
            enum_type=RelatedStatus,
        )

    @property
    def treatment_2_name(self) -> str | None:
        return self._treatment_2_name

    @treatment_2_name.setter
    def treatment_2_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.treatment_2_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def was_serious_grade_expected_treatment_1(self) -> bool | None:
        return self._was_serious_grade_expected_treatment_1

    @was_serious_grade_expected_treatment_1.setter
    def was_serious_grade_expected_treatment_1(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_serious_grade_expected_treatment_1,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    @property
    def was_serious_grade_expected_treatment_2(self) -> bool | None:
        return self._was_serious_grade_expected_treatment_2

    @was_serious_grade_expected_treatment_2.setter
    def was_serious_grade_expected_treatment_2(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.was_serious_grade_expected_treatment_2,
            value=value,
            validator=StrictValidators.validate_optional_bool,
        )

    def __repr__(self, delim=",") -> str:
        return (
            f"{self.__class__.__name__}("
            f"patient_id={self.patient_id!r}{delim} "
            f"term={self.term!r}{delim} "
            f"grade={self.grade!r}{delim} "
            f"outcome={self.outcome!r}{delim} "
            f"start_date={self.start_date!r}{delim} "
            f"end_date={self.end_date!r}{delim} "
            f"sequence_id={self.sequence_id!r}{delim}"
            f"was_serious={self.was_serious!r}{delim} "
            f"turned_serious_date={self.turned_serious_date!r}{delim} "
            f"related_to_treatment_1_status={self.related_to_treatment_1_status!r}{delim} "
            f"treatment_1_name={self.treatment_1_name!r}{delim} "
            f"related_to_treatment_2_status={self.related_to_treatment_2_status!r}{delim} "
            f"treatment_2_name={self.treatment_2_name!r}{delim} "
            f"was_serious_grade_expected_treatment_1={self.was_serious_grade_expected_treatment_1!r}{delim} "
            f"was_serious_grade_expected_treatment_2={self.was_serious_grade_expected_treatment_2!r}"
            f")"
        )
