from typing import Set
import datetime as dt

from omop_etl.harmonization.core.make_validated_property import make_validated_property
from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class EQ5D(DomainBase):
    """
    One EQ-5D quality-of-life questionnaire response at a timepoint.

    The five item answers (q1..q5 + codes) and qol_metric are conditionally
    populated: materiality is OR-shaped ("has at least one answer"), this is enforced in
    the processors, not required.

    Identity (NATURAL_KEY_FIELDS) = (event_name, date): the questionnaire event and
    when it was taken.
    Validity (REQUIRED_FIELDS) = (event_name, date): collection NKs need a
    guaranteed non-null core to be real dedup-identity.

    Fields:
    - date: Date when questionnaire was carried out.
    - event_name: Source-specific event name (e.g. "PROMS on paper, w16").
    - qol_metric: QoL score (0-100) for this time-point.
    - q1: Text answer of mobility score.
    - q1_code: Answer code (1-5) of mobility score.
    - q2: Text answer of self-care score.
    - q2_code: Answer code (1-5) of self-care score.
    - q3: Text answer of usual activities score.
    - q3_code: Answer code (1-5) of usual activities score.
    - q4: Text answer of pain discomfort score.
    - q4_code: Answer code (1-5) of pain discomfort score.
    - q5: Text answer of anxiety depression score.
    - q5_code: Answer code (1-5) of anxiety depression score.
    """

    Q_COUNT = 5

    class Fields:
        DATE = "date"
        EVENT_NAME = "event_name"
        QOL_METRIC = "qol_metric"
        Q1 = "q1"
        Q1_CODE = "q1_code"
        Q2 = "q2"
        Q2_CODE = "q2_code"
        Q3 = "q3"
        Q3_CODE = "q3_code"
        Q4 = "q4"
        Q4_CODE = "q4_code"
        Q5 = "q5"
        Q5_CODE = "q5_code"

    def __init__(self, patient_id: str):
        self.updated_fields: Set[str] = set()
        self._patient_id = patient_id
        self._date: dt.date | None = None
        self._event_name: str | None = None
        self._qol_metric: int | None = None

    NATURAL_KEY_FIELDS = (Fields.EVENT_NAME, Fields.DATE)
    REQUIRED_FIELDS = (Fields.EVENT_NAME, Fields.DATE)

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

    @property
    def qol_metric(self) -> int | None:
        return self._qol_metric

    @qol_metric.setter
    def qol_metric(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.qol_metric,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    def __repr__(self) -> str:
        base = [
            f"patient_id={self.patient_id!r}",
            f"date={self.date!r}",
            f"event_name={self.event_name!r}",
            f"qol_metric={self.qol_metric!r}",
        ]
        qs = [f"q{i}={getattr(self, f'q{i}')!r}" for i in range(1, self.Q_COUNT + 1) if getattr(self, f"q{i}", None) is not None]
        qcs = [f"q{i}_code={getattr(self, f'q{i}_code')!r}" for i in range(1, self.Q_COUNT + 1) if getattr(self, f"q{i}_code", None) is not None]
        return f"{self.__class__.__name__}({', '.join(base + qs + qcs)})"


# generate q1-q5 and q1_code-q5_code properties at class definition time
for _i in range(1, EQ5D.Q_COUNT + 1):
    setattr(EQ5D, f"q{_i}", make_validated_property(f"q{_i}", StrictValidators.validate_optional_str))
    setattr(EQ5D, f"q{_i}_code", make_validated_property(f"q{_i}_code", StrictValidators.validate_optional_int))
