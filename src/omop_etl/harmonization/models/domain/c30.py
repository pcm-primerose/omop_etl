from typing import Set
import datetime as dt

from omop_etl.harmonization.core.make_validated_property import make_validated_property
from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class C30(DomainBase):
    Q_COUNT = 30

    class Cols:
        DATE = "date"
        EVENT_NAME = "event_name"
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
        Q6 = "q6"
        Q6_CODE = "q6_code"
        Q7 = "q7"
        Q7_CODE = "q7_code"
        Q8 = "q8"
        Q8_CODE = "q8_code"
        Q9 = "q9"
        Q9_CODE = "q9_code"
        Q10 = "q10"
        Q10_CODE = "q10_code"
        Q11 = "q11"
        Q11_CODE = "q11_code"
        Q12 = "q12"
        Q12_CODE = "q12_code"
        Q13 = "q13"
        Q13_CODE = "q13_code"
        Q14 = "q14"
        Q14_CODE = "q14_code"
        Q15 = "q15"
        Q15_CODE = "q15_code"
        Q16 = "q16"
        Q16_CODE = "q16_code"
        Q17 = "q17"
        Q17_CODE = "q17_code"
        Q18 = "q18"
        Q18_CODE = "q18_code"
        Q19 = "q19"
        Q19_CODE = "q19_code"
        Q20 = "q20"
        Q20_CODE = "q20_code"
        Q21 = "q21"
        Q21_CODE = "q21_code"
        Q22 = "q22"
        Q22_CODE = "q22_code"
        Q23 = "q23"
        Q23_CODE = "q23_code"
        Q24 = "q24"
        Q24_CODE = "q24_code"
        Q25 = "q25"
        Q25_CODE = "q25_code"
        Q26 = "q26"
        Q26_CODE = "q26_code"
        Q27 = "q27"
        Q27_CODE = "q27_code"
        Q28 = "q28"
        Q28_CODE = "q28_code"
        Q29 = "q29"
        Q29_CODE = "q29_code"
        Q30 = "q30"
        Q30_CODE = "q30_code"

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
