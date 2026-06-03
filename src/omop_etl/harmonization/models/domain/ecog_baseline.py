from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class EcogBaseline(DomainBase):
    """
    The patient's baseline ECOG performance status (one per patient).

    Identity (NATURAL_KEY_FIELDS) = (grade,): a singleton anchors on its required field(s).
    Validity (REQUIRED_FIELDS) = (grade,): a baseline ECOG without a grade isn't a real record.

    Fields:
    - grade: ECOG performance status (0-5).
    - description: text description of the grade (e.g. "Fully active").
    - date: baseline assessment date (used by the measurement builder).
    """

    class Fields:
        GRADE = "grade"
        DESCRIPTION = "description"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._description: str | None = None
        self._grade: int | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    NATURAL_KEY_FIELDS = (Fields.GRADE,)
    REQUIRED_FIELDS = (Fields.GRADE,)

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def description(self) -> str | None:
        return self._description

    @description.setter
    def description(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.description,
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
    def date(self) -> dt.date | None:
        return self._date

    @date.setter
    def date(self, value: dt.date | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.date,
            value=value,
            validator=StrictValidators.validate_optional_date,
        )

    def __repr__(self, delim=","):
        return f"{self.__class__.__name__}(description={self.description!r}{delim} grade={self.grade!r}, date={self.date!r})"
