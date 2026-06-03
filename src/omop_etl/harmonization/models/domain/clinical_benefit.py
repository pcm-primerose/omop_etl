from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class ClinicalBenefit(DomainBase):
    """
    Clinical benefit assessment at a source-specific timepoint. Each patient
    has at most one instace, the timepoint varies by trial (e.g. IMPRESS uses W16,
    other sources may use W24). week is recorded explicitly so downstream
    consumers can filter by timepoint without joining trial metadata.

    Identity (NATURAL_KEY_FIELDS) = (has_benefit,): the singleton anchors on has_benefit.
    Validity (REQUIRED_FIELDS) = (has_benefit,): a clinical-benefit record without
    the outcome is invalid.

    Fields:
    - has_benefit: If the patient had clinical benefit at the timepoint.
    - week: Source-speciifc assessment timepoint (e.g. 16 for week 16).
    - date: Assessment date.
    """

    class Fields:
        WEEK = "week"
        HAS_BENEFIT = "has_benefit"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._week: int | None = None
        self._has_benefit: bool | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    NATURAL_KEY_FIELDS = (Fields.HAS_BENEFIT,)
    REQUIRED_FIELDS = (Fields.HAS_BENEFIT,)

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def week(self) -> int | None:
        return self._week

    @week.setter
    def week(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.week,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def has_benefit(self) -> bool | None:
        return self._has_benefit

    @has_benefit.setter
    def has_benefit(self, value: bool | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.has_benefit,
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

    def __repr__(self, delim=","):
        return f"{self.__class__.__name__}(week={self.week!r}{delim}has_benefit={self.has_benefit!r}{delim} date={self.date!r})"
