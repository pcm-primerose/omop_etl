from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class Biomarkers(DomainBase):
    """
    The patient's target biomarker (one per patient).

    A single canonical field, each processor resolves its own per-source
    preference into it (IMPRESS: cohort target name, then mutation, then measured
    gene).

    Identity (NATURAL_KEY_FIELDS) = (target_biomarker,): a singleton anchors on its required field.
    Validity (REQUIRED_FIELDS) = (target_biomarker,): a biomarker record without a
    target isn't usable.

    Fields:
    - target_biomarker: The biomarker the patient was enrolled/tested for (subject).
    - date: Biomarker assessment date (used by the measurement builder).
    """

    class Fields:
        TARGET_BIOMARKER = "target_biomarker"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._target_biomarker: str | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    REQUIRED_FIELDS = (Fields.TARGET_BIOMARKER,)
    NATURAL_KEY_FIELDS = (Fields.TARGET_BIOMARKER,)

    @property
    def target_biomarker(self) -> str | None:
        return self._target_biomarker

    @target_biomarker.setter
    def target_biomarker(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.target_biomarker,
            value=value,
            validator=StrictValidators.validate_optional_str,
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
        return f"{self.__class__.__name__}(target_biomarker={self.target_biomarker!r}{delim} date={self.date!r})"
