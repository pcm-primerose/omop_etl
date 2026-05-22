from enum import Enum
from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class TrialOutcomeStatus(str, Enum):
    """
    Standardized trial-outcome status, aligned with the OHDSI Clinical
    Trials WG (trial enrollment and trial outcome).

    - COMPLETED: the patient completed the trial protocol to specification
      per the trial's own completion definition. For IMPRESS this is the
      EOT reason "Normal completion according to cohort-specific manual".
    - WITHDRAWN: the patient ended treatment for any other recorded
      reason (e.g. disease progression, adverse event, patient decision,
      investigator decision, lost to follow-up). The specific
      reason text is preserved on the singleton's `reason` field and
      mapped to a value_as_concept_id by the observation builder.

    Each trial's harmonizer is responsible for deciding which source
    signal (text, source code, or combined logic) maps to which status.
    The categorization is trial-specific, but the resulting enum value
    is standardized so the OMOP builder can stay trial-agnostic.
    """

    COMPLETED = "completed"
    WITHDRAWN = "withdrawn"


class EndOfTreatment(DomainBase):
    """
    End-of-treatment record for the patient. At most one per patient.

    Fields:
    - status: a TrialOutcomeStatus enum value (COMPLETED or WITHDRAWN),
      or None when no EOT event was recorded in source.
    - reason: raw EOT reason text from source.
    - date: end-of-treatment date. Derived per trial.
    """

    class Fields:
        STATUS = "status"
        REASON = "reason"
        DATE = "date"

    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._status: TrialOutcomeStatus | None = None
        self._reason: str | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    NATURAL_KEY_FIELDS = (Fields.DATE,)

    @property
    def patient_id(self) -> str:
        return self._patient_id

    @property
    def status(self) -> TrialOutcomeStatus | None:
        return self._status

    @status.setter
    def status(self, value: TrialOutcomeStatus | str | None) -> None:
        if isinstance(value, str) and not isinstance(value, TrialOutcomeStatus):
            value = TrialOutcomeStatus(value)
        self._set_validated_prop(
            prop=self.__class__.status,
            value=value,
            validator=self._validate_optional_status,
        )

    @staticmethod
    def _validate_optional_status(value, field_name: str) -> TrialOutcomeStatus | None:
        if value is None:
            return None
        if isinstance(value, TrialOutcomeStatus):
            return value
        raise ValueError(f"{field_name}: expected TrialOutcomeStatus or None, got {type(value).__name__}: {value!r}")

    @property
    def reason(self) -> str | None:
        return self._reason

    @reason.setter
    def reason(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.reason,
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
        return f"{self.__class__.__name__}(status={self.status!r}{delim} reason={self.reason!r}{delim} date={self.date!r})"
