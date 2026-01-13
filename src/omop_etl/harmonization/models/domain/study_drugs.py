from typing import Set

from omop_etl.harmonization.core.track_validated import TrackedValidated
from omop_etl.harmonization.core.validators import StrictValidators


class StudyDrugs(TrackedValidated):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._primary_treatment_drug: str | None = None
        self._primary_treatment_drug_code: int | None = None
        self._secondary_treatment_drug: str | None = None
        self._secondary_treatment_drug_code: int | None = None
        self.updated_fields: Set[str] = set()

    @property
    def primary_treatment_drug(self) -> str | None:
        return self._primary_treatment_drug

    @primary_treatment_drug.setter
    def primary_treatment_drug(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.primary_treatment_drug,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def secondary_treatment_drug(self) -> str | None:
        return self._secondary_treatment_drug

    @secondary_treatment_drug.setter
    def secondary_treatment_drug(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.secondary_treatment_drug,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def primary_treatment_drug_code(self) -> int | None:
        return self._primary_treatment_drug_code

    @primary_treatment_drug_code.setter
    def primary_treatment_drug_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.primary_treatment_drug_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def secondary_treatment_drug_code(self) -> int | None:
        return self._secondary_treatment_drug_code

    @secondary_treatment_drug_code.setter
    def secondary_treatment_drug_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.secondary_treatment_drug_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    def __repr__(self, delim=","):
        return (
            f"{self.__class__.__name__}("
            f"primary_treatment_drug={self.primary_treatment_drug!r}{delim} "
            f" primary_treatment_drug_code={self.primary_treatment_drug_code!r}{delim} "
            f" secondary_treatment_drug={self.secondary_treatment_drug!r}{delim} "
            f" secondary_treatment_drug_code={self.secondary_treatment_drug_code!r})"
        )
