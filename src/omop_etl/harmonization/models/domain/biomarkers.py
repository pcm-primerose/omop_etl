from typing import Set
import datetime as dt

from omop_etl.harmonization.core.validators import StrictValidators
from omop_etl.harmonization.models.domain.base import DomainBase


class Biomarkers(DomainBase):
    def __init__(self, patient_id: str):
        self._patient_id = patient_id
        self._gene_and_mutation: str | None = None
        self._gene_and_mutation_code: int | None = None
        self._cohort_target_name: str | None = None
        self._cohort_target_mutation: str | None = None
        self._date: dt.date | None = None
        self.updated_fields: Set[str] = set()

    @property
    def gene_and_mutation(self) -> str | None:
        return self._gene_and_mutation

    @gene_and_mutation.setter
    def gene_and_mutation(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.gene_and_mutation,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def gene_and_mutation_code(self) -> int | None:
        return self._gene_and_mutation_code

    @gene_and_mutation_code.setter
    def gene_and_mutation_code(self, value: int | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.gene_and_mutation_code,
            value=value,
            validator=StrictValidators.validate_optional_int,
        )

    @property
    def cohort_target_name(self) -> str | None:
        return self._cohort_target_name

    @cohort_target_name.setter
    def cohort_target_name(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cohort_target_name,
            value=value,
            validator=StrictValidators.validate_optional_str,
        )

    @property
    def cohort_target_mutation(self) -> str | None:
        return self._cohort_target_mutation

    @cohort_target_mutation.setter
    def cohort_target_mutation(self, value: str | None) -> None:
        self._set_validated_prop(
            prop=self.__class__.cohort_target_mutation,
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
        return (
            f"{self.__class__.__name__}("
            f"gene_and_mutation={self.gene_and_mutation!r}{delim} "
            f" gene_and_mutation_code={self.gene_and_mutation_code!r}{delim} "
            f" cohort_target_name={self.cohort_target_name!r}{delim} "
            f" cohort_target_mutation={self.cohort_target_mutation!r}{delim} "
            f" date={self.date!r})"
        )
