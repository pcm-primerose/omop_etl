from dataclasses import field
from pydantic.dataclasses import dataclass as pd_dataclass
from pydantic import Field as pd_field
import datetime as dt
from typing import ClassVar

from omop_etl.omop.core.row_validator import validate_required_fields


@pd_dataclass(frozen=True, slots=True)
class PersonRow:
    """
    OMOP Person table row.
    https://ohdsi.github.io/CommonDataModel/cdm54.html#person

    Natural key: person_id (effectively no deduplication - one per patient)
    """

    natural_key_fields: ClassVar[tuple[str, ...]] = ("person_id",)

    person_id: int
    gender_concept_id: int
    year_of_birth: int
    race_concept_id: int
    ethnicity_concept_id: int
    person_source_value: str
    gender_source_concept_id: int
    race_source_concept_id: int
    ethnicity_source_concept_id: int
    gender_source_value: str | None = None
    day_of_birth: int | None = None
    month_of_birth: int | None = None
    birth_datetime: dt.datetime | None = None
    race_source_value: str | None = None
    ethnicity_source_value: str | None = None

    # provenance tracking (excluded from comparison/hash)
    _source_field: str | None = field(default=None, compare=False, hash=False, repr=False)

    def natural_key(self) -> tuple:
        return tuple(getattr(self, f) for f in self.natural_key_fields)

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)


@pd_dataclass(frozen=True, slots=True)
class ObservationPeriodRow:
    """
    OMOP ObservationPeriod table row.
    https://ohdsi.github.io/CommonDataModel/cdm54.html#observation_period

    Natural key: person_id (one observation period per patient)
    """

    natural_key_fields: ClassVar[tuple[str, ...]] = ("person_id",)

    observation_period_id: int
    person_id: int
    observation_period_start_date: dt.date
    observation_period_end_date: dt.date
    period_type_concept_id: int

    # provenance tracking (excluded from comparison/hash)
    _source_field: str | None = field(default=None, compare=False, hash=False, repr=False)

    def natural_key(self) -> tuple:
        return tuple(getattr(self, f) for f in self.natural_key_fields)

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)


@pd_dataclass(frozen=True, slots=True)
class CdmSourceRow:
    """
    OMOP CdmSource table row.
    https://ohdsi.github.io/CommonDataModel/cdm54.html#cdm_source
    """

    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version_concept_id: int
    cdm_source_name: str = pd_field(max_length=255)
    cdm_source_abbreviation: str = pd_field(max_length=25)
    cdm_holder: str = pd_field(max_length=255)
    vocabulary_version: str = pd_field(max_length=20)
    source_description: str | None = pd_field(None, max_length=2147483647)
    source_documentation_reference: str | None = pd_field(max_length=255)
    cdm_etl_reference: str | None = pd_field(max_length=255)
    cdm_version: str | None = pd_field(max_length=10)

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)


@pd_dataclass(frozen=True, slots=True)
class VisitOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
    """

    natural_key_fields: ClassVar[tuple[str, ...]] = ("person_id",)

    visit_occurrence_id: int
    person_id: int
    visit_concept_id: int
    visit_start_date: dt.date
    visit_end_date: dt.date
    visit_type_concept_id: int
    visit_start_datetime: dt.datetime | None = None
    visit_end_datetime: dt.datetime | None = None
    provider_id: int | None = None
    care_site_id: int | None = None
    visit_source_value: str | None = None
    visit_source_concept_id: int | None = None
    admitted_from_concept_id: int | None = None
    admitted_from_source_value: str | None = None
    discharged_to_concept_id: int | None = None
    discharged_to_source_value: str | None = None
    preceding_visit_occurrence_id: int | None = None
