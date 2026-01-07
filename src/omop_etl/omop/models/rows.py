from dataclasses import dataclass, field
import datetime as dt
from typing import ClassVar

from omop_etl.omop.core.row_validator import validate_required_fields


@dataclass(frozen=True, slots=True)
class PersonRow:
    """OMOP Person table row.

    Natural key: person_id (effectively no deduplication - one per patient)
    """

    natural_key_fields: ClassVar[tuple[str, ...]] = ("person_id",)

    person_id: int
    gender_concept_id: int
    year_of_birth: int
    month_of_birth: int | None
    day_of_birth: int | None
    birth_datetime: dt.datetime | None
    race_concept_id: int
    ethnicity_concept_id: int
    person_source_value: str
    gender_source_value: str | None
    gender_source_concept_id: int
    race_source_value: str | None
    race_source_concept_id: int
    ethnicity_source_value: str | None
    ethnicity_source_concept_id: int

    # provenance tracking (excluded from comparison/hash)
    _source_field: str | None = field(default=None, compare=False, hash=False, repr=False)

    def natural_key(self) -> tuple:
        return tuple(getattr(self, f) for f in self.natural_key_fields)

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)


@dataclass(frozen=True, slots=True)
class ObservationPeriodRow:
    """OMOP ObservationPeriod table row.

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


@dataclass(frozen=True, slots=True)
class CdmSourceRow:
    """
    OMOP CdmSource table row.
    """

    cdm_source_name: str
    cdm_source_abbreviation: str
    cdm_holder: str
    source_description: str | None
    source_documentation_reference: str | None
    cdm_etl_reference: str | None
    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version: str | None
    cdm_version_concept_id: int
    vocabulary_version: str

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)
