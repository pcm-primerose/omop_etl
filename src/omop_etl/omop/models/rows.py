from dataclasses import dataclass, field
import datetime as dt
from typing import ClassVar

from omop_etl.omop.core.row_validator import validate_required_fields


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
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


@dataclass(frozen=True, slots=True)
class CdmSourceRow:
    """
    OMOP CdmSource table row.
    https://ohdsi.github.io/CommonDataModel/cdm54.html#cdm_source
    """

    cdm_source_name: str
    cdm_source_abbreviation: str
    cdm_holder: str
    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version_concept_id: int
    vocabulary_version: str
    source_description: str | None = None
    source_documentation_reference: str | None = None
    cdm_etl_reference: str | None = None
    cdm_version: str | None = None

    def validate(self) -> None:
        """Validate required fields based on type hints. Raises RowValidationError if invalid."""
        validate_required_fields(self)


@dataclass(frozen=True, slots=True)
class VisitOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
    """

    # so all classes with visit dates goes here?
    # yeah, basically all out-patient visits as well (labs, tele, etc): map to hospital visit id
    # visits are tracked in vituma, but only for tumor assessments?
    # what about questionnaires: they should be covered, but are not
    #   -- they have a different visit ID e.g. V03_E
    #       -- yeha cause they're really not visits...
    #   -- but can't I just decide what Patient classes have dates that maps to visits instead?
    #   -- one point of this was to abstract away from EHR (but i don't track visit ID in questionnaires so don't use that)
    # todo: look at patient model and decide
    # todo: figure out if VI contains all visits or we need more data
    # -- think VI is sufficient, visit name can be EventId/ActivityId?
    #    since many different assessments can be done on a visit...

    # ok so basically this just tracks when a patient had a visit
    # need static mapping of visit concept ids
    # req:
    # visit occerrence id (autogen id)
    # person id,
    # visit concept id (always the same? map to hospital concept)
    # visit start date,
    # visit end date (date of EHR extraction if no end), visit type concept id (provenance, just map to EHR in static:
    # https://athena.ohdsi.org/search-terms/terms?domain=Type+Concept&standardConcept=Standard&page=1&pageSize=15&query=)

    # so i don't model visits but, can just take questionnaires + any tumor assessments?
    # or maybe worth it to make a visit class?
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
