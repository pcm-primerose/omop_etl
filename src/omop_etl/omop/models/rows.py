from functools import partial
import datetime as dt
from typing import Annotated, TypeAlias
from pydantic.dataclasses import dataclass as pd_dataclass
from pydantic import BeforeValidator, Field as pd_field


def _truncate(value: object, *, max_length: int) -> object:
    """Truncate a too-long string instead of raising, so builders never need to remember to."""
    if isinstance(value, str):
        return value[:max_length]
    return value


Str2: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=2)), pd_field(max_length=2)]
Str9: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=9)), pd_field(max_length=9)]
Str10: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=10)), pd_field(max_length=10)]
Str20: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=20)), pd_field(max_length=20)]
Str25: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=25)), pd_field(max_length=25)]
Str50: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=50)), pd_field(max_length=50)]
Str60: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=60)), pd_field(max_length=60)]
Str80: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=80)), pd_field(max_length=80)]
Str255: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=255)), pd_field(max_length=255)]
Str10000: TypeAlias = Annotated[str, BeforeValidator(partial(_truncate, max_length=10000)), pd_field(max_length=10000)]


@pd_dataclass(frozen=True, slots=True)
class PersonRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#person
    """

    person_id: int
    gender_concept_id: int
    year_of_birth: int
    race_concept_id: int
    ethnicity_concept_id: int
    person_source_value: Str50 | None = None
    ethnicity_source_concept_id: int | None = None
    race_source_concept_id: int | None = None
    gender_source_concept_id: int | None = None
    gender_source_value: Str50 | None = None
    day_of_birth: int | None = None
    month_of_birth: int | None = None
    birth_datetime: dt.datetime | None = None
    race_source_value: Str50 | None = None
    ethnicity_source_value: Str50 | None = None
    provider_id: int | None = None
    location_id: int | None = None
    care_site_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ObservationPeriodRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#observation_period
    """

    observation_period_id: int
    person_id: int
    observation_period_start_date: dt.date
    observation_period_end_date: dt.date
    period_type_concept_id: int


@pd_dataclass(frozen=True, slots=True)
class CdmSourceRow:
    """
    OMOP CdmSource table row.
    https://ohdsi.github.io/CommonDataModel/cdm55.html#cdm_source
    """

    cdm_source_name: Str255
    cdm_source_abbreviation: Str25
    cdm_holder: Str255
    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version_concept_id: int
    vocabulary_version: Str20
    source_description: Str10000 | None = None
    source_documentation_reference: Str255 | None = None
    cdm_etl_reference: Str255 | None = None
    cdm_version: Str10 | None = None
    cdm_release_identifier: Str255 | None = None


@pd_dataclass(frozen=True, slots=True)
class VisitOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#visit_occurrence
    """

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
    visit_source_value: Str50 | None = None
    visit_source_concept_id: int | None = None
    admitted_from_concept_id: int | None = None
    admitted_from_source_value: Str50 | None = None
    discharged_to_concept_id: int | None = None
    discharged_to_source_value: Str50 | None = None
    preceding_visit_occurrence_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ConditionOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#condition_occurrence
    """

    condition_occurrence_id: int
    person_id: int
    condition_concept_id: int
    condition_start_date: dt.date
    condition_type_concept_id: int
    condition_start_datetime: dt.datetime | None = None
    condition_end_date: dt.date | None = None
    condition_end_datetime: dt.datetime | None = None
    condition_status_concept_id: int | None = None
    stop_reason: Str20 | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    condition_source_value: Str50 | None = None
    condition_source_concept_id: int | None = None
    condition_status_source_value: Str50 | None = None


@pd_dataclass(frozen=True, slots=True)
class DrugExposureRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#drug_exposure
    """

    drug_exposure_id: int
    person_id: int
    drug_concept_id: int
    drug_exposure_start_date: dt.date
    drug_exposure_end_date: dt.date
    drug_type_concept_id: int
    drug_exposure_start_datetime: dt.datetime | None = None
    drug_exposure_end_datetime: dt.datetime | None = None
    verbatim_end_date: dt.date | None = None
    stop_reason: Str20 | None = None
    refills: int | None = None
    quantity: float | None = None
    days_supply: int | None = None
    sig: Str10000 | None = None
    route_concept_id: int | None = None
    lot_number: Str50 | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    drug_source_value: Str50 | None = None
    drug_source_concept_id: int | None = None
    route_source_value: Str50 | None = None
    dose_unit_source_value: Str50 | None = None


@pd_dataclass(frozen=True, slots=True)
class ProcedureOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#procedure_occurrence
    """

    procedure_occurrence_id: int
    person_id: int
    procedure_concept_id: int
    procedure_date: dt.date
    procedure_type_concept_id: int
    procedure_datetime: dt.datetime | None = None
    procedure_end_date: dt.date | None = None
    procedure_end_datetime: dt.datetime | None = None
    modifier_concept_id: int | None = None
    quantity: int | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    procedure_source_value: Str50 | None = None
    procedure_source_concept_id: int | None = None
    modifier_source_value: Str50 | None = None


@pd_dataclass(frozen=True, slots=True)
class MeasurementRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#measurement
    """

    measurement_id: int
    person_id: int
    measurement_concept_id: int
    measurement_date: dt.date
    measurement_type_concept_id: int
    measurement_datetime: dt.datetime | None = None
    measurement_time: Str10 | None = None  # deprecated in next cdm version
    operator_concept_id: int | None = None
    value_as_number: float | None = None
    value_as_concept_id: int | None = None
    unit_concept_id: int | None = None
    range_low: float | None = None
    range_high: float | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    measurement_source_value: Str50 | None = None
    measurement_source_concept_id: int | None = None
    unit_source_value: Str50 | None = None
    unit_source_concept_id: int | None = None
    value_source_value: Str50 | None = None
    measurement_event_id: int | None = None
    meas_event_field_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ObservationRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#observation
    """

    observation_id: int
    person_id: int
    observation_concept_id: int
    observation_date: dt.date
    observation_type_concept_id: int
    observation_datetime: dt.datetime | None = None
    value_as_number: float | None = None
    value_as_string: Str60 | None = None
    value_as_concept_id: int | None = None
    value_as_date: dt.date | None = None
    qualifier_concept_id: int | None = None
    unit_concept_id: int | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    observation_source_value: Str50 | None = None
    observation_source_concept_id: int | None = None
    unit_source_value: Str50 | None = None
    unit_source_concept_id: int | None = None
    qualifier_source_value: Str50 | None = None
    value_source_value: Str50 | None = None
    value_as_source_concept_id: int | None = None
    observation_event_id: int | None = None
    obs_event_field_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class DeathRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#death
    """

    person_id: int
    death_date: dt.date
    death_datetime: dt.datetime | None = None
    death_type_concept_id: int | None = None
    cause_concept_id: int | None = None
    cause_source_value: Str50 | None = None
    cause_source_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class EpisodeRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#episode
    """

    episode_id: int
    person_id: int
    episode_concept_id: int
    episode_start_date: dt.date
    episode_object_concept_id: int
    episode_type_concept_id: int
    episode_start_datetime: dt.datetime | None = None
    episode_end_date: dt.date | None = None
    episode_end_datetime: dt.datetime | None = None
    episode_parent_id: int | None = None
    episode_number: int | None = None
    episode_source_value: Str50 | None = None
    episode_source_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class EpisodeEventRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#episode_event
    """

    episode_id: int
    event_id: int
    episode_event_field_concept_id: int


@pd_dataclass(frozen=True, slots=True)
class CohortRow:
    """
    OMOP RESULTS-schema cohort table row: one trial-arm membership per patient.
    https://ohdsi.github.io/CommonDataModel/cdm55.html#cohort
    """

    cohort_definition_id: int
    subject_id: int
    cohort_start_date: dt.date
    cohort_end_date: dt.date


@pd_dataclass(frozen=True, slots=True)
class CohortDefinitionRow:
    """
    OMOP RESULTS-schema cohort_definition table row: one per distinct trial arm.
    https://ohdsi.github.io/CommonDataModel/cdm55.html#cohort_definition
    """

    cohort_definition_id: int
    cohort_definition_name: Str255
    definition_type_concept_id: int
    subject_concept_id: int
    cohort_definition_description: Str10000 | None = None
    cohort_definition_syntax: Str10000 | None = None
    cohort_initiation_date: dt.date | None = None


@pd_dataclass(frozen=True, slots=True)
class LocationRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#location
    """

    location_id: int
    address_1: Str50 | None = None
    address_2: Str50 | None = None
    city: Str50 | None = None
    state: Str2 | None = None
    zip: Str9 | None = None
    county: Str20 | None = None
    location_source_value: Str50 | None = None
    country_concept_id: int | None = None
    country_source_value: Str80 | None = None
    latitude: float | None = None
    longitude: float | None = None


@pd_dataclass(frozen=True, slots=True)
class ConditionEraRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#condition_era
    """

    condition_era_id: int
    person_id: int
    condition_concept_id: int
    condition_era_start_date: dt.date
    condition_era_end_date: dt.date
    condition_occurrence_count: int | None = None


@pd_dataclass(frozen=True, slots=True)
class DrugEraRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#drug_era
    """

    drug_era_id: int
    person_id: int
    drug_concept_id: int
    drug_era_start_date: dt.date
    drug_era_end_date: dt.date
    drug_exposure_count: int | None = None
    gap_days: int | None = None


@pd_dataclass(frozen=True, slots=True)
class DoseEraRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm55.html#dose_era
    """

    dose_era_id: int
    person_id: int
    drug_concept_id: int
    unit_concept_id: int
    dose_value: float
    dose_era_start_date: dt.date
    dose_era_end_date: dt.date
