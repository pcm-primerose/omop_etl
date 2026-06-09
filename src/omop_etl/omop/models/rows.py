from pydantic.dataclasses import dataclass as pd_dataclass
from pydantic import Field as pd_field
import datetime as dt
from typing import Annotated


@pd_dataclass(frozen=True, slots=True)
class PersonRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#person
    """

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
    provider_id: int | None = None
    location_id: int | None = None
    care_site_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ObservationPeriodRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#observation_period
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
    https://ohdsi.github.io/CommonDataModel/cdm54.html#cdm_source
    """

    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version_concept_id: int
    cdm_source_name: Annotated[str, pd_field(max_length=255)]
    cdm_source_abbreviation: Annotated[str, pd_field(max_length=25)]
    cdm_holder: Annotated[str, pd_field(max_length=255)]
    vocabulary_version: Annotated[str, pd_field(max_length=20)]
    source_description: Annotated[str | None, pd_field(max_length=2147483647)] = None
    source_documentation_reference: Annotated[str | None, pd_field(max_length=255)] = None
    cdm_etl_reference: Annotated[str | None, pd_field(max_length=255)] = None
    cdm_version: Annotated[str | None, pd_field(max_length=10)] = None


@pd_dataclass(frozen=True, slots=True)
class VisitOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#visit_occurrence
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
    visit_source_value: str | None = None
    visit_source_concept_id: int | None = None
    admitted_from_concept_id: int | None = None
    admitted_from_source_value: str | None = None
    discharged_to_concept_id: int | None = None
    discharged_to_source_value: str | None = None
    preceding_visit_occurrence_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ConditionOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#condition_occurrence
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
    stop_reason: Annotated[str | None, pd_field(max_length=20)] = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    condition_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    condition_source_concept_id: int | None = None
    condition_status_source_value: Annotated[str | None, pd_field(max_length=50)] = None


@pd_dataclass(frozen=True, slots=True)
class DrugExposureRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#drug_exposure
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
    stop_reason: Annotated[str | None, pd_field(max_length=20)] = None
    refills: int | None = None
    quantity: float | None = None
    days_supply: int | None = None
    sig: Annotated[str | None, pd_field(max_length=10000)] = None
    route_concept_id: int | None = None
    lot_number: Annotated[str | None, pd_field(max_length=50)] = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    drug_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    drug_source_concept_id: int | None = None
    route_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    dose_unit_source_value: Annotated[str | None, pd_field(max_length=50)] = None


@pd_dataclass(frozen=True, slots=True)
class ProcedureOccurrenceRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#procedure_occurrence
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
    procedure_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    procedure_source_concept_id: int | None = None
    modifier_source_value: Annotated[str | None, pd_field(max_length=50)] = None


@pd_dataclass(frozen=True, slots=True)
class MeasurementRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement
    """

    measurement_id: int
    person_id: int
    measurement_concept_id: int
    measurement_date: dt.date
    measurement_type_concept_id: int
    measurement_datetime: dt.datetime | None = None
    measurement_time: Annotated[str | None, pd_field(max_length=10)] = None  # deprecated in next cdm version
    operator_concept_id: int | None = None
    value_as_number: float | None = None
    value_as_concept_id: int | None = None
    unit_concept_id: int | None = None
    range_low: float | None = None
    range_high: float | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    measurement_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    measurement_source_concept_id: int | None = None
    unit_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    unit_source_concept_id: int | None = None
    value_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    measurement_event_id: int | None = None
    meas_event_field_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class ObservationRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#observation
    """

    observation_id: int
    person_id: int
    observation_concept_id: int
    observation_date: dt.date
    observation_type_concept_id: int
    observation_datetime: dt.datetime | None = None
    value_as_number: float | None = None
    value_as_string: Annotated[str | None, pd_field(max_length=60)] = None
    value_as_concept_id: int | None = None
    qualifier_concept_id: int | None = None
    unit_concept_id: int | None = None
    provider_id: int | None = None
    visit_occurrence_id: int | None = None
    visit_detail_id: int | None = None
    observation_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    observation_source_concept_id: int | None = None
    unit_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    qualifier_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    value_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    observation_event_id: int | None = None
    obs_event_field_concept_id: int | None = None


@pd_dataclass(frozen=True, slots=True)
class DeathRow:
    """
    https://ohdsi.github.io/CommonDataModel/cdm54.html#death
    """

    person_id: int
    death_date: dt.date
    death_datetime: dt.datetime | None = None
    death_type_concept_id: int | None = None
    cause_concept_id: int | None = None
    cause_source_value: Annotated[str | None, pd_field(max_length=50)] = None
    cause_source_concept_id: int | None = None
