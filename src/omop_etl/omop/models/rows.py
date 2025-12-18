from dataclasses import dataclass
import datetime as dt

# todo: maybe just use pydantic instead? would be nice with field validators for length etc
#   - yes


@dataclass(frozen=True, slots=True)
class PersonRow:
    person_id: int
    gender_concept_id: int
    year_of_birth: int
    month_of_birth: None | int
    day_of_birth: None | int
    birth_datetime: None | dt.datetime
    race_concept_id: int
    ethnicity_concept_id: int
    person_source_value: str
    gender_source_value: None | str
    gender_source_concept_id: int
    race_source_value: None | str
    race_source_concept_id: int
    ethnicity_source_value: None | str
    ethnicity_source_concept_id: int


@dataclass(frozen=True, slots=True)
class ObservationPeriodRow:
    observation_period_id: int
    person_id: int
    observation_start_date: dt.date
    observation_end_date: dt.date
    period_type_concept_id: int


# todo: make singleton
@dataclass(frozen=True, slots=True)
class CdmSourceRow:
    cdm_source_name: str
    cdm_source_abbreviation: str
    cdm_holder: str
    source_description: None | str
    source_documentation_reference: None | str
    cdm_etl_reference: None | str
    source_release_date: dt.date
    cdm_release_date: dt.date
    cdm_version: None | str
    cdm_version_concept_id: int  # max len 10
    vocabulary_version: str  # max len 20
