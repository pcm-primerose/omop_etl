from dataclasses import dataclass
import datetime as dt


@dataclass(slots=True)
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
