from pydantic import BaseModel
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, TypeVar

# have some main entry point to take X extracted ecrf files and standardize, then harmonize by instantiating
# dataclasses mirroring variable list struct and have meta for which trials are where, so we can just call this on all data
# and automate processing

class HarmonizeData():
    # based on meta or config process any file from ecrf extraction,
    # some logic to conver stuff etc
    # and ends by instantiating nested pydantic class
    # which can then be passed
    pass


@dataclass
class Meta:
    trial_name: str
    source_ecrf_file: Path
    number_of_patients: int
    # etc
    pass

class AdverseEvents(BaseModel):
    # look at variable sheet and
    pass

class HarmonizedVariables(BaseModel):
    AEs: AdverseEvents
    # etc
    pass

# TODO Need to extract from eCRF before this, 
# as each abstract var contains many vars and I don't know their types 
# and level of nesting etc



@dataclass 
class MedicalHistory: 
    # for instance what will this contain? 
    pass 


# almost all of these are nested and need separate dataclasses 
# so we can just nest adverse events to one field per patient
# and probably need further nesting in each subclass as well 
# as this is per patient 
@dataclass 
class PrimeRoseVariables:
    cohort_name: str 
    trial: str 
    refined_trial_id: str 
    tumor_type: str 
    study_drug_1: str
    study_drug_2: str 
    biomarker: str 
    age: int 
    sex: str 
    ecog_who_performance_status_basline: str #? 
    medical_history: MedicalHistory #? 
    previous_treatment_lines: str #?
    date_of_death: Optional["date_type_here"] = "NaN type here"
    date_lost_to_follow_up: Optional["date_type_here"] = "NaN type here"
    evaluability: bool # actually yes no but idk 
    date_of_treatment_start: "date_type_here"
    date_of_treatment_cycle_start: "date_type_here"
    date_of_treatment_cycle_end: "date_type_here"
    date_of_last_treatment_cycle_start: "date_type_here"
    date_of_end_of_treatment: Optional["date_type_here"] = "NaN type here"
    dose_delivered_milligrams: int # nested, should make drug class 
    all_concomitant_medication: str # composition here 
    adverse_event: bool 
    adverse_event_grade: int 
    adverse_event_ctcae_term: str 
    date_of_adverse_event_start: str
    date_of_adverse_event_end: str 
    adverse_event_outcome: str 
    adverse_event_management: str 
    total_number_of_adverse_events: int 
    serious_adverse_event: bool 
    adverse_event_related_to_treatment: bool # or yes no unknown 
    adverse_event_expectedness: str # nested, susar and what drug etc 
    tumor_type_assessment: str 
    date_of_assessment: str 
    baseline_evaluation: str # actually many vars again so need separate struct 
    change_from_baseline: str # idk what we landed on but probably absolute measruments etc 
    response_assessment: str 
    patient_date_of_end_of_treatment: str #so patient decided to end? bool + date type 
    date_of_end_of_treatment_reasons: str 
    best_overall_response: str 
    clinical_benfit_week16: str 
    quality_of_life_assessment: str #did we decide to just drop this? 
    