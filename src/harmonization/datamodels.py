import logging
from typing import List, Optional, Set
from dataclasses import dataclass, field
import datetime as dt
from src.harmonization.validation.validators import StrictValidators

"""
Flat data
    Patient = Cohort Name, Trial, ID, Age, Sex, Death, Lost to follow-up, Evaluability, 
    Date End of Treatment, Reason EOT, Best Overall Response, Clinical benefit
    
Nested data: 

TumorType = Tumor Type
StudyDrugs = 
Biomarker = 
ECOG/WHO performance status = 
Medical History 
Treatments = 
    Treatment start 
    Treatment start cycle
    Treatment end cycle
    Treatment start last cycle
    Treatment end
    Dose delivered 
Previous treatment lines = 
Concomitant medication
Adverse Event (AE) =
    AE grade
    AE CTCAE Term
    AE start date
    AE end date
    AE outcome
    AE management
    Number of AEs
    SAE
    Related to Treatment 
    Expectedness
    
Tumor Assessment = 
    Type Tumor Assessment
    Event date assessment
    Baseline evaluation
    Change from baseline
Response assessment
Quality of Life assessment

"""


# TODO
"""
Medical History 
Previous treatment lines
Treatment start 
Treatment start cycle
Treatment end cycle
Treatment start last cycle
Treatment end
Dose delivered 
Concomitant medication
Adverse Event (AE)
AE grade
AE CTCAE Term
AE start date
AE end date
AE outcome
AE management
Number of AEs
SAE
Related to Treatment 
Expectedness
Type Tumor Assessment
Event date assessment
Baseline evaluation
Change from baseline
Response assessment
Date End of Treatment
Reason EOT
Best Overall Response
Clinical benefit
Quality of Life assessment
"""

# todo:
#   just do validation here,
#   [ ] finish getters/setters and dunders methods (when parsers, coercers, validators done)
#   [ ] make type hint assertions in setters (yes)


class TumorType:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self._icd10_code: Optional[str] = None
        self._icd10_description: Optional[str] = None
        self._tumor_type: Optional[str] = None
        self._tumor_type_code: Optional[int] = None
        self._cohort_tumor_type: Optional[str] = None
        self._other_tumor_type: Optional[str] = None
        self._updated_fields: Set = field(default_factory=Set)
        self.logger = logger if logger else logging.Logger

    @property
    def icd10_code(self) -> Optional[int]:
        return self._icd10_code

    @icd10_code.setter
    def icd10_code(self, value: int | str | None) -> None:
        self._icd10_code = StrictValidators.validate_optional_str(
            value=value, field_name="icd10_code"
        )
        self._updated_fields.add("icd10_code")

    @property
    def icd10_description(self) -> Optional[str]:
        return self._icd10_description

    @icd10_description.setter
    def icd10_description(self, value: str | None) -> None:
        self._icd10_description = StrictValidators.validate_optional_str(
            value=value, field_name="icd10_description"
        )
        self._updated_fields.add("icd10_description")

    @property
    def tumor_type(self) -> Optional[str]:
        return self._tumor_type

    @tumor_type.setter
    def tumor_type(self, value: str | None) -> None:
        self._tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name="tumor type"
        )
        self._updated_fields.add(value)

    @property
    def tumor_type_code(self) -> Optional[int]:
        return self._tumor_type_code

    @tumor_type_code.setter
    def tumor_type_code(self, value: int | str | None) -> None:
        self._tumor_type_code = StrictValidators.validate_optional_int(
            value=value, field_name="tumor_type_code"
        )
        self._updated_fields.add(value)

    @property
    def cohort_tumor_type(self) -> Optional[str]:
        return self._cohort_tumor_type

    @cohort_tumor_type.setter
    def cohort_tumor_type(self, value: str | None) -> None:
        self._cohort_tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name="cohort_tumor_type"
        )
        self._updated_fields.add(value)

    @property
    def other_tumor_type(self) -> Optional[str]:
        return self._other_tumor_type

    @other_tumor_type.setter
    def other_tumor_type(self, value: str | None) -> None:
        self._other_tumor_type = StrictValidators.validate_optional_str(
            value=value, field_name="other_tumor_type"
        )
        self._updated_fields.add(value)


@dataclass
class StudyDrugs:
    primary_treatment_drug: Optional[str] = None
    primary_treatment_drug_code: Optional[int] = None
    secondary_treatment_drug: Optional[str] = None
    secondary_treatment_drug_code: Optional[int] = None


@dataclass
class Biomarkers:
    gene_and_mutation: Optional[str] = None  # genmut
    gene_and_mutation_code: Optional[int] = None
    cohort_target_name: Optional[str] = None  # cohctn
    cohort_target_mutation: Optional[str] = None  # cohtmn


@dataclass
class FollowUp:
    lost_to_followup: bool
    date_lost_to_followup: Optional[dt.datetime] = None  # date last known alive


@dataclass
class Ecog:
    description: Optional[str] = None
    grade: Optional[int] = None


@dataclass
class MedicalHistory:
    patient_id: str
    treatment_type: str
    treatment_type_code: int
    treatment_specification: str
    treatment_start_date: str
    treatment_end_date: str
    previous_treatment_lines: int


@dataclass
class PreviousTreatmentLine:
    patient_id: str
    treatment: str


@dataclass
class AdverseEvent:
    patient_id: str
    ae_term: str


@dataclass
class ResponseAssessment:
    patient_id: str
    response: str


@dataclass
class ClinicalBenefit:
    patient_id: str
    best_overall_response: str


@dataclass
class QualityOfLife:
    patient_id: str
    eq5d: str
    c30: str


class Patient:
    def __init__(self, patient_id: str, trial_id: str):
        # immutable
        self._patient_id = patient_id
        self._trial_id = trial_id
        # mutable
        self._patient_id: Optional[str] = patient_id
        self._cohort_name: Optional[str] = None
        self._age: Optional[int] = None
        self._sex: Optional[str] = None
        self._tumor_type: Optional[TumorType] = None
        self._study_drugs: Optional[StudyDrugs] = None
        self._biomarker: Optional[Biomarkers] = None
        self._date_of_death: Optional[dt.datetime] = None
        self._lost_to_followup: Optional[FollowUp] = None
        self._evaluable_for_efficacy_analysis: Optional[bool] = None
        self._ecog: Optional[Ecog] = None
        self._updated_fields: set = field(default_factory=set)

    @property
    def patient_id(self) -> str:
        """Patient ID (immutable)"""
        return self._patient_id

    @property
    def trial_id(self) -> str:
        """Trial ID (immutable)"""
        return self._trial_id

    @property
    def cohort_name(self) -> str:
        return self._cohort_name

    @property
    def age(self):
        return self._age

    @age.setter
    def age(self, value: Optional[str | int | None]) -> None:
        """Set age with validation"""
        self._age = StrictValidators.validate_optional_int(
            value=value, field_name="age"
        )
        self._updated_fields.add("age")

    @property
    def sex(self):
        return self._sex

    @sex.setter
    def sex(self, value: Optional[str | None]) -> None:
        """Set sex with validation"""
        self._sex = StrictValidators.validate_optional_str(
            value=value, field_name="sex"
        )
        self._updated_fields.add("sex")

    def get_updated_fields(self) -> Set[str]:
        return self._updated_fields

    # TODO nested repr, to str, to dict, to polars df, etc


@dataclass
class HarmonizedData:
    trial_id: str
    patients: List[Patient] = field(default_factory=list)
    medical_histories: List[MedicalHistory] = field(default_factory=list)
    previous_treatments: List[PreviousTreatmentLine] = field(default_factory=list)
    ecog_assessments: List[Ecog] = field(default_factory=list)
    adverse_events: List[AdverseEvent] = field(default_factory=list)
    clinical_benefits: List[ClinicalBenefit] = field(default_factory=list)
    quality_of_life_assessments: List[QualityOfLife] = field(default_factory=list)

    # add get specific patient data method
    # and get all patient data
    # and specific trial data
    # return as dict instead of object? yes
