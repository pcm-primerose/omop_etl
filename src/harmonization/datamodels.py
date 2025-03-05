from typing import List, Optional
from dataclasses import field
from pydantic.dataclasses import dataclass
import datetime as dt


@dataclass
class TumorType:
    icd10_code: Optional[str] = None
    icd10_description: Optional[str] = None
    tumor_type: Optional[str] = None
    tumor_type_code: Optional[int] = None
    cohort_tumor_type: Optional[str] = None
    other_tumor_type: Optional[str] = None


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
class Ecog:
    patient_id: str
    ecog: str


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


@dataclass
class Patient:
    patient_id: str
    trial_id: str
    cohort_name: Optional[str] = None
    age: Optional[int] = None
    sex: Optional[str] = None
    tumor_type: Optional[TumorType] = None
    study_drugs: Optional[StudyDrugs] = None
    biomarker: Optional[Biomarkers] = None
    date_of_death: Optional[dt.datetime] = None
    lost_to_followup: Optional[FollowUp] = None
    evaluable_for_efficacy_analysis: Optional[bool] = None
    treatment_start_first_dose: Optional[dt.datetime] = None
    type_of_tumor_assessment: Optional[str] = None
    tumor_assessment_date: Optional[dt.datetime] = None
    baseline_evaluation: Optional[str] = None
    change_from_baseline: Optional[int] = None
    end_of_treatment_date: Optional[dt.datetime] = None
    end_of_treatment_reason: Optional[str] = None
    best_overall_response: Optional[str] = None


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
