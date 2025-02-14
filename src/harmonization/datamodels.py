# import modules
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional


# TODO: Refactor this!
#   and consider using


@dataclass
class Meta:
    trial_name: str
    source_ecrf_file: Path
    number_of_patients: int
    # etc
    pass


@dataclass
class Ecog:
    score: int  # ECOG: ECOGS
    description: str  # ECOG: ECOGSCD

    # TODO only take rows where EventId = V00 (first treatment, only baseline assessment)


@dataclass
class MedicalHistory:
    treatment_type: str  # CT: CTTYPE
    treatment_type_code: int  # CT: CTTYPECD
    treatment_specification: str  # CT: CTTYPESP
    treatment_start_date: str  # CT: CTSTDAT
    treatment_end_date: str  # CT: CTENDAT
    previous_treatment_lines: int  # CT: CTSPID

    # TODO only need: Which treatment lines were undertaken (Yes/No) and dates (YYYY-MM-DD)
    # described as: Treatment lines prior to inclusion in treatment phase, but does that include all these vars?


@dataclass
class PreviousTreatmentLines:
    treatment: str
    end_date: str

    # TODO Which treatment lines were undertaken (Yes/No) and dates (YYYY-MM-DD)
    # combine with MedicalHistory?


@dataclass
class Cycle:
    start: str  # TR: TRC1_DT (date, first day of cycle treatment)
    end: str  # EOT:	either EventDate, EOTDAT or EOTPROGDTC (last day of cycle treatment)
    dose_delivered: int  # TR: TRIVDS1, TRIVU1, TRIVDELYN1, TRDSDEL1


@dataclass
class Treatment:
    start: str  # need to calculate from Cycle.start date, first dose in first cycle
    cycles: List[Cycle]
    last_cycle_start: str  # need to calculate from Cycle.start date (first dat in last cycle)
    last_cycle_end: str  # need to calculate from Cycle.end date (last dat in last cycle)
    concomitant_medication: str  # CM: CMTRT, CMMHYN, CMSTDAT, CMONGO, CMENDAT
    end: str  # date of last dose in the last cycle or timepoint for end of treatment. (YYYY-MM-DD).

    # How is this different (can be several cycles, so first and last cycle day are across all cycles):
    # Treatment start: First dose in first cycle (YYYY-MM-DD)
    # Treatment start cycle: First day of treatment cycle start (YYYY-MM-DD)

    # also, how is this different (it is, there can be several cycles, same as above):
    # Treatment end cycle: Date of treatment cycle end (YYYY-MM-DD)
    # Treatment start last cycle: First day in last treatment cycle (YYYY-MM-DD)


@dataclass
class AdverseEvent:
    ctcae_term: str  # AE: AECTCAET (AE term)
    start_date: str  # AE: AESTDAT
    end_date: str  # AE: AEENDAT
    outcome: str  # AE: AEOUT
    management: str  # CM: CMAEYN, CMAENO - AE treatment/management | discontinuation/dose reductions
    # (TODO make separate attributes)
    serious_event: str  # AE: AESERCD (yes/no - perhaps bool with NaN instead?)
    treatment_related: str  # AE: AETRT1, AEREL1, AETRT2, AEREL2 (yes/no/unknown - perhaps bool with NaN instead?)
    expectedness: str  # SUSAR: speciy which drug - but where is this data??


@dataclass
class AdverseEvents:
    occurance: str  # (yes/no - what file and column?)
    worst_ae_grade: int  # AE: AETOXGRECD (worst grade experienced by patient (grade 3 or higher))
    number_of_aes: int  # total number of AEs experienced for a patient
    # (can just count instances/elements of AdverseEvent class)
    all_events: List[AdverseEvent]


@dataclass
class Patient:
    cohort_name: str  # COH: COHORTNAME (format: Target/Tumor Type/Treatment)
    trial_id: str  # manually set
    patient_id: str  # SubjectId in all files/sheets
    tumor_type: str  # COH: ICD10COD
    study_drug_1: str  # COH: COHALLO1, COHALLO1__2 (INN format)
    study_drug_2: Optional[str]  # COH: COHALLO2, COHALLO2__2 (if applicable)
    biomarker: str  # COH: COHTMN
    age: int  # DM: BRTHDAT (age at treatment start)
    sex: str  # DM: SEX
    ecog: Ecog  # (only at baseline)
    medical_history: MedicalHistory
    previous_treatment_lines: PreviousTreatmentLines
    date_of_death: Optional[int]  # EOS/FU: DEATHDTC/FUPDEDAT
    date_lost_to_followup: Optional[str]  # What file? If applicable.
    evaluable_for_efficacy_analysis: str  # What file? (yes/no)
    treatment_start_first_dose: str  # TR: TRC1_DT
    adverse_events: AdverseEvents
    type_of_tumor_assessment: str  # VI:	VITUMA, VITUMA_2 (Type of tumor assessment (RECIST, iRECIST, LUAGNO, RANO, AML)
    tumor_assessment_date: str  # RA, RNRSP, LUGRSP, EMLRSP: EventDate
    # (file name determines event type, that is, tumor assessment type)
    baseline_evaluation: str  # RA, RNRSP: RARECBAS, TERNTBAS
    change_from_baseline: str  # RA, RNRSP: RARECCUR, RARECNAD, RABASECH, RARECCH, TERNTB, TERNAD, TERNCFB
    # (Sum size of target lesion at visit, % change from baseline, evaluation non-target-lesion, new lesions?) - what to extract?
    response_assessment: str  # RA, RNRSP, LUGRSP, EMLRSP: RATIMRES, RAiMOD, RNRSPCL, LUGOVRL, EMLRESP - need nesting:
    # Response assessment (target lesion, non-target lesion and over-all). 	Response asseessment RECIST.
    end_of_treatment_date: str  # EOT: EOTPROGDTC, EOTDAT, EventDate
    end_of_treatment_reason: str  # EOT:	EOTREOTCD
    best_overall_response: str  # BR: BRRESP
    clinical_benefit: str  # (Clinical benefit - CR, PR or SD after 16 weeks of treatment.) - what file/column?
    quality_of_life_assessment: str  # EQ5D: EventName, EventDate, EQ5D1, EQ5D2, EQ5D3, EQ5D4, EQ5D5 & C30:
    # EventName, EventDate, C30_Q1 - 30
    # is this needed?
    # Health-related Quality of Life (HrQoL)-questionnaires collected in the trial
    # (QLQ-C30, EQ-5D-5L, WISP and Patient Preference).


@dataclass
class Trial:
    trial_name: str
    patients: List[Patient]
    pass
