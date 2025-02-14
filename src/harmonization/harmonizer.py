from abc import abstractmethod, ABC
import pandas as pd


# Each abstract method harmonized towards one PRIME-ROSE variable, and return instances of that model?
# need to update the data models in any case, implement IMPRESS first if we can export the anonymized data,
# if not start with DRUP even though it's missing some QoL stuff.
# Main file for harmonizing in this package.
# Each subclass reads in respective data from each trial, instantiate input file
# need input class, we can easily infer trial name from data nad call the correct subclass from main
# ending in validated dataclasses contianing proper strucutre (datamodels, immutable dataclasses with field validation).
# These will then be passed as structs to the next part of the pipeline, doing sematic mapping and then structural mapping.
# probably easiest to just keep using pandas or polars since we have tabular data and we probably get most/all data as csv files
# or I could extract from csv to struct but that seems kind of unneccesary.
# after mapping, we can use SQL Alchemy (but avoid too much state) and raw SQL to do inner logic,
# and populate the empty OMOP CDM. We then need to link each struct to the respective table in the database.
# if I can export the pseduo-anonymized data I can test all of this locally, just use the Athena data and make a CDM
# from that to do mapping (or keep as files), then query that with AutOMOP, make mapping files, log unmapped vars,
# and contruct emopty DB with same structure as CDM, use structural mapping with linkage and logic from struct to tables
# and instantiate, something like that.


class BaseHarmonizer(ABC):
    """
    Abstract base class that defines the methods needed to produce
    each final output variable. The idea is that each output variable
    is computed by a dedicated method, which may pull data from different
    sheets (i.e. from differently prefixed columns in the combined DataFrame).
    """

    def __init__(self, combined_df: pd.DataFrame):
        self.combined_df = combined_df

    def process(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def _process_cohort_name(self):
        pass

    pass


class ImpressHarmonizer(BaseHarmonizer):
    @abstractmethod
    def _process_cohort_name(self):
        pass


class DrupHarmonizer(BaseHarmonizer):
    @abstractmethod
    def _process_cohort_name(self):
        pass


# TODO move this to separate config later or store in a struct, should be the same for all trials

drup_ecrf_output = {
    "patient ID",
    "CohortName",
    "Trial",
    "ID",
    "TumourType",
    "TumourTypeOther",
    "ICD10Code",
    "ICD10Description",
    "TumourType2",
    "StudyTreatment",
    "Biomarker",
    "Biomarker_mutation",
    "BiomarkerTargets",
    "BiomarkerCategory",
    "BiomarkerOther",
    "Age",
    "Sex",
    "WHO",
    "PT_start_date",
    "PT_end_date",
    "PT_chemotherapy_YN",
    "PT_chemo_end_date",
    "PT_radiotherapy_YN",
    "PT_radiotherapy_end_date",
    "PT_immunotherapy_YN",
    "PT_immunotherapy_end_date",
    "PT_hormonaltherapy_YN",
    "PT_hormonaltherapy_end_date",
    "PT_targetedtherapy_YN",
    "PT_targetedtherapy_end_date",
    "Death",
    "Lost_to_follow_up",
    "Days_treated",
    "Evaluability",
    "Treatment_type",
    "Treatment_start",
    "Number_of_cyles",
    "Treatment_end",
    "Treatment_end_last_dose",
    "Dose_delivered",
    "Concominant_start_date",
    "Concominant_end_date",
    "Concominant_medication",
    "Concominant_indication",
    "Concominant_ongoing",
    "AE_event",
    "AE_grade",
    "CTCAE_term",
    "AE_start_date",
    "AE_end_date",
    "AE_total",
    "SAE",
    "AE_related_to_treatment1",
    "AE_related_to_treatment2",
    "Type_tumour_assessment",
    "Baseline_evaluation",
    "Event_date_assessment",
    "Change_from_baseline",
    "Change_from_minimum",
    "Response_assessment",
    "Best_overall_response_BOR",
    "Best_overall_response_Ra",
    "Clinical_benefit",
}
primerose_output_schema = {
    "Cohort Name",
    "Trial",
    "Patient ID",
    "Study Drug 1",
    "Study Drug 2",
    "Biomarker/Target",
    "Age",
    "Sex",
    "ECOG/WHO performance status",
    "Medical History",
    "Previous treatment lines",
    "Death",
    "Lost to follow-up",
    "Evaluability",
    "Treatment start",
    "Treatment start cycle",
    "Treatment end cycle",
    "Treatment start last cycle",
    "Treatment end",
    "Dose delivered",
    "Concomitant medication",
    "Adverse Event (AE)",
    "AE grade",
    "AE CTCAE Term",
    "AE start date",
    "AE end date",
    "AE outcome",
    "AE management",
    "Number of AEs",
    "SAE",
    "Related to Treatment",
    "Expectedness",
    "Type Tumor Assessment",
    "Event date assessment",
    "Baseline evaluation",
    "Change from baseline",
    "Response assessment",
    "Date End of Treatment",
    "Reason EOT",
    "Best Overall Response",
    "Clinical benefit",
    "Quality of Life assessment",
}
