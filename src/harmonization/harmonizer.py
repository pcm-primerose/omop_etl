import datetime as dt
from abc import ABC, abstractmethod
import polars as pl
from pathlib import Path
from typing import List, Optional, Dict
from dataclasses import field
from pydantic.dataclasses import dataclass
from pydantic import BaseModel

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
# Should also have a method on the top-level that finds unique values for all fields per trial, and writes to a vocabulary file
# to be used for sematic mapping (in the semantic mapping stage).
# Do final patient ID renaming after processing data across all trials (just easier to get total patients first)

# TODO:
#   [ ] Implement basic example to harmonize cohort name (ignore upstream I/O, factory etc)
#   [ ] Implement patient ID (need to know trial - best way to do this? just use dependancy injection and worry about that later)
#   [ ] Test these with output (make fixtures) and if that works extend and think about best way to design this


# So pydantic dataclasses should just mirror the OMOP CDM tables I need to use.
# so that'll make things a lot easier. Then I'll use Polars in harmonization and to process the data
# and keep each dataclass modular by using patient ID as a foreign key.
# The end result can then be composition of all patients. Unsure if I should make more modular, using patient_id as foreign key per dataclass
# but for now just implement using normal composition and if needed later make more modular.

# make separate dataclasses for variables that can be multiple entries per patient? e.g.:


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
    tumor_type: Optional[str] = None
    study_drug_1: Optional[str] = None
    study_drug_2: Optional[str] = None
    biomarker: Optional[str] = None
    date_of_death: Optional[dt.datetime] = None
    date_lost_to_followup: Optional[dt.datetime] = None
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


def drup_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def impress_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


class BaseHarmonizer(ABC):
    """
    Abstract base class that defines the methods needed to produce
    each final output variable. The idea is that each output variable
    is computed by a dedicated method, which may pull data from different
    sheets (i.e. from differently prefixed columns in the combined DataFrame).
    """

    def __init__(self, data: pl.DataFrame, trial_id: str):
        self.data = data
        self.trial_id = trial_id
        self.patient_data: Optional[Dict[str, Patient]] = {}
        self.medical_histories: Optional[List] = []
        self.previous_treatment_lines: List = []
        self.ecog_assessments: List = []
        self.adverse_events: List = []
        self.clinical_benefits: List = []
        self.quality_of_life_assessment: List = []

    @abstractmethod
    def process(self):
        """Processes all data and returns a complete, harmonized structure"""
        pass

    @abstractmethod
    def _process_patient_id(self):
        pass

    @abstractmethod
    def _process_cohort_name(self):
        pass


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        print(self.data)
        self._process_patient_id()
        self._process_cohort_name()

        # flatten patient values
        patients = list(self.patient_data.values())

        output = HarmonizedData(patients=patients, trial_id=self.trial_id)

        print(f"Output: {output}")

        return output

        # medical_histories=self.medical_histories,
        # ecog_assessments=self.ecog_assessments,
        # previous_treatments=self.previous_treatments,
        # adverse_events=self.adverse_events,
        # response_assessments=self.response_assessments,
        # clinical_benefits=self.clinical_benefits,
        # quality_of_life_assessments=self.quality_of_life_assessments,

    def _process_patient_id(self):
        patient_ids = self.data.select("SubjectId").unique().to_series().to_list()

        # create initial patient object
        for patient_id in patient_ids:
            self.patient_data[patient_id] = Patient(trial_id=self.trial_id, patient_id=patient_id)

    def _process_cohort_name(self):
        """Process cohort names and update patient objects"""
        cohort_data = self.data.filter(pl.col("COH_COHORTNAME") != "NA")

        for row in cohort_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            cohort_name = row["COH_COHORTNAME"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].cohort_name = cohort_name


class DrupHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame):
        super().__init__(data)

    def process(self):
        pass

    def _process_patient_id(self):
        pass

    def _process_cohort_name(self):
        cohort_name = self.data.filter(pl.all_horizontal(pl.col("CohortName") != "NA"))
        print(cohort_name)
        pass


def process_impress(file: Path) -> HarmonizedData:
    data = impress_data(file)
    harmonizer = ImpressHarmonizer(data, trial_id="IMPRESS")
    return harmonizer.process()


if __name__ == "__main__":
    drup_file = Path(__file__).parents[2] / ".data" / "drup_dummy_data.txt"
    impress_file = Path(__file__).parents[2] / ".data" / "impress_mockdata_2025-02-18.csv"
    impress = process_impress(impress_file)
