from abc import abstractmethod, ABC
import pandas as pd
import polars as pl
from pathlib import Path
from typing import List
from pydantic.dataclasses import dataclass

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

# TODO:
#   [ ] Implement basic example to harmonize cohort name (ignore upstream I/O, factory etc)
#   [ ] Implement patient ID (need to know trial - best way to do this? just use dependancy injection and worry about that later)
#   [ ] Test these with output (make fixtures) and if that works extend and think about best way to design this


@dataclass
class Patient:
    cohort_name: str
    patient_id: str


@dataclass
class TrailData:
    patients: List[Patient]


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

    def __init__(self, data: pd.DataFrame):
        self.data = data

    @abstractmethod
    def process(self) -> List[Patient]:
        pass

    @abstractmethod
    def _process_cohort_name(self):
        pass

    pass


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame):
        super().__init__(data)

    @abstractmethod
    def process(self) -> List[Patient]:
        pass

    @abstractmethod
    def _process_cohort_name(self):
        pass


class DrupHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame):
        super().__init__(data)

    @abstractmethod
    def process(self) -> Patient:
        cohort_name = self._process_cohort_name()
        patient_id = "test"
        return Patient(cohort_name=cohort_name, patient_id=patient_id)

    @abstractmethod
    def _process_cohort_name(self):
        pass


def process_impress(file: Path):
    data = impress_data(file)
    harmonizer = ImpressHarmonizer(data)
    return harmonizer.process()


def process_drup(file: Path):
    data = drup_data(file)
    harmonizer = DrupHarmonizer(data)
    return harmonizer.process()


if __name__ == "__main__":
    drup_file = Path(__file__).parents[3] / ".data" / "drup_dummy_data.txt"
    impress_file = Path(__file__).parents[3] / ".data" / "mockdata_impress_2025-02-18.csv"
    process_drup(drup_file)
    process_impress(impress_file)

    print("impress out:")
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
