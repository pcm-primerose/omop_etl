import datetime

import polars as pl
import datetime as dt
from src.harmonization.datamodels import HarmonizedData, Patient
from src.harmonization.harmonizers.base import BaseHarmonizer
from src.utils.helpers import date_parser_helper


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        self._process_patient_id()
        self._process_cohort_name()
        self._process_gender()
        self._process_age()
        self._process_tumor_type()

        # flatten patient values
        patients = list(self.patient_data.values())

        output = HarmonizedData(patients=patients, trial_id=self.trial_id)

        print(f"Impress output: {output}")

        return output
        # medical_histories=self.medical_histories,
        # ecog_assessments=self.ecog_assessments,
        # previous_treatments=self.previous_treatments,
        # adverse_events=self.adverse_events,
        # response_assessments=self.response_assessments,
        # clinical_benefits=self.clinical_benefits,
        # quality_of_life_assessments=self.quality_of_life_assessments,

    def _process_patient_id(self):
        """Process patient ID and create patient object"""
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

    def _process_gender(self):
        """Process gender and update patient object"""
        gender_data = self.data.with_columns(pl.col("DM_SEX").str.to_lowercase()).filter(pl.col("DM_SEX").is_in(["female", "male"]))

        for row in gender_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            sex = row["DM_SEX"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].sex = sex

    def _process_age(self):
        """Process and calculate age and update patient object"""
        birth_date_data = (
            self.data.filter(pl.col("DM_BRTHDAT") != "NA")
            .select("SubjectId", "DM_BRTHDAT")
            .with_columns(parsed_date=date_parser_helper("DM_BRTHDAT"))
            .with_columns(birth_date=pl.col("parsed_date").str.to_date())
            .select("SubjectId", "birth_date")
        )

        treatment_dates = (
            self.data.filter(pl.col("TR_TRC1_DT") != "NA")
            .select("SubjectId", "TR_TRC1_DT")
            .with_columns(parsed_date=date_parser_helper("TR_TRC1_DT"))
            .with_columns(parsed_treatment_date=pl.col("parsed_date").str.to_date())
            .group_by("SubjectId")
            .agg(latest_treatment_date=pl.col("parsed_treatment_date").max())
        )

        combined_data = birth_date_data.join(treatment_dates, on="SubjectId", how="inner")

        with_age = combined_data.with_columns(age_at_treatment=((pl.col("latest_treatment_date") - pl.col("birth_date")).dt.total_days() / 365.25).round(0).cast(pl.Int8))

        # update patient objects
        for row in with_age.iter_rows(named=True):
            patient_id = row["SubjectId"]
            age = row["age_at_treatment"]
            print(f"Age: {age}")

            if patient_id in self.patient_data:
                self.patient_data[patient_id].age = age

    def _process_tumor_type(self):
        tumor_data = self.data.with_columns(
            pl.cols("COH_ICD10DES", "COH_ICD10DES", "COH_COHTTYPE, COH_COHTTYPECD", "COH_COHTTYPE__2", "COH_COHTTYPE__2CD", "COH_COHTT", "COH_COHTTOSP")
        )

        print(tumor_data)
        pass

    "COH_ICD10COD"  # tumor type ICD10 code
    "COH_ICD10DES"  # tumor type ICD10 description

    # mutually exlusive (either COHTTYPE and COHTTYPECD or COHTTYPE__2 and COHTTYPE__2CD):
    "COH_COHTTYPE"  # tumor type
    "COH_COHTTYPECD"  # tumor type code
    "COH_COHTTYPE__2"  # tumor type 2
    "COH_COHTTYPE__2CD"  # tumor type 2 code
    # So these will be one description and one code --> tumor_type / tumor_type_code

    "COH_COHTT"  # cohort tumor type --> cohort_tumor_type
    "COH_COHTTOSP"  # other tumor type --> other_tumor_type


"""
ICD10 codes, ICD10 description, pull-down menu tumor types, cohort tumor type, free text description. All given as text
"""


"""
@dataclass
class Patient:
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
"""
