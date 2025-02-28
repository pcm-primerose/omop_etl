import polars as pl
import datetime as dt
from src.harmonization.datamodels import HarmonizedData, Patient
from src.harmonization.harmonizers.base import BaseHarmonizer


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        self._process_patient_id()
        self._process_cohort_name()
        self._process_gender()
        self._process_age()

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
        gender_data = self.data.filter(
            (pl.col("DM_SEX") != "NA") & (pl.col("DM_SEX").is_in(["Female", "Male"])),
        )

        for row in gender_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            sex = row["DM_SEX"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].sex = sex

    def _process_age(self):
        """Process and calculate age and update patient object"""
        birth_date_data = self.data.filter(pl.col("DM_BRTHDAT") != "NA").select("SubjectId", "DM_BRTHDAT").with_columns(pl.col("DM_BRTHDAT").str.to_date())

        treatment_dates = (
            self.data.filter(pl.col("TR_TRC1_DT") != "NA")
            .select("SubjectId", "TR_TRC1_DT")
            .with_columns(pl.col("TR_TRC1_DT").str.to_date())
            .group_by("SubjectId")
            .agg(latest_treatment_date=pl.col("TR_TRC1_DT").max())
        )

        combined_data = birth_date_data.join(treatment_dates, on="SubjectId", how="inner")

        with_age = combined_data.with_columns(age_at_treatment=((pl.col("latest_treatment_date") - pl.col("DM_BRTHDAT")).dt.total_days() / 365.25).round(0).cast(pl.Int8))

        # update patient objects
        for row in with_age.iter_rows(named=True):
            patient_id = row["SubjectId"]
            age = row["age_at_treatment"]
            print(f"Age: {age}")

            if patient_id in self.patient_data:
                self.patient_data[patient_id].age = age


"""
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
"""
