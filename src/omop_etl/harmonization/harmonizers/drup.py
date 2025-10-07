import polars as pl
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer
from omop_etl.harmonization.datamodels import HarmonizedData, Patient


class DrupHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        self._process_patient_id()
        self._process_cohort_name()

        # flatten patient values
        patients = list(self.patient_data.values())

        output = HarmonizedData(patients=patients, trial_id=self.trial_id)

        print(f"Drup output: {output}\n\n")

        return output

        # medical_histories=self.medical_histories,
        # ecog_assessments=self.ecog_assessments,
        # previous_treatments=self.previous_treatments,
        # adverse_events=self.adverse_events,
        # response_assessments=self.response_assessments,
        # clinical_benefits=self.clinical_benefits,
        # quality_of_life_assessments=self.quality_of_life_assessments,

    def _process_patient_id(self):
        patient_ids = self.data.select("ID").unique().to_series().to_list()

        # create initial patient object
        for patient_id in patient_ids:
            self.patient_data[patient_id] = Patient(trial_id=self.trial_id, patient_id=patient_id)

    def _process_cohort_name(self):
        """Process cohort names and update patient objects"""
        cohort_data = self.data.filter(pl.col("ID") != "NA")

        for row in cohort_data.iter_rows(named=True):
            patient_id = row["ID"]
            cohort_name = row["CohortName"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].cohort_name = cohort_name
