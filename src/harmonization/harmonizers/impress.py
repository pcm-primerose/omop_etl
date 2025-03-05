import polars as pl
import datetime as dt
from typing import Optional
from src.harmonization.harmonizers.base import BaseHarmonizer
from src.harmonization.datamodels import (
    HarmonizedData,
    Patient,
    TumorType,
    StudyDrugs,
    Biomarkers,
    FollowUp,
)
from src.utils.helpers import (
    date_parser_helper,
    safe_get,
    safe_int,
    parse_date,
    parse_flexible_date,
)


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        self._process_patient_id()
        self._process_cohort_name()
        self._process_gender()
        self._process_age()
        self._process_tumor_type()
        self._process_study_drugs()
        self._process_biomarkers()
        self._process_date_of_death()
        self._process_date_lost_to_followup()

        # flatten patient values
        patients = list(self.patient_data.values())

        for idx in range(len(patients)):
            print(f"Patient {idx}: {patients[idx]} \n")

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
            self.patient_data[patient_id] = Patient(
                trial_id=self.trial_id, patient_id=patient_id
            )

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
        gender_data = self.data.with_columns(
            pl.col("DM_SEX").str.to_lowercase()
        ).filter(pl.col("DM_SEX").is_in(["female", "male"]))

        for row in gender_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            sex = row["DM_SEX"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].sex = sex

    def _process_age(self):
        """Process and calculate age and update patient object"""
        birth_dates = self.data.filter(pl.col("DM_BRTHDAT") != "NA").select(
            "SubjectId", "DM_BRTHDAT"
        )

        treatment_dates = self.data.filter(pl.col("TR_TRC1_DT") != "NA").select(
            "SubjectId", "TR_TRC1_DT"
        )

        # update patient age
        for patient_id in self.patient_data:
            birth_rows = birth_dates.filter(pl.col("SubjectId") == patient_id)
            if birth_rows.height == 0:
                continue

            birth_row = birth_rows.row(0, named=True)
            birth_date = parse_flexible_date(birth_row["DM_BRTHDAT"])
            if birth_date is None:
                continue

            treatment_rows = treatment_dates.filter(pl.col("SubjectId") == patient_id)
            if treatment_rows.height == 0:
                continue

            latest_treatment_date = None
            for row in treatment_rows.iter_rows(named=True):
                treatment_date = parse_flexible_date(row["TR_TRC1_DT"])
                if treatment_date is not None:
                    if (
                        latest_treatment_date is None
                        or treatment_date > latest_treatment_date
                    ):
                        latest_treatment_date = treatment_date

            if latest_treatment_date is None:
                continue

            age_delta = latest_treatment_date - birth_date
            age_years = int(age_delta.days / 365.25)

            self.patient_data[patient_id].age = age_years

    def _process_tumor_type(self):
        tumor_data = (
            self.data.select(
                (
                    "SubjectId",
                    "COH_ICD10COD",
                    "COH_ICD10DES",
                    "COH_COHTTYPE",
                    "COH_COHTTYPECD",
                    "COH_COHTTYPE__2",
                    "COH_COHTTYPE__2CD",
                    "COH_COHTT",
                    "COH_COHTTOSP",
                )
            )
        ).filter(
            pl.col("COH_ICD10COD") != "NA"
        )  # Note: Assume tumor type data is always on the same row and we don't have multiple,
        # if this is not always the case, can use List[TumorType] in Patient class with default factory and store multiple

        # update patient objects
        for row in tumor_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            icd10_code = row["COH_ICD10COD"] if row["COH_ICD10COD"] != "NA" else None
            icd10_description = (
                row["COH_ICD10DES"] if row["COH_ICD10DES"] != "NA" else None
            )
            cohort_tumor_type = row["COH_COHTT"] if row["COH_COHTT"] != "NA" else None
            other_tumor_type = (
                row["COH_COHTTOSP"] if row["COH_COHTTOSP"] != "NA" else None
            )

            # get main tumor type (mutally exclusive)
            tumor_type = None
            tumor_type_code = None

            if row["COH_COHTTYPE"] != "NA" and row["COH_COHTTYPECD"] != "NA":
                tumor_type = row["COH_COHTTYPE"]
                tumor_type_code = int(row["COH_COHTTYPECD"])
            elif row["COH_COHTTYPE__2"] != "NA" and row["COH_COHTTYPE__2CD"] != "NA":
                tumor_type = row["COH_COHTTYPE__2"]
                tumor_type_code = int(row["COH_COHTTYPE__2CD"])

            if patient_id in self.patient_data:
                self.patient_data[patient_id].tumor_type = TumorType(
                    icd10_code=icd10_code,
                    icd10_description=icd10_description,
                    tumor_type=tumor_type,
                    tumor_type_code=tumor_type_code,
                    cohort_tumor_type=cohort_tumor_type,
                    other_tumor_type=other_tumor_type,
                )

    def _process_study_drugs(self):
        drug_data = self.data.select(
            "SubjectId",
            "COH_COHALLO1",
            "COH_COHALLO1CD",
            "COH_COHALLO1__2",
            "COH_COHALLO1__2CD",
            "COH_COHALLO2",
            "COH_COHALLO2CD",
            "COH_COHALLO2__2",
            "COH_COHALLO2__2CD",
        ).filter(
            (pl.col("COH_COHALLO1") != "NA")
            | (pl.col("COH_COHALLO1__2") != "NA")
            | (pl.col("COH_COHALLO2") != "NA")
            | (pl.col("COH_COHALLO2__2") != "NA")
        )

        # assuming we have all drug data in one row and that 1 / 1__2 & 2 / 2__2 vars are mutally exclusive
        # such that 1 <--> 2 & 1__2 <--> 2__2 and != 1 <--> 2__2 & 1__2 <--> 2
        for row in drug_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            if patient_id not in self.patient_data:
                continue

            # process primary drug
            primary_drug = None
            primary_drug_code = None

            if safe_get(row["COH_COHALLO1"]) is not None:
                primary_drug = row["COH_COHALLO1"]
                primary_drug_code = safe_int(row["COH_COHALLO1CD"])

            elif safe_get(row["COH_COHALLO1__2"]) is not None:
                primary_drug = row["COH_COHALLO1__2"]
                primary_drug_code = safe_int(row["COH_COHALLO1__2CD"])

            # use code if no drugs found
            elif safe_get(row["COH_COHALLO1CD"]) is not None:
                primary_drug_code = safe_int(row["COH_COHALLO1CD"])
            elif safe_get(row["COH_COHALLO1__2CD"]) is not None:
                primary_drug_code = safe_int(row["COH_COHALLO1__2CD"])

            # process seconadry drug
            secondary_drug = None
            secondary_drug_code = None

            if safe_get(row["COH_COHALLO2"]) is not None:
                secondary_drug = row["COH_COHALLO2"]
                secondary_drug_code = safe_int(row["COH_COHALLO2CD"])

            elif safe_get(row["COH_COHALLO2__2"]) is not None:
                secondary_drug = row["COH_COHALLO2__2"]
                secondary_drug_code = safe_int(row["COH_COHALLO2__2CD"])

            # use code if no drugs found
            elif safe_get(row["COH_COHALLO2CD"]) is not None:
                secondary_drug_code = safe_int(row["COH_COHALLO2CD"])
            elif safe_get(row["COH_COHALLO2__2CD"]) is not None:
                secondary_drug_code = safe_int(row["COH_COHALLO2__2CD"])

            self.patient_data[patient_id].study_drugs = StudyDrugs(
                primary_treatment_drug=primary_drug,
                primary_treatment_drug_code=primary_drug_code,
                secondary_treatment_drug=secondary_drug,
                secondary_treatment_drug_code=secondary_drug_code,
            )

    def _process_biomarkers(self):
        biomarker_data = self.data.select(
            "SubjectId", "COH_GENMUT1", "COH_GENMUT1CD", "COH_COHCTN", "COH_COHTMN"
        ).filter(
            (pl.col("COH_GENMUT1") != "NA")
            | (pl.col("COH_GENMUT1CD") != "NA")
            | (pl.col("COH_COHCTN") != "NA")
            | (pl.col("COH_COHTMN") != "NA")
        )

        for row in biomarker_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            gene_and_mutation = row["COH_GENMUT1"]
            gene_and_mutation_code = int(row["COH_GENMUT1CD"])
            cohort_target_name = row["COH_COHCTN"]
            cohort_target_mutation = row["COH_COHTMN"]

            self.patient_data[patient_id].biomarker = Biomarkers(
                gene_and_mutation=gene_and_mutation,
                gene_and_mutation_code=gene_and_mutation_code,
                cohort_target_name=cohort_target_name,
                cohort_target_mutation=cohort_target_mutation,
            )

    def _process_date_of_death(self):
        death_data = self.data.select(
            "SubjectId", "EOS_DEATHDTC", "FU_FUPDEDAT"
        ).filter((pl.col("EOS_DEATHDTC") != "NA") | (pl.col("FU_FUPDEDAT") != "NA"))

        for row in death_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            death_date = None

            eos_date = parse_date(row["EOS_DEATHDTC"])
            fu_date = parse_date(row["FU_FUPDEDAT"])

            # use latest date
            if eos_date and fu_date:
                death_date = max(eos_date, fu_date)
            elif eos_date:
                death_date = eos_date
            elif fu_date:
                death_date = fu_date

            self.patient_data[patient_id].date_of_death = death_date

    def _process_date_lost_to_followup(self):
        # TODO: What do we actually want to store here? Just a bool or date + bool, or all the info?
        followup_data = self.data.select(
            "SubjectId", "FU_FUPALDAT", "FU_FUPDEDAT", "FU_FUPSST", "FU_FUPSSTCD"
        )  # .filter(
        # (pl.col("FU_FUPALDAT") != "NA")
        # | (pl.col("FU_FUPSST") != "NA")
        # )

        # TODO: Problem = Not all patients match these filter conditions, so for some we get None instead of instantiateed FollowUp class
        #   FIX!
        #   Yes - some patients just have NA for all conditions, we just need another way to get the rows instead of filtering (

        print(followup_data)

        for row in followup_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            if patient_id not in self.patient_data:
                continue

            lost_to_followup_status: bool = False
            date_lost_to_followup: Optional[dt.datetime] = None
            # if this row is Death or Alive, exit with lost_to_followup = False
            # if this row is something else, set status to True and get date
            if row["FU_FUPSST"] not in ["Alive", "Death"]:
                lost_to_followup_status = True
                if row["FU_FUPALDAT"] != "NA":
                    date_lost_to_followup = row["FU_FUPALDAT"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].lost_to_followup = FollowUp(
                    lost_to_followup=lost_to_followup_status,
                    date_lost_to_followup=date_lost_to_followup,
                )


"""
"FU_FUPALDAT" # date last known alive, "FU_FUPDEDAT" # date of death, "FU_FUPSST" # subject status code , "FU_FUPSSTCD" # subject status 
# FUPSST stores: alive, death and lost_to_followup
# for those with lost to followup we take date last known to be alive 
# and store that 
"""


"""
@dataclass
class Patient:
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
