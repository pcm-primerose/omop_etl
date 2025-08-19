# harmonization/impress.py

from typing import Optional
import polars as pl
import datetime as dt
from logging import getLogger

from omop_etl.harmonization.parsing.coercion import TypeCoercion
from omop_etl.harmonization.parsing.core import CoreParsers
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer
from omop_etl.harmonization.datamodels import (
    HarmonizedData,
    Patient,
    TumorType,
    StudyDrugs,
    Biomarkers,
    FollowUp,
    Ecog,
)
from omop_etl.harmonization.utils import detect_paired_field_collisions

log = getLogger(__name__)


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
        self._process_evaluability()
        self._process_ecog()
        self._process_previous_treatment_lines()

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

        for patient_id in self.patient_data:
            # get date of birth
            birth_rows = birth_dates.filter(pl.col("SubjectId") == patient_id)
            if birth_rows.height == 0:
                continue

            birth_row = birth_rows.row(0, named=True)
            birth_date = CoreParsers.parse_date_flexible(birth_row["DM_BRTHDAT"])
            if birth_date is None:
                continue

            # use latest treatment date
            treatment_rows = treatment_dates.filter(pl.col("SubjectId") == patient_id)
            if treatment_rows.height == 0:
                continue

            latest_treatment_date = None
            for row in treatment_rows.iter_rows(named=True):
                treatment_date = CoreParsers.parse_date_flexible(row["TR_TRC1_DT"])
                if treatment_date is not None and (
                    latest_treatment_date is None
                    or treatment_date > latest_treatment_date
                ):
                    latest_treatment_date = treatment_date

            if latest_treatment_date is None:
                continue

            age_years = int((latest_treatment_date - birth_date).days / 365.25)
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
        )  # Note: assumes tumor type data is always on the same row and we don't have multiple,
        # if this is not always the case, can use List[TumorType] in Patient class with default factory and store multiple

        # update patient objects
        for row in tumor_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            if patient_id not in self.patient_data:
                continue

            icd10_code = TypeCoercion.to_optional_string(row["COH_ICD10COD"])
            icd10_description = TypeCoercion.to_optional_string(row["COH_ICD10DES"])
            cohort_tumor_type = TypeCoercion.to_optional_string(row["COH_COHTT"])
            other_tumor_type = TypeCoercion.to_optional_string(row["COH_COHTTOSP"])

            # determine tumor type (mutually exclusive options)˛
            main_tumor_type = None
            main_tumor_type_code = None

            if (
                TypeCoercion.to_optional_string(row["COH_COHTTYPE"]) is not None
                and TypeCoercion.to_optional_int(row["COH_COHTTYPECD"]) is not None
            ):
                main_tumor_type = row["COH_COHTTYPE"]
                main_tumor_type_code = TypeCoercion.to_optional_int(
                    row["COH_COHTTYPECD"]
                )
            elif (
                TypeCoercion.to_optional_string(row["COH_COHTTYPE__2"]) is not None
                and TypeCoercion.to_optional_int(row["COH_COHTTYPE__2CD"]) is not None
            ):
                main_tumor_type = row["COH_COHTTYPE__2"]
                main_tumor_type_code = TypeCoercion.to_optional_int(
                    row["COH_COHTTYPE__2CD"]
                )

            tumor_type = TumorType()
            tumor_type.icd10_code = icd10_code
            tumor_type.icd10_description = icd10_description
            tumor_type.main_tumor_type = main_tumor_type
            tumor_type.main_tumor_type_code = main_tumor_type_code
            tumor_type.cohort_tumor_type = cohort_tumor_type
            tumor_type.other_tumor_type = other_tumor_type

            str(f"tumor type inst {tumor_type}")

            # assign complete object to patient
            self.patient_data[patient_id].tumor_type = tumor_type

    def _process_study_drugs(self):
        drug_data = self.data.select(
            "SubjectId",
            "COH_COHALLO1",
            "COH_COHALLO1CD",
            "COH_COHALLO1__2",
            "COH_COHALLO1__2CD",
            "COH_COHALLO1__3",
            "COH_COHALLO1__3CD",
            "COH_COHALLO2",
            "COH_COHALLO2CD",
            "COH_COHALLO2__2",
            "COH_COHALLO2__2CD",
            "COH_COHALLO2__3",
            "COH_COHALLO2__3CD",
        ).filter(
            (pl.col("COH_COHALLO1") != "NA")
            | (pl.col("COH_COHALLO1__2") != "NA")
            | (pl.col("COH_COHALLO1__3") != "NA")
            | (pl.col("COH_COHALLO2") != "NA")
            | (pl.col("COH_COHALLO2__2") != "NA")
            | (pl.col("COH_COHALLO2__3") != "NA")
        )

        for row in drug_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            if patient_id not in self.patient_data:
                continue

            # study drugs should always be mutually exclusive,
            # if they aren't, log and return None for entire model
            primary_drug_pairs = [
                ("COH_COHALLO1", "COH_COHALLO1CD"),
                ("COH_COHALLO1__2", "COH_COHALLO1__2CD"),
                ("COH_COHALLO1__3", "COH_COHALLO1__3CD"),
            ]

            secondary_drug_pairs = [
                ("COH_COHALLO2", "COH_COHALLO2CD"),
                ("COH_COHALLO2__2", "COH_COHALLO2__2CD"),
                ("COH_COHALLO2__3", "COH_COHALLO2__3CD"),
            ]

            primary_collision, _ = detect_paired_field_collisions(
                row, primary_drug_pairs, patient_id, "primary_study_drug"
            )

            secondary_collision, _ = detect_paired_field_collisions(
                row, secondary_drug_pairs, patient_id, "secondary_study_drug"
            )

            if primary_collision or secondary_collision:
                continue

            primary_drug = (
                TypeCoercion.to_optional_string(row["COH_COHALLO1"])
                or TypeCoercion.to_optional_string(row["COH_COHALLO1__2"])
                or TypeCoercion.to_optional_string(row["COH_COHALLO1__3"])
            )

            primary_drug_code = (
                TypeCoercion.to_optional_int(row["COH_COHALLO1CD"])
                or TypeCoercion.to_optional_int(row["COH_COHALLO1__2CD"])
                or TypeCoercion.to_optional_int(row["COH_COHALLO1__3CD"])
            )

            secondary_drug = (
                TypeCoercion.to_optional_string(row["COH_COHALLO2"])
                or TypeCoercion.to_optional_string(row["COH_COHALLO2__2"])
                or TypeCoercion.to_optional_string(row["COH_COHALLO2__3"])
            )

            secondary_drug_code = (
                TypeCoercion.to_optional_int(row["COH_COHALLO2CD"])
                or TypeCoercion.to_optional_int(row["COH_COHALLO2__2CD"])
                or TypeCoercion.to_optional_int(row["COH_COHALLO2__3CD"])
            )

            study_drugs = StudyDrugs()
            study_drugs.primary_treatment_drug = primary_drug
            study_drugs.secondary_treatment_drug = secondary_drug
            study_drugs.primary_treatment_drug_code = primary_drug_code
            study_drugs.secondary_treatment_drug_code = secondary_drug_code

            self.patient_data[patient_id].study_drugs = study_drugs

    def _process_biomarkers(self):
        biomarker_data = self.data.select(
            "SubjectId",
            "COH_GENMUT1",
            "COH_GENMUT1CD",
            "COH_COHCTN",
            "COH_COHTMN",
            "COH_EventDate",
        ).filter(
            (pl.col("COH_GENMUT1") is not None)
            | (pl.col("COH_GENMUT1CD") is not None)
            | (pl.col("COH_COHCTN") is not None)
            | (pl.col("COH_COHTMN") is not None)
        )

        for row in biomarker_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            if patient_id not in self.patient_data:
                continue

            biomarkers = Biomarkers()
            biomarkers.event_date = CoreParsers.parse_date_flexible(
                row["COH_EventDate"]
            )
            biomarkers.gene_and_mutation = TypeCoercion.to_optional_string(
                row["COH_GENMUT1"]
            )
            biomarkers.gene_and_mutation_code = TypeCoercion.to_optional_int(
                row["COH_GENMUT1CD"]
            )
            biomarkers.cohort_target_mutation = TypeCoercion.to_optional_string(
                row["COH_COHCTN"]
            )
            biomarkers.cohort_target_name = TypeCoercion.to_optional_string(
                row["COH_COHTMN"]
            )

            self.patient_data[patient_id].biomarker = biomarkers

    def _process_date_of_death(self):
        death_data = self.data.select(
            "SubjectId", "EOS_DEATHDTC", "FU_FUPDEDAT"
        ).filter((pl.col("EOS_DEATHDTC") != "NA") | (pl.col("FU_FUPDEDAT") != "NA"))

        for row in death_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            if patient_id not in self.patient_data:
                continue

            eos_date = CoreParsers.parse_date_flexible(row["EOS_DEATHDTC"])
            fu_date = CoreParsers.parse_date_flexible(row["FU_FUPDEDAT"])

            # use latest date
            death_date = None
            if eos_date and fu_date:
                death_date = max(eos_date, fu_date)
            elif eos_date:
                death_date = eos_date
            elif fu_date:
                death_date = fu_date

            self.patient_data[patient_id].date_of_death = death_date

    def _process_date_lost_to_followup(self):
        """Process lost to follow-up status and date from follow-up data"""
        # select all relevant follow-up data without filtering
        followup_data = self.data.select(
            "SubjectId", "FU_FUPALDAT", "FU_FUPDEDAT", "FU_FUPSST", "FU_FUPSSTCD"
        )

        for row in followup_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            lost_to_followup_status = False
            date_lost_to_followup = None

            # get followup status and convert to lowercase
            fu_status = TypeCoercion.to_optional_string(row["FU_FUPSST"])
            if fu_status is not None:
                fu_status = fu_status.lower()
                if fu_status not in ["alive", "death"]:
                    lost_to_followup_status = True
                    date_lost_to_followup = CoreParsers.parse_date_flexible(
                        row["FU_FUPALDAT"]
                    )

            followup = FollowUp()
            followup.lost_to_followup = lost_to_followup_status
            followup.date_lost_to_followup = date_lost_to_followup

            self.patient_data[patient_id].lost_to_followup = followup

    def _process_evaluability(self):
        """
        Current filtering criteria for marking patient as evaluable for efficacy analysis:
            - must have treatment length over 28 days (taking treatment length of first treatment) and either one of:
            - tumor assessment (all rows in assessments suffice, EventDate always has data)
            - clinical assessment (EventId from EOT sheet)

        Unsure if these are correct criteria!

        TODO:
            - distinguish between oral and IV drug treatment lengt requirements (4 --> IV, 8 --> oral)?
        """
        evaluability_data = self.data.select(
            "SubjectId",
            "TR_TROSTPDT",
            "TR_TRO_STDT",
            "RA_EventDate",
            "RNRSP_EventDate",
            "RCNT_EventDate",
            "RNTMNT_EventDate",
            "EOT_EventDate",
        )

        # lugrsp data is missing in synthetic data (becuase no subject in eCRF have lugano data)
        optional_lugrsp: Optional[pl.Series] = self.data.get_column(
            name="LUGRSP_EventDate", default=None
        )
        if optional_lugrsp:
            evaluability_data.join(optional_lugrsp)

        for patient_id in self.patient_data:
            patient_data = evaluability_data.filter(pl.col("SubjectId") == patient_id)

            start_dates = []
            end_dates = []

            has_tumor_evaluation = False
            has_eot_evaluation = False

            # check all evaluations
            for row in patient_data.iter_rows(named=True):
                start_date = CoreParsers.parse_date_flexible(row["TR_TRO_STDT"])
                if start_date:
                    start_dates.append(start_date)

                end_date = CoreParsers.parse_date_flexible(row["TR_TROSTPDT"])
                if end_date:
                    end_dates.append(end_date)

                if any(
                    [
                        CoreParsers.parse_date_flexible(row["RA_EventDate"]),
                        CoreParsers.parse_date_flexible(row["RNRSP_EventDate"]),
                        CoreParsers.parse_date_flexible(row["RCNT_EventDate"]),
                        CoreParsers.parse_date_flexible(row["RNTMNT_EventDate"]),
                        # CoreParsers.parse_date_flexible(row["LUGRSP_EventDate"]), note: no data for this in ecrf, not yet implemented
                    ]
                ):
                    has_tumor_evaluation = True

                # check for end-of-treatment evaluatuon
                if CoreParsers.parse_date_flexible(row["EOT_EventDate"]):
                    has_eot_evaluation = True

            # check treatment length
            sufficient_treatment_length = False
            if start_dates and end_dates:
                earliest_start = min(start_dates)
                latest_end = max(end_dates)
                sufficient_treatment_length = (
                    latest_end - earliest_start
                ) >= dt.timedelta(days=28)

            # apply criteria filters
            evaluable_status = sufficient_treatment_length and (
                has_tumor_evaluation or has_eot_evaluation
            )

            print(f"Evaluability status: {evaluable_status}")

            self.patient_data[
                patient_id
            ].evaluable_for_efficacy_analysis = evaluable_status

    def _process_ecog(self):
        # parse ECOG description and grade
        ecog_data = self.data.select(
            "SubjectId", "ECOG_EventId", "ECOG_ECOGS", "ECOG_ECOGSCD"
        )

        for row in ecog_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            ecog_description = None
            ecog_grade = None
            if TypeCoercion.to_optional_string(row["ECOG_EventId"]):
                ecog_description = TypeCoercion.to_optional_string(row["ECOG_ECOGS"])
                ecog_grade = TypeCoercion.to_optional_int(row["ECOG_ECOGSCD"])

            ecog = Ecog()
            ecog.grade = ecog_grade
            ecog.description = ecog_description

            self.patient_data[patient_id].ecog = ecog

    def _process_medical_history(self):
        # MHTER, MHSTDAT, MHONGO, MHONGOCD
        # TODO: Implement when we know if we need this, and after eCRF extraction upadate
        pass

    def _process_previous_treatment_lines(self):
        # TODO: Add CT_CTTYPESP when eCRF extraction update
        #   it contains e.g. "horomonal therapy"
        treatment_lines_data = self.data.select(
            "SubjectId",
            "CT_CTTYPE",
            "CT_CTTYPECD",
            "CT_CTSTDAT",
            "CT_CTENDAT",
            "CT_CTSPID",
        )

        for row in treatment_lines_data.iter_rows(named=True):
            print(row)

        # TODO
        #   Just implement intended data in test fixtures and make sure implementation works
        #   and stop spending so much time on mock data (can also just add some empty cols with NA for new data)
        #   but need to re-run eCRF script now anyways


# TODO after making nice mock data add dates to everything
