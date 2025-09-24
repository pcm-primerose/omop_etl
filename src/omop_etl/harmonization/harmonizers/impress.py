import re
from typing import Mapping, Any, Optional
from deprecated import deprecated
import polars as pl
from logging import getLogger

from omop_etl.harmonization.parsing.coercion import TypeCoercion
from omop_etl.harmonization.parsing.core import CoreParsers, PolarsParsers
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer
from omop_etl.harmonization.utils import detect_paired_field_collisions
from omop_etl.harmonization.datamodels import (
    HarmonizedData,
    Patient,
    TumorType,
    StudyDrugs,
    Biomarkers,
    FollowUp,
    EcogBaseline,
    MedicalHistory,
    PreviousTreatments,
    TreatmentCycle,
    ConcomitantMedication,
    AdverseEvent,
    TumorAssessmentBaseline,
    TumorAssessment,
    C30,
    EQ5D,
    BestOverallResponse,
)


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
        self._process_ecog_baseline()
        self._process_previous_treatments()
        self._process_medical_histories()
        self._process_treatment_start_date()
        self._process_treatment_stop_date()
        self._process_start_last_cycle()
        self._process_treatment_cycle()
        self._process_concomitant_medication()
        self._process_has_any_adverse_events()
        self._process_number_of_adverse_events()
        self._process_number_of_serious_adverse_events()
        self._process_adverse_events()
        self._process_baseline_tumor_assessment()
        self._process_tumor_assessments()
        self._process_c30()
        self._process_eq5d()
        self._process_best_overall_response()
        self._process_clinical_benefit()
        self._process_eot_reason()
        self._process_eot_date()

        # flatten patient values
        patients = list(self.patient_data.values())

        for idx in range(len(patients)):
            print(f"Patient {idx}: {patients[idx]} \n")

        output = HarmonizedData(patients=patients, trial_id=self.trial_id)
        # print(f"Impress output: {output}")

        return output

    # todo: add collapsing of multiple records per patient,
    #   just unsure how to handle these additional events in datamodel,
    #   what exact data is shared, and under what conditions do subjects get two seperate records?
    def _process_patient_id(self) -> None:
        """Process patient ID and create patient object"""
        patient_ids = self.data.select("SubjectId").unique().to_series().to_list()

        # create initial patient object
        for patient_id in patient_ids:
            self.patient_data[patient_id] = Patient(trial_id=self.trial_id, patient_id=patient_id)

    def _process_cohort_name(self) -> None:
        """Process cohort names and update patient objects"""
        cohort_data = self.data.filter(pl.col("COH_COHORTNAME").is_not_null())

        for row in cohort_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            cohort_name = row["COH_COHORTNAME"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].cohort_name = cohort_name

    def _process_gender(self) -> None:
        gender_data = (self.data.select(["SubjectId", "DM_SEX"])).filter(pl.col("DM_SEX").is_not_null())

        for row in gender_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            sex = TypeCoercion.to_optional_string(row["DM_SEX"])
            if not sex:
                log.warning(f"No sex found for patient {patient_id}")
                continue

            # todo: parse in polars instead
            if sex.lower() == "m" or sex.lower() == "male":
                sex = "male"
            elif sex.lower() == "f" or sex.lower() == "female":
                sex = "female"
            else:
                log.warning(f"Cannot parse sex from {patient_id}, {sex}")
                continue

            self.patient_data[patient_id].sex = sex

    def _process_age(self) -> None:
        """Process and calculate age at treatment start and update patient object"""
        age_data = (
            self.data.lazy()
            .group_by("SubjectId")
            .agg(
                [
                    pl.col("DM_BRTHDAT").drop_nulls().first().alias("birth_date"),
                    pl.col("TR_TRC1_DT").drop_nulls().min().alias("first_treatment"),
                ],
            )
            .collect()
        )

        for row in age_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            birth_date = CoreParsers.parse_date_flexible(row["birth_date"])
            latest_treatment = CoreParsers.parse_date_flexible(row["first_treatment"])

            # TODO: use inclusion date or molecular profiling date as fallbacks?
            if not birth_date:
                log.warning(f"No birth date found for {patient_id}")
                continue

            if not latest_treatment:
                log.warning(f"No latest treatment date found for {patient_id}")
                continue

            age_years = int((latest_treatment - birth_date).days / 365.25)
            self.patient_data[patient_id].age = age_years

    def _process_tumor_type(self) -> None:
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
                ),
            )
        ).filter(
            pl.col("COH_ICD10COD").is_not_null(),
        )  # Note: assumes tumor type data is always on the same row and we don't have multiple

        # update patient objects
        for row in tumor_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            icd10_code = TypeCoercion.to_optional_string(row["COH_ICD10COD"])
            icd10_description = TypeCoercion.to_optional_string(row["COH_ICD10DES"])
            cohort_tumor_type = TypeCoercion.to_optional_string(row["COH_COHTT"])
            other_tumor_type = TypeCoercion.to_optional_string(row["COH_COHTTOSP"])

            # determine tumor type (mutually exclusive options)
            main_tumor_type: str | None = None
            main_tumor_type_code: str | None = None

            if TypeCoercion.to_optional_string(row["COH_COHTTYPE"]) is not None and TypeCoercion.to_optional_int(row["COH_COHTTYPECD"]) is not None:
                main_tumor_type = row["COH_COHTTYPE"]
                main_tumor_type_code = TypeCoercion.to_optional_int(row["COH_COHTTYPECD"])
            elif (
                TypeCoercion.to_optional_string(row["COH_COHTTYPE__2"]) is not None
                and TypeCoercion.to_optional_int(row["COH_COHTTYPE__2CD"]) is not None
            ):
                main_tumor_type = row["COH_COHTTYPE__2"]
                main_tumor_type_code = TypeCoercion.to_optional_int(row["COH_COHTTYPE__2CD"])

            tumor_type = TumorType(patient_id=patient_id)
            tumor_type.icd10_code = icd10_code
            tumor_type.icd10_description = icd10_description
            tumor_type.main_tumor_type = main_tumor_type
            tumor_type.main_tumor_type_code = main_tumor_type_code
            tumor_type.cohort_tumor_type = cohort_tumor_type
            tumor_type.other_tumor_type = other_tumor_type

            # assign complete object to patient
            self.patient_data[patient_id].tumor_type = tumor_type

    def _process_study_drugs(self) -> None:
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
            (pl.col("COH_COHALLO1").is_not_null())
            | (pl.col("COH_COHALLO1__2").is_not_null())
            | (pl.col("COH_COHALLO1__3").is_not_null())
            | (pl.col("COH_COHALLO2").is_not_null())
            | (pl.col("COH_COHALLO2__2").is_not_null())
            | (pl.col("COH_COHALLO2__3").is_not_null()),
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

            primary_collision, _ = detect_paired_field_collisions(row, primary_drug_pairs, patient_id, "primary_study_drug")

            secondary_collision, _ = detect_paired_field_collisions(row, secondary_drug_pairs, patient_id, "secondary_study_drug")

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

            study_drugs = StudyDrugs(patient_id=patient_id)
            study_drugs.primary_treatment_drug = primary_drug
            study_drugs.secondary_treatment_drug = secondary_drug
            study_drugs.primary_treatment_drug_code = primary_drug_code
            study_drugs.secondary_treatment_drug_code = secondary_drug_code

            self.patient_data[patient_id].study_drugs = study_drugs

    def _process_biomarkers(self) -> None:
        biomarker_data = self.data.select(
            "SubjectId",
            "COH_GENMUT1",
            "COH_GENMUT1CD",
            "COH_COHCTN",
            "COH_COHTMN",
            "COH_EventDate",
        ).filter(
            (pl.col("COH_GENMUT1").is_not_null())
            | (pl.col("COH_GENMUT1CD").is_not_null())
            | (pl.col("COH_COHCTN").is_not_null())
            | (pl.col("COH_COHTMN").is_not_null()),
        )

        for row in biomarker_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            biomarkers = Biomarkers(patient_id=patient_id)
            biomarkers.date = CoreParsers.parse_date_flexible(row["COH_EventDate"])
            biomarkers.gene_and_mutation = TypeCoercion.to_optional_string(row["COH_GENMUT1"])
            biomarkers.gene_and_mutation_code = TypeCoercion.to_optional_int(row["COH_GENMUT1CD"])
            biomarkers.cohort_target_mutation = TypeCoercion.to_optional_string(row["COH_COHTMN"])
            biomarkers.cohort_target_name = TypeCoercion.to_optional_string(row["COH_COHCTN"])

            self.patient_data[patient_id].biomarker = biomarkers

    def _process_date_of_death(self) -> None:
        death_data = self.data.select("SubjectId", "EOS_DEATHDTC", "FU_FUPDEDAT").filter(
            (pl.col("EOS_DEATHDTC").is_not_null()) | (pl.col("FU_FUPDEDAT").is_not_null()),
        )

        for row in death_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

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
            elif not eos_date and not fu_date:
                log.warning(f"No data of death found for {patient_id}")
                death_date = None

            self.patient_data[patient_id].date_of_death = death_date

    def _process_has_any_adverse_events(self) -> None:
        ae_status = (
            self.data.with_columns(
                row_has_ae=pl.any_horizontal(
                    [
                        (
                            pl.col("AE_AECTCAET").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AECTCAET").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                        (
                            pl.col("AE_AESTDAT").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AESTDAT").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                        (
                            pl.col("AE_AETOXGRECD").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AETOXGRECD").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                    ],
                ),
            )
            .group_by("SubjectId")
            .agg(has_ae=pl.col("row_has_ae").any())
        )

        for row in ae_status.iter_rows(named=True):
            pid = row["SubjectId"]
            if pid in self.patient_data:
                self.patient_data[pid].has_any_adverse_events = bool(row["has_ae"])

    def _process_number_of_adverse_events(self) -> None:
        ae_num = (
            self.data.with_columns(
                ae_num=pl.any_horizontal(
                    [
                        (
                            pl.col("AE_AECTCAET").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AECTCAET").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                        (
                            pl.col("AE_AESTDAT").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AESTDAT").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                        (
                            pl.col("AE_AETOXGRECD").cast(pl.Utf8).is_not_null()
                            & (pl.col("AE_AETOXGRECD").cast(pl.Utf8).str.strip_chars().str.len_bytes() > 0)
                        ),
                    ],
                ),
            )
            .group_by("SubjectId")
            .agg(ae_number=pl.col("ae_num").sum())
        )

        for row in ae_num.iter_rows(named=True):
            pid = row["SubjectId"]
            if pid in self.patient_data:
                self.patient_data[pid].number_of_adverse_events = int(row["ae_number"])

    def _process_number_of_serious_adverse_events(self) -> None:
        sae_num = (
            self.data.with_columns(
                sae_num=pl.any_horizontal(
                    [pl.col("AE_AESERCD").cast(pl.Utf8).is_not_null() & (pl.col("AE_AESERCD").cast(pl.Utf8).str.strip_chars() == "1")],
                ),
            )
            .group_by("SubjectId")
            .agg(sae_number=pl.col("sae_num").sum())
        )

        for row in sae_num.iter_rows(named=True):
            pid = row["SubjectId"]
            if pid in self.patient_data:
                self.patient_data[pid].number_of_serious_adverse_events = int(row["sae_number"])

    def _process_date_lost_to_followup(self) -> None:
        """Process lost to follow-up status and date from follow-up data"""
        # select all relevant follow-up data without filtering
        followup_data = self.data.select("SubjectId", "FU_FUPALDAT", "FU_FUPDEDAT", "FU_FUPSST", "FU_FUPSSTCD")

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
                    date_lost_to_followup = CoreParsers.parse_date_flexible(row["FU_FUPALDAT"])

            followup = FollowUp(patient_id=patient_id)
            followup.lost_to_followup = lost_to_followup_status
            followup.date_lost_to_followup = date_lost_to_followup

            self.patient_data[patient_id].lost_to_followup = followup

    def _process_evaluability(self) -> None:
        """
        Filtering criteria:
            Any patient having valid treatment for sufficient length (21 days IV, 28 days oral).
            Days are not inclusive, the end date of the previous cycle is the start of following,
              e.g. 01-01-2001 to 21-01-2001 has insufficient length.

        For subjects with oral drugs, the start and end date per cycle is checked directly.
            - if a subject has any cycle lasting 28 days or more they are marked as having sufficient treatment length

        For subjects without oral drugs, cycle stop date is set to start date of next cycle and needs to last 21 days or more.
            - Note: this means subjects with just one cycle are marked as non-evaluable since cycle end cannot be determined.
            - each cycle is grouped by treatment number, any treatment having a cycle with sufficient length marks subject as evaluable.
            - assumes no malformed dates, because imputing would change the length.

        Old filteing criteria:
            Patients marked as evaluable for efficacy analysis needs to have:
                - sufficient treatment length for any cycle (21 days for IV, 28 days for oral) and *either one of*:
                - tumor assessment after week 4 (patient has any tumor assessment with EventId==V04 in RA, RCNT, RTNTMNT, RNRSP)
                - clinical assessment (patient has stopped treatment: EventDate from EOT sheet)
        """
        evaluability_data = self.data.select(
            "SubjectId",
            "TR_TROSTPDT",
            "TR_TRO_STDT",
            "TR_TRTNO",
            "TR_TRC1_DT",
            "TR_TRCYNCD",
            # not currently used:
            # "RA_EventDate",
            # "RA_EventId",
            # "RNRSP_EventDate",
            # "RNRSP_EventId",
            # "RCNT_EventDate",
            # "RCNT_EventId",
            # "RNTMNT_EventDate",
            # "RNTMNT_EventId",
            # "EOT_EventDate",
        )

        def oral_treatment_lengths() -> pl.DataFrame:
            oral_sufficient_treatment_length = (
                evaluability_data.select(["SubjectId", "TR_TRO_STDT", "TR_TROSTPDT", "TR_TRCYNCD"])
                .with_columns(
                    start=pl.col("TR_TRO_STDT").cast(pl.Utf8).replace("", None).str.strptime(pl.Date, strict=False),
                    stop=pl.col("TR_TROSTPDT").cast(pl.Utf8).replace("", None).str.strptime(pl.Date, strict=False),
                    not_recieved_treatment_this_cycle=pl.col("TR_TRCYNCD") != 1,
                )
                .filter(~pl.col("not_recieved_treatment_this_cycle"))
                .with_columns(treatment_duration=((pl.col("stop") - pl.col("start")).dt.total_days()))
                .group_by("SubjectId")
                .agg((pl.col("treatment_duration").fill_null(-1) >= 28).any().alias("oral_sufficient_treatment_length"))
            )
            return oral_sufficient_treatment_length

        def iv_treatment_lengths() -> pl.DataFrame:
            iv_sufficient_treatment_length = (
                evaluability_data.select(
                    "SubjectId",
                    "TR_TRTNO",
                    "TR_TRC1_DT",
                    "TR_TRO_STDT",
                    "TR_TROSTPDT",
                    "TR_TRCYNCD",
                )
                # remove oral treatment rows
                .with_columns(
                    oral_present=pl.any_horizontal(
                        pl.col(["TR_TRO_STDT", "TR_TROSTPDT"]).cast(pl.Utf8).replace("", None).str.len_bytes().fill_null(0) > 0,
                    ),
                    start=pl.col("TR_TRC1_DT").cast(pl.Utf8, strict=False).replace("", None).str.strptime(pl.Date, strict=False),
                    not_recieved_treatment_this_cycle=pl.col("TR_TRCYNCD") != 1,
                )
                .filter(~pl.col("oral_present") & ~pl.col("not_recieved_treatment_this_cycle"))
                .drop_nulls("start")
                .sort(["SubjectId", "TR_TRTNO", "start"])
                # partitioned shift to make next start
                .with_columns(pl.col("start").shift(-1).over(["SubjectId", "TR_TRTNO"]).alias("next_start"))
                # compute gap days
                .with_columns((pl.col("next_start") - pl.col("start")).dt.total_days().alias("gap_days"))
                .group_by("SubjectId")
                .agg(pl.col("gap_days").ge(21).fill_null(False).any().alias("iv_sufficient_treatment_length"))
            )

            return iv_sufficient_treatment_length

        @deprecated
        def eot_filter() -> pl.DataFrame:
            has_ended_treatment = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(pl.col(["EOT_EventDate"]).str.len_bytes() > 0).any().alias("has_clinical_assessment"),
            )
            return has_ended_treatment

        @deprecated
        def tumor_assessment() -> pl.DataFrame:
            # need to add V04 filter (if this is to be used again)
            has_tumor_assessment_week_4 = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(
                    pl.col(
                        [
                            "RA_EventDate",
                            "RNRSP_EventDate",
                            "RCNT_EventDate",
                            "RNTMNT_EventDate",
                        ],
                    ).str.len_bytes()
                    > 0,
                )
                .any()
                .alias("has_tumor_assessment"),
            )
            return has_tumor_assessment_week_4

        def _merge_evaluability() -> pl.DataFrame:
            base = evaluability_data.select("SubjectId").unique()
            _merged_df: pl.DataFrame = (
                base.join(oral_treatment_lengths(), on="SubjectId", how="left")
                .join(iv_treatment_lengths(), on="SubjectId", how="left")
                .with_columns(
                    pl.col("oral_sufficient_treatment_length").fill_null(False),
                    pl.col("iv_sufficient_treatment_length").fill_null(False),
                )
                .with_columns(is_evaluable=(pl.col("oral_sufficient_treatment_length") | pl.col("iv_sufficient_treatment_length")))
            )

            return _merged_df

        # hydrate
        merged_evaluability: pl.DataFrame = _merge_evaluability()
        for row in merged_evaluability.iter_rows(named=True):
            patient_id = row["SubjectId"]
            self.patient_data[patient_id].evaluable_for_efficacy_analysis = bool(row["is_evaluable"])

    def _process_ecog_baseline(self) -> None:
        """
        Parses dates with defaults, strips description data, casts to correct types.
        Only select one baseline ECOG event per patient, using latest available date.
        """

        ecog_base = self.data.select("SubjectId", "ECOG_EventId", "ECOG_ECOGS", "ECOG_ECOGSCD", "ECOG_ECOGDAT").filter(
            pl.col("ECOG_EventId") == "V00",
        )  # filter in base to compare only baseline

        def parse_ecog_data(ecog_data: pl.DataFrame) -> pl.DataFrame:
            filtered_ecog_data = (
                ecog_data.with_columns(
                    date=PolarsParsers.parse_date_column(pl.col("ECOG_ECOGDAT")),
                    grade=pl.col("ECOG_ECOGSCD").cast(pl.Int64, strict=False),
                    _stripped_description=pl.col("ECOG_ECOGS").cast(pl.Utf8, strict=False).str.strip_chars(),
                )
                .with_columns(
                    description=pl.when(pl.col("_stripped_description").str.to_lowercase().is_in(PolarsParsers.NA_VALUES))
                    .then(None)
                    .otherwise(pl.col("_stripped_description")),
                )
                .select("SubjectId", "date", "description", "grade")
            )
            return filtered_ecog_data

        def select_latest_baseline(data: pl.DataFrame) -> pl.DataFrame:
            _latest = data.sort(["SubjectId", "date"]).group_by("SubjectId").tail(1)
            return _latest

        def filter_all_nulls(data: pl.DataFrame) -> pl.DataFrame:
            return data.with_columns(has_ecog=pl.any_horizontal(pl.col(["description"]).is_not_null()).fill_null(False))

        def merge_ecog(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            return base.join(processed, on="SubjectId", how="left")

        # process
        parsed = parse_ecog_data(ecog_data=ecog_base)
        latest = select_latest_baseline(parsed)
        valid = filter_all_nulls(latest)
        joined = merge_ecog(base=ecog_base, processed=valid)
        labeled = filter_all_nulls(joined).select("SubjectId", "date", "description", "grade", "has_ecog")

        def build_ecog(pid: str, row: Mapping[str, Any]) -> EcogBaseline:
            e = EcogBaseline(pid)
            e.date = row["date"]
            e.description = row["description"]
            e.grade = row["grade"]
            return e

        self.hydrate_singleton(
            labeled,
            patients=self.patient_data,
            builder=build_ecog,
            target_attr="ecog_baseline",
        )

    def _process_medical_histories(self) -> None:
        mh_base = self.data.select(
            "SubjectId",
            "MH_MHSPID",
            "MH_MHTERM",
            "MH_MHSTDAT",
            "MH_MHENDAT",
            "MH_MHONGO",
            "MH_MHONGOCD",
        )

        def filter_medical_histories(data: pl.DataFrame) -> pl.DataFrame:
            filtered_data = data.with_columns(
                term=(PolarsParsers.null_if_na(pl.col("MH_MHTERM"))).cast(pl.Utf8, strict=False).str.strip_chars(),
                sequence_id=(pl.col("MH_MHSPID")).cast(pl.Int64, strict=False),
                start_date=PolarsParsers.parse_date_column(pl.col("MH_MHSTDAT")),
                end_date=PolarsParsers.parse_date_column(pl.col("MH_MHENDAT")),
                status=(PolarsParsers.null_if_na(pl.col("MH_MHONGO"))).cast(pl.Utf8, strict=False).str.strip_chars(),
                status_code=(pl.col("MH_MHONGOCD")).cast(pl.Int64, strict=False),
            ).filter(pl.col("term").is_not_null())

            return filtered_data

        def merge_medical_history(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col(
                        [
                            "term",
                            "sequence_id",
                            "start_date",
                            "end_date",
                            "status",
                            "status_code",
                        ],
                    ).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_medical_histories(mh_base)
        merged = merge_medical_history(base=mh_base, processed=filtered)

        # pack to structs grouped by SubjectId
        packed = self.pack_structs(
            merged,
            subject_col="SubjectId",
            value_cols=merged.select(pl.all().exclude("SubjectId")).columns,
            order_by_cols=["sequence_id", "start_date"],
        )

        # build mh objects
        def build_mh(pid: str, s: Mapping[str, Any]) -> MedicalHistory:
            obj = MedicalHistory(pid)
            obj.term = s["term"]
            obj.sequence_id = s["sequence_id"]
            obj.start_date = s["start_date"]
            obj.end_date = s["end_date"]
            obj.status = s["status"]
            obj.status_code = s["status_code"]
            return obj

        # hydrate to Patient class
        self.hydrate_list_field(
            packed,
            builder=build_mh,
            patients=self.patient_data,
            target_attr="medical_histories",
            skip_missing=False,
        )

    def _process_previous_treatments(self) -> None:
        ct_base = self.data.select(
            "SubjectId",
            "CT_CTTYPE",
            "CT_CTTYPECD",
            "CT_CTSPID",
            "CT_CTSTDAT",
            "CT_CTENDAT",
            "CT_CTTYPESP",
        )

        def filter_previous_treatments(data: pl.DataFrame) -> pl.DataFrame:
            filtered_data = data.with_columns(
                treatment=(
                    pl.when(pl.col("CT_CTTYPE").is_not_null()).then(pl.col("CT_CTTYPE").cast(pl.Utf8, strict=False).str.strip_chars()).otherwise(None)
                ),
                treatment_code=(pl.col("CT_CTTYPECD")).cast(pl.Int64, strict=False),
                sequence_id=(pl.col("CT_CTSPID")).cast(pl.Int64, strict=False),
                start_date=PolarsParsers.parse_date_column(pl.col("CT_CTSTDAT")),
                end_date=PolarsParsers.parse_date_column(pl.col("CT_CTENDAT")),
                additional_treatment=(
                    pl.when(pl.col("CT_CTTYPESP").is_not_null())
                    .then(pl.col("CT_CTTYPESP").cast(pl.Utf8, strict=False).str.strip_chars())
                    .otherwise(None)
                ),
            ).filter(pl.col("treatment").is_not_null())
            return filtered_data

        def merge_previous_treatments(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col(
                        [
                            "treatment",
                            "treatment_code",
                            "sequence_id",
                            "start_date",
                            "end_date",
                            "additional_treatment",
                        ],
                    ).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_previous_treatments(ct_base)
        merged = merge_previous_treatments(base=ct_base, processed=filtered)

        # pack
        packed = self.pack_structs(
            merged,
            subject_col="SubjectId",
            value_cols=merged.select(pl.all().exclude("SubjectId")).columns,
            order_by_cols=["sequence_id", "start_date"],
        )

        def build_ct(pid: str, s: Mapping[str, Any]) -> PreviousTreatments:
            obj = PreviousTreatments(pid)
            obj.treatment = s["treatment"]
            obj.treatment_code = s["treatment_code"]
            obj.treatment_sequence_number = s["sequence_id"]
            obj.start_date = s["start_date"]
            obj.end_date = s["end_date"]
            obj.additional_treatment = s["additional_treatment"]
            return obj

        self.hydrate_list_field(
            packed,
            builder=build_ct,
            patients=self.patient_data,
            target_attr="previous_treatments",
            skip_missing=False,
        )

    def _process_treatment_start_date(self) -> None:
        treatment_start_data = (
            self.data.lazy()
            .select(["SubjectId", "TR_TRNAME", "TR_TRC1_DT"])
            .with_columns(
                tr_name=pl.col("TR_TRNAME").cast(pl.Utf8).str.strip_chars(),
            )
            # keep only real names: non-null AND length > 0
            .filter(pl.col("tr_name").is_not_null() & (pl.col("tr_name").str.len_chars() > 0))
            .group_by("SubjectId")
            .agg(pl.col("TR_TRC1_DT").drop_nulls().min().alias("treatment_start_date"))
            .collect()
            .select(["SubjectId", "treatment_start_date"])
        )

        for row in treatment_start_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            treatment_start_date = CoreParsers.parse_date_flexible(row["treatment_start_date"])
            self.patient_data[patient_id].treatment_start_date = treatment_start_date

    def _process_treatment_stop_date(self) -> None:
        treatment_stop_data = (
            self.data.select(
                "SubjectId",
                "TR_TRCYNCD",
                "TR_TROSTPDT",
                "TR_TRC1_DT",
                "EOT_EOTDAT",
            )
            .with_columns(
                valid=pl.col("TR_TRCYNCD").cast(pl.Int64, strict=False) == 1,
                eot_date=PolarsParsers.parse_date_column("EOT_EOTDAT"),
                oral_stop=PolarsParsers.parse_date_column("TR_TROSTPDT"),
                iv_start=PolarsParsers.parse_date_column("TR_TRC1_DT"),
            )
            # only valid TR rows for oral/IV
            .with_columns(
                oral_stop_valid=pl.when(pl.col("valid")).then(pl.col("oral_stop")).otherwise(None),
                iv_start_valid=pl.when(pl.col("valid")).then(pl.col("iv_start")).otherwise(None),
            )
            .group_by("SubjectId")
            .agg(
                last_eot=pl.col("eot_date").max(),
                last_oral=pl.col("oral_stop_valid").max(),
                last_iv=pl.col("iv_start_valid").max(),
            )
            .with_columns(
                # precedence: EOT > oral > IV
                treatment_end=pl.coalesce([pl.col("last_eot"), pl.col("last_oral"), pl.col("last_iv")]),
                treatment_end_source=(
                    pl.when(pl.col("last_eot").is_not_null())
                    .then(pl.lit("EOT"))
                    .when(pl.col("last_oral").is_not_null())
                    .then(pl.lit("ORAL_STOP"))
                    .when(pl.col("last_iv").is_not_null())
                    .then(pl.lit("IV_START"))
                    .otherwise(pl.lit(None))
                ),
            )
        )

        # log no ends
        subjects = self.data.select("SubjectId").unique()
        df_all = subjects.join(treatment_stop_data, on="SubjectId", how="left")
        no_end = df_all.filter(pl.col("treatment_end").is_null()).get_column("SubjectId").to_list()
        for pid in no_end:
            log.warning(f"No treatment end found for SubjectId={pid}")

        # hydrate
        for pid, end_date in treatment_stop_data.select("SubjectId", "treatment_end").iter_rows():
            self.patient_data[pid].treatment_end_date = end_date

    def _process_start_last_cycle(self) -> None:
        """
        Note: currently not filtering for valid cycles, just selecting latest treatment starts.
        Set enforce_valid=True if TR_TRCYNCD must be 1 (i.e. filtering for valid cycles only)
        """
        enforce_valid = False

        last_cycle_data = (
            self.data.select("SubjectId", "TR_TRC1_DT", "TR_TRCYNCD")
            .with_columns(
                cycle_start=PolarsParsers.parse_date_column("TR_TRC1_DT"),
                valid=pl.col("TR_TRCYNCD").cast(pl.Int64, strict=False) == 1,
            )
            .with_columns(cycle_start=pl.when(~pl.lit(enforce_valid) | pl.col("valid")).then(pl.col("cycle_start")).otherwise(None))
            .group_by("SubjectId")
            .agg(treatment_start_last_cycle=pl.col("cycle_start").max())
            .select("SubjectId", "treatment_start_last_cycle")
        )

        # hydrate
        for pid, last_cycle in last_cycle_data.select("SubjectId", "treatment_start_last_cycle").iter_rows():
            self.patient_data[pid].treatment_start_last_cycle = last_cycle

    def _process_treatment_cycle(self) -> None:
        treatment_cycle_cols = [
            "SubjectId",
            "TR_TRNAME",
            "TR_TRTNO",
            "TR_TRCNO1",
            "TR_TRC1_DT",
            "TR_TRO_STDT",
            "TR_TROSTPDT",
            "TR_TRDSDEL1",
            "TR_TRCYN",
            "TR_TRO_YNCD",
            "TR_TRIVU1",
            "TR_TRIVDS1",
            "TR_TRCYNCD",
            "TR_TRIVDELYN1",
            "TR_TRO_YN",
            "TR_TROREA",
            "TR_TROOTH",
            "TR_TRODSU",
            "TR_TRODSUOT",
            "TR_TRODSTOT",
            "TR_TROTAKE",
            "TR_TROTAKECD",
            "TR_TROTABNO",
            "TR_TROSPE",
        ]

        cycle_base = self.data.select(treatment_cycle_cols)

        def add_treatment_type(frame: pl.DataFrame) -> pl.DataFrame:
            """
            If any bytes in any oral-only cols, set to `oral`, if any in iv-only cols, set to `IV` else `None`.
            """

            oral_only_cols = ["TR_TRO_YN", "TR_TRODSTOT", "TR_TRO_STDT", "TR_TROSTPDT"]
            iv_only_cols = ["TR_TRIVDS1", "TR_TRIVU1", "TR_TRIVDELYN1"]

            oral_cols = [c for c in oral_only_cols if c in frame.columns]
            iv_cols = [c for c in iv_only_cols if c in frame.columns]

            def row_has_any(cols: list[str]) -> pl.Expr:
                if not cols:
                    return pl.lit(False)

                return pl.any_horizontal(pl.col(cols).cast(pl.Utf8).str.strip_chars().str.len_bytes().fill_null(0) > 0)

            has_oral = row_has_any(oral_cols)
            has_iv = row_has_any(iv_cols)

            return frame.with_columns(
                pl.when(has_oral).then(pl.lit("oral")).when(has_iv).then(pl.lit("IV")).otherwise(pl.lit(None, dtype=pl.Utf8)).alias("treatment_type"),
            )

        def add_iv_cycle_stop_dates(frame: pl.DataFrame) -> pl.DataFrame:
            """
            For IV cycles, selects next cycle start date - 1 day as current cycle end, set to `None` for last cycle.
            """
            iv_cycle_ends = (
                frame.with_columns(start=PolarsParsers.parse_date_column(pl.col("TR_TRC1_DT")))
                .sort(["SubjectId", "TR_TRTNO", "start"])
                .with_columns(
                    # apply shift to IV rows, others get None
                    next_start=pl.when(pl.col("treatment_type") == "IV")
                    .then(pl.col("start").shift(-1).over(["SubjectId", "TR_TRTNO"]))
                    .otherwise(None),
                )
                .with_columns(
                    # calculate end date where next_start exists
                    iv_cycle_end=pl.when(pl.col("next_start").is_not_null()).then(pl.col("next_start") - pl.duration(days=1)).otherwise(None),
                )
                .drop(["start", "next_start"])
            )
            return iv_cycle_ends

        def coalesce_cycle_ends(frame: pl.DataFrame) -> pl.DataFrame:
            """
            Coalesces IV and oral cycle end dates.
            """
            coalesced = frame.with_columns(oral_cycle_end=PolarsParsers.parse_date_column("TR_TROSTPDT").alias("oral_cycle_end")).with_columns(
                # conflict = both present
                end_date_conflict=(pl.col("oral_cycle_end").is_not_null() & pl.col("iv_cycle_end").is_not_null()),
                # mutually exclusive coalesced result; None if both or neither
                cycle_end=pl.when(pl.col("oral_cycle_end").is_not_null() & pl.col("iv_cycle_end").is_null())
                .then(pl.col("oral_cycle_end"))
                .when(pl.col("iv_cycle_end").is_not_null() & pl.col("oral_cycle_end").is_null())
                .then(pl.col("iv_cycle_end"))
                .otherwise(pl.lit(None, dtype=pl.Date)),
            )
            return coalesced

        def filter_parse_treatment_cycles(frame: pl.DataFrame) -> pl.DataFrame:
            filtered_data = frame.with_columns(
                cycle_start_date=PolarsParsers.parse_date_column(pl.col("TR_TRC1_DT")),
                recieved_treatment_this_cycle=PolarsParsers.int_to_bool(true_int=1, false_int=0, expr=pl.col("TR_TRCYNCD")),
                was_total_dose_delivered=PolarsParsers.yes_no_to_bool(pl.col("TR_TRIVDELYN1")),
                was_dose_administered_to_spec=PolarsParsers.int_to_bool(true_int=1, false_int=0, expr=pl.col("TR_TRO_YNCD")),
                was_tablet_taken_to_prescription_in_previous_cycle=PolarsParsers.int_to_bool(true_int=1, false_int=0, expr=pl.col("TR_TROTAKECD")),
            ).filter(pl.col("TR_TRNAME").is_not_null())

            return filtered_data

        treatment_typed = add_treatment_type(cycle_base)
        iv_cycle_end_dates = add_iv_cycle_stop_dates(treatment_typed)
        combined_end_dates = coalesce_cycle_ends(iv_cycle_end_dates)
        filtered = filter_parse_treatment_cycles(combined_end_dates)

        # pack to structs grouped by SubjectId
        packed = self.pack_structs(
            filtered,
            subject_col="SubjectId",
            value_cols=filtered.select(pl.all().exclude("SubjectId")).columns,
            order_by_cols=["TR_TRTNO", "TR_TRCNO1", "TR_TRC1_DT"],
        )

        # build TreatmentCycle objects
        def build_tc(pid: str, s: Mapping[str, Any]) -> TreatmentCycle:
            obj = TreatmentCycle(pid)
            # core
            obj.cycle_type = s["treatment_type"]
            obj.treatment_name = s["TR_TRNAME"]
            obj.treatment_number = s["TR_TRTNO"]
            obj.cycle_number = s["TR_TRCNO1"]
            obj.start_date = s["cycle_start_date"]
            obj.recieved_treatment_this_cycle = s["recieved_treatment_this_cycle"]
            obj.was_total_dose_delivered = s["was_total_dose_delivered"]
            obj.end_date = s["cycle_end"]

            # iv
            obj.iv_dose_prescribed = s["TR_TRIVDS1"]
            obj.iv_dose_prescribed_unit = s["TR_TRIVU1"]

            # oral
            obj.was_dose_administered_to_spec = s["was_dose_administered_to_spec"]
            obj.oral_dose_prescribed_per_day = s["TR_TRODSTOT"]
            obj.oral_dose_prescribed_unit = s["TR_TRODSU"]
            obj.was_tablet_taken_to_prescription_in_previous_cycle = s["was_tablet_taken_to_prescription_in_previous_cycle"]
            obj.reason_not_administered_to_spec = s["TR_TROREA"]
            obj.reason_tablet_not_taken = s["TR_TROSPE"]
            obj.number_of_days_tablet_not_taken = s["TR_TROTABNO"]

            return obj

        # hydrate to Patient class
        self.hydrate_list_field(
            packed,
            builder=build_tc,
            patients=self.patient_data,
            target_attr="treatment_cycles",
            skip_missing=False,
        )

    def _process_concomitant_medication(self) -> None:
        cm_base = self.data.select(
            "SubjectId",
            "CM_CMTRT",
            "CM_CMMHYNCD",
            "CM_CMAEYN",
            "CM_CMONGOCD",
            "CM_CMSTDAT",
            "CM_CMENDAT",
            "CM_CMSPID",
        )

        def filter_concomitant_data(frame: pl.DataFrame) -> pl.DataFrame:
            filtered_data = frame.with_columns(
                medication_name=PolarsParsers.null_if_na(pl.col("CM_CMTRT")).cast(pl.Utf8, strict=False).str.strip_chars(),
                medication_ongoing=PolarsParsers.int_to_bool(pl.col("CM_CMONGOCD")),
                was_taken_due_to_medical_history_event=PolarsParsers.int_to_bool(true_int=1, false_int=0, expr=pl.col("CM_CMMHYNCD")),
                was_taken_due_to_adverse_event=PolarsParsers.yes_no_to_bool(pl.col("CM_CMAEYN")),
                is_adverse_event_ongoing=PolarsParsers.int_to_bool(true_int=1, false_int=0, expr=pl.col("CM_CMONGOCD")),
                start_date=PolarsParsers.parse_date_column(pl.col("CM_CMSTDAT")),
                end_date=PolarsParsers.parse_date_column(pl.col("CM_CMENDAT")),
                sequence_id=pl.col("CM_CMSPID").cast(pl.Int16, strict=False),
            ).filter(pl.col("CM_CMTRT").is_not_null())

            return filtered_data

        filtered = filter_concomitant_data(cm_base)

        packed = self.pack_structs(
            filtered,
            subject_col="SubjectId",
            value_cols=filtered.select(pl.all().exclude("SubjectId")).columns,
            order_by_cols=["sequence_id", "start_date"],
        )

        def build_cm(pid: str, s: Mapping[str, Any]) -> ConcomitantMedication:
            obj = ConcomitantMedication(pid)
            obj.medication_name = s["medication_name"]
            obj.medication_ongoing = s["medication_ongoing"]
            obj.was_taken_due_to_medical_history_event = s["was_taken_due_to_medical_history_event"]
            obj.was_taken_due_to_adverse_event = s["was_taken_due_to_adverse_event"]
            obj.is_adverse_event_ongoing = s["is_adverse_event_ongoing"]
            obj.start_date = s["start_date"]
            obj.end_date = s["end_date"]
            obj.sequence_id = s["sequence_id"]
            return obj

        self.hydrate_list_field(
            packed,
            builder=build_cm,
            patients=self.patient_data,
            target_attr="concomitant_medications",
            skip_missing=False,
        )

    def _process_adverse_events(self) -> None:
        ae_base = self.data.select(
            "SubjectId",
            "AE_AECTCAET",
            "AE_AETOXGRECD",
            "AE_AEOUT",
            "AE_AESTDAT",
            "AE_AEENDAT",
            "AE_SAESTDAT",
            "AE_AEREL1",
            "AE_AEREL1CD",
            "AE_AETRT1",
            "AE_AEREL2",
            "AE_AEREL2CD",
            "AE_AETRT2",
            "AE_AESERCD",
            "AE_SAEEXP1CD",
            "AE_SAEEXP2CD",
            "FU_FUPDEDAT",
            "TR_TRNAME",
            "TR_TRTNO",
        ).filter(pl.col("AE_AECTCAET").str.strip_chars().is_not_null())

        def parse_events(frame: pl.DataFrame) -> pl.DataFrame:
            _parsed = frame.with_columns(
                start_date=PolarsParsers.parse_date_column(pl.col("AE_AESTDAT")),
                end_date=PolarsParsers.parse_date_column(pl.col("AE_AEENDAT")),
                serious_date=PolarsParsers.parse_date_column(pl.col("AE_SAESTDAT")),
                was_serious=PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=0,
                    expr=pl.col("AE_AESERCD").cast(pl.Int8, strict=False),
                ),
                ser_expected_treatment_1=PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    expr=pl.col("AE_SAEEXP1CD").cast(pl.Int8, strict=False),
                ),
                ser_expected_treatment_2=PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    expr=pl.col("AE_SAEEXP2CD").cast(pl.Int8, strict=False),
                ),
                ae_rel_code_1=pl.col("AE_AEREL1CD").cast(pl.Int8, strict=False),
                ae_rel_code_2=pl.col("AE_AEREL2CD").cast(pl.Int8, strict=False),
            ).with_columns(
                related_status_1=(
                    pl.when(pl.col("ae_rel_code_1") == 4)
                    .then(pl.lit("related"))
                    .when(pl.col("ae_rel_code_1") == 1)
                    .then(pl.lit("not_related"))
                    .when(pl.col("ae_rel_code_1").is_in([2, 3]))
                    .then(pl.lit("unknown"))
                    .otherwise(None)
                    .cast(pl.Enum(["related", "not_related", "unknown"]))
                ),
                related_status_2=(
                    pl.when(pl.col("ae_rel_code_2") == 4)
                    .then(pl.lit("related"))
                    .when(pl.col("ae_rel_code_2") == 1)
                    .then(pl.lit("not_related"))
                    .when(pl.col("ae_rel_code_2").is_in([2, 3]))
                    .then(pl.lit("unknown"))
                    .otherwise(None)
                    .cast(pl.Enum(["related", "not_related", "unknown"]))
                ),
            )
            return _parsed

        def locate_end_date_for_deceased(frame: pl.DataFrame) -> pl.DataFrame:
            end_date_frame = (
                frame.with_columns(death_date=PolarsParsers.parse_date_column(pl.col("FU_FUPDEDAT")))
                .with_columns(
                    end_date=pl.when(pl.col("end_date").is_null() & pl.col("was_serious").fill_null(False) & pl.col("death_date").is_not_null())
                    .then(pl.col("death_date"))
                    .otherwise(pl.col("end_date")),
                )
                .drop("death_date")
            )
            return end_date_frame

        parsed = parse_events(ae_base)
        annot = locate_end_date_for_deceased(parsed)
        packed = self.pack_structs(df=annot, value_cols=annot.select(pl.all().exclude("SubjectId")).columns)

        def build_ae(pid: str, s: Mapping[str, Any]) -> AdverseEvent:
            obj = AdverseEvent(pid)
            obj.term = s["AE_AECTCAET"]
            obj.grade = s["AE_AETOXGRECD"]
            obj.outcome = s["AE_AEOUT"]
            obj.was_serious = s["was_serious"]
            obj.turned_serious_date = s["serious_date"]
            obj.related_to_treatment_1_status = s["related_status_1"]
            obj.related_to_treatment_2_status = s["related_status_2"]
            obj.was_serious_grade_expected_treatment_1 = s["ser_expected_treatment_1"]
            obj.was_serious_grade_expected_treatment_2 = s["ser_expected_treatment_2"]
            obj.treatment_1_name = s["AE_AETRT1"]
            obj.treatment_2_name = s["AE_AETRT2"]
            obj.start_date = s["start_date"]
            obj.end_date = s["end_date"]
            return obj

        self.hydrate_list_field(
            patients=self.patient_data,
            packed=packed,
            builder=build_ae,
            skip_missing=False,
            target_attr="adverse_events",
        )

    def _process_baseline_tumor_assessment(self):
        """
        Get target lesion size at baseline, and off-target lesions.

        Assumes tumor assessments are always mutally exclusive, if not, this doesn't work and needs refactoring.

        Also, for the non-target lesions from RCNT each NTL is associated with a distinct date, whereas in RNTMNT NTLs
        this is not the case. We just want to track the number of NTLs at baseline, so for now using EventDate for
        both RNTMNT and RCNT, but this is always with a 2-5 week delay after the assessment date for each respective NTL (for some reason).
        In RNTMNT this is not the case, each NTL at baseline has an assessment date.
        Could coalesce and take first RCNT data across all lesions in the future if more detailed dates are needed?

        For the target lesions there are no separate entries for baseline evals, the earliest assessment with valid date + baseline size
        is selected.
        """

        base = self.data.select(
            [
                "SubjectId",
                # tumor assessment type
                "VI_VITUMA",
                "VI_VITUMA__2",
                "VI_EventDate",
                "VI_EventId",
                # baseline off-target lesions
                "RCNT_RCNTNOB",
                "RCNT_EventDate",
                "RCNT_EventId",
                "RNTMNT_RNTMNTNOB",
                "RNTMNT_RNTMNTNO",
                "RNTMNT_EventId",
                "RNTMNT_EventDate",
                # baseline target lesion size
                "RNRSP_TERNTBAS",
                "RNRSP_TERNAD",
                "RNRSP_EventDate",
                "RNRSP_EventId",
                "RA_RARECBAS",
                "RA_RARECNAD",
                "RA_EventDate",
                "RA_EventId",
            ],
        )

        def tumor_assessment(df: pl.DataFrame) -> pl.DataFrame:
            return (
                df.with_columns(
                    vi_value=pl.coalesce([pl.col("VI_VITUMA"), pl.col("VI_VITUMA__2")]),
                    vi_date=PolarsParsers.parse_date_column(pl.col("VI_EventDate")),
                )
                .with_columns(vi_ok=(pl.col("VI_EventId").eq("V00") & pl.col("vi_value").is_not_null()))
                .filter(pl.col("vi_ok"))
                .sort(["SubjectId"])
                .unique("SubjectId", keep="first")
                .select(
                    "SubjectId",
                    pl.col("vi_value").alias("tumor_assessment_vi"),
                    pl.col("vi_date").alias("assessment_date_vi"),
                )
            )

        # earliest V00 RCNT & RNTMNT row with value and date
        def off_target_lesions_baseline(df: pl.DataFrame) -> pl.DataFrame:
            return (
                df.with_columns(
                    rnt_ok=pl.col("RNTMNT_EventId") == "V00",
                    rcnt_ok=pl.col("RCNT_EventId") == "V00",
                )
                .with_columns(
                    rnt_num=pl.when(pl.col("rnt_ok"))
                    .then(pl.coalesce([pl.col("RNTMNT_RNTMNTNOB"), pl.col("RNTMNT_RNTMNTNO")]).cast(pl.Int64, strict=False))
                    .otherwise(None),
                    rnt_date=pl.when(pl.col("rnt_ok")).then(PolarsParsers.parse_date_column(pl.col("RNTMNT_EventDate"))).otherwise(None),
                    rcnt_num=pl.when(pl.col("rcnt_ok")).then(pl.col("RCNT_RCNTNOB").cast(pl.Int64, strict=False)).otherwise(None),
                    rcnt_date=pl.when(pl.col("rcnt_ok")).then(PolarsParsers.parse_date_column(pl.col("RCNT_EventDate"))).otherwise(None),
                )
                .with_columns(
                    num_candidate=pl.coalesce([pl.col("rcnt_num"), pl.col("rnt_num")]),
                    date_candidate=pl.coalesce([pl.col("rcnt_date"), pl.col("rnt_date")]),
                )
                .filter(pl.col("num_candidate").is_not_null())
                .sort(["SubjectId"])
                .unique("SubjectId", keep="first")
                .select(
                    "SubjectId",
                    pl.col("num_candidate").alias("off_target_lesions_number"),
                    pl.col("date_candidate").alias("off_target_lesion_size_measurment_date"),
                )
            )

        # earliest row with value and date across RNRSP & RA
        def target_lesions_baseline(df: pl.DataFrame) -> pl.DataFrame:
            return (
                df.with_columns(
                    rnrsp_size=pl.col("RNRSP_TERNTBAS").cast(pl.Int64, strict=False),
                    rnrsp_nadir=pl.col("RNRSP_TERNAD").cast(pl.Int64, strict=False),
                    ra_size=pl.col("RA_RARECBAS").cast(pl.Int64, strict=False),
                    ra_nadir=pl.col("RA_RARECNAD").cast(pl.Int64, strict=False),
                    rnrsp_date=PolarsParsers.parse_date_column(pl.col("RNRSP_EventDate")),
                    ra_date=PolarsParsers.parse_date_column(pl.col("RA_EventDate")),
                )
                .with_columns(
                    size_candidate=pl.coalesce([pl.col("rnrsp_size"), pl.col("ra_size")]),
                    nadir_candidate=pl.coalesce([pl.col("rnrsp_nadir"), pl.col("ra_nadir")]),
                    date_candidate=pl.coalesce([pl.col("rnrsp_date"), pl.col("ra_date")]),
                )
                .filter(pl.col("size_candidate").is_not_null())
                .sort(["SubjectId", "date_candidate"])
                .unique("SubjectId", keep="first")
                .select(
                    "SubjectId",
                    pl.col("date_candidate").alias("assessment_date"),
                    pl.col("size_candidate").alias("target_lesion_size"),
                    pl.col("nadir_candidate").alias("target_lesion_nadir"),
                    pl.when(pl.col("rnrsp_size").is_not_null()).then(pl.lit("RNRSP")).otherwise(pl.lit("RA")).alias("baseline_source"),
                )
            )

        ta = tumor_assessment(base)
        ntl = off_target_lesions_baseline(base)
        tl = target_lesions_baseline(base)

        # filter out rows with only None
        subjects_with_any = pl.concat(
            [
                ta.select("SubjectId"),
                ntl.select("SubjectId"),
                tl.select("SubjectId"),
            ],
        ).unique()

        # anchor join on subjects
        joined = subjects_with_any.join(ta, on="SubjectId", how="left").join(ntl, on="SubjectId", how="left").join(tl, on="SubjectId", how="left")

        def build_tumor_assessment_baseline(pid: str, row) -> TumorAssessmentBaseline:
            tab = TumorAssessmentBaseline(pid)
            tab.assessment_type = row["tumor_assessment_vi"]
            tab.assessment_date = row["assessment_date_vi"]
            tab.target_lesion_size = row["target_lesion_size"]
            tab.target_lesion_nadir = row["target_lesion_nadir"]
            tab.target_lesion_measurment_date = row["assessment_date"]
            tab.number_off_target_lesions = row["off_target_lesions_number"]
            tab.off_target_lesion_measurment_date = row["off_target_lesion_size_measurment_date"]
            return tab

        self.hydrate_singleton(
            joined,
            patients=self.patient_data,
            builder=build_tumor_assessment_baseline,
            target_attr="tumor_assessment_baseline",
        )

    def _process_tumor_assessments(self):
        base = self.data.select(
            "SubjectId",
            "RA_RAASSESS1",
            "RA_RAASSESS2",
            "RA_RABASECH",
            "RNRSP_TERNCFB",
            "RA_RARECCH",
            "RNRSP_TERNCFN",
            "RA_RANLBASECD",
            "RNRSP_RNRSPNLCD",
            "RA_EventDate",
            "RNRSP_EventDate",
            "RA_RATIMRES",
            "RNRSP_RNRSPCL",
            "RA_RAiMOD",
            "RA_RAPROGDT",
            "RA_RAiUNPDT",
            "RA_EventId",
            "RNRSP_EventId",
        )

        def process(frame: pl.DataFrame) -> pl.DataFrame:
            _processed = (
                frame.with_columns(
                    assessment_type=pl.when(pl.col("RA_RAASSESS2").is_not_null())
                    .then(pl.lit("irecist"))
                    # takes precendence over irecist, so if collision, overwrite
                    .when(pl.col("RA_RAASSESS1").is_not_null())
                    .then(pl.lit("recist"))
                    # infer from row since not separate variable
                    .when(pl.col("RNRSP_TERNCFB").is_not_null())
                    .then(pl.lit("rano"))
                    .otherwise(None),
                )
                .with_columns(
                    _tl_change_baseline=pl.coalesce([pl.col("RA_RABASECH"), pl.col("RNRSP_TERNCFB")]).cast(pl.Float64, strict=False),
                    _tl_change_nadir=pl.coalesce([pl.col("RA_RARECCH"), pl.col("RNRSP_TERNCFN")]).cast(pl.Float64, strict=False),
                    event_id=pl.coalesce([pl.col("RA_EventId"), pl.col("RNRSP_EventId")]).cast(pl.Utf8, strict=False),
                )
                .with_columns(
                    target_lesion_change_from_baseline=(pl.when(pl.col("_tl_change_baseline").is_null()))
                    .then(None)
                    .when(pl.col("_tl_change_baseline") == 0)
                    .then(0)
                    .otherwise(pl.col("_tl_change_baseline") / 100),
                    target_lesion_change_from_nadir=(pl.when(pl.col("_tl_change_nadir").is_null()))
                    .then(None)
                    .when(pl.col("_tl_change_nadir") == 0)
                    .then(0)
                    .otherwise(pl.col("_tl_change_nadir") / 100),
                )
                .with_columns(
                    new_lesions_after_baseline=PolarsParsers.int_to_bool(
                        expr=pl.coalesce([pl.col("RA_RANLBASECD"), pl.col("RNRSP_RNRSPNLCD")]).cast(pl.Int64, strict=False),
                        true_int=1,
                        false_int=0,
                    ),
                )
                .with_columns(date=PolarsParsers.parse_date_column(pl.coalesce([pl.col("RA_EventDate"), pl.col("RNRSP_EventDate")])))
                .with_columns(
                    recist_response=pl.col("RA_RATIMRES").str.strip_chars(),
                    irecist_response=pl.col("RA_RAiMOD").str.strip_chars(),
                    rano_response=pl.col("RNRSP_RNRSPCL").str.strip_chars(),
                    recist_progression_date=PolarsParsers.parse_date_column(pl.col("RA_RAPROGDT")),
                    irecist_progression_date=PolarsParsers.parse_date_column(pl.col("RA_RAiUNPDT")),
                )
                # keep only rows with real signal
                .with_columns(
                    has_any=pl.any_horizontal(
                        [
                            pl.col("assessment_type").str.len_bytes() > 0,
                            pl.col("target_lesion_change_from_baseline").is_not_null(),
                            pl.col("target_lesion_change_from_nadir").is_not_null(),
                            pl.col("new_lesions_after_baseline").is_not_null(),
                            pl.col("date").is_not_null(),
                            pl.col("recist_response").str.len_bytes() > 0,
                            pl.col("irecist_response").str.len_bytes() > 0,
                            pl.col("rano_response").str.len_bytes() > 0,
                            pl.col("recist_progression_date").is_not_null(),
                            pl.col("irecist_progression_date").is_not_null(),
                        ],
                    ),
                )
                .filter(pl.col("has_any"))
                .select(
                    "SubjectId",
                    "assessment_type",
                    "target_lesion_change_from_baseline",
                    "target_lesion_change_from_nadir",
                    "new_lesions_after_baseline",
                    "date",
                    "recist_response",
                    "irecist_response",
                    "rano_response",
                    "recist_progression_date",
                    "irecist_progression_date",
                    "event_id",
                )
            )

            return _processed

        processed = process(base)
        packed = self.pack_structs(
            df=processed,
            value_cols=processed.select(pl.all().exclude("SubjectId")).columns,
        )

        def build_ta(pid: str, s: Mapping[str, Any]) -> Optional[TumorAssessment]:
            obj = TumorAssessment(pid)
            obj.assessment_type = s["assessment_type"]
            obj.target_lesion_change_from_baseline = s["target_lesion_change_from_baseline"]
            obj.target_lesion_change_from_nadir = s["target_lesion_change_from_nadir"]
            obj.was_new_lesions_registered_after_baseline = s["new_lesions_after_baseline"]
            obj.date = s["date"]
            obj.recist_response = s["recist_response"]
            obj.irecist_response = s["irecist_response"]
            obj.rano_response = s["rano_response"]
            obj.recist_date_of_progression = s["recist_progression_date"]
            obj.irecist_date_of_progression = s["irecist_progression_date"]
            obj.event_id = s["event_id"]
            return obj

        self.hydrate_list_field(
            patients=self.patient_data,
            packed=packed,
            builder=build_ta,
            skip_missing=False,
            target_attr="tumor_assessments",
        )

    # TODO: refactor to not use regex later
    def _process_c30(self):
        question_text_re = re.compile(r"^(?:C30_)?C30_?Q([1-9]|[12]\d|30)$")
        question_code_re = re.compile(r"^(?:C30_)?C30_?Q([1-9]|[12]\d|30)CD$")

        base = self.data.select(
            pl.col(["SubjectId", "C30_EventName", "C30_EventDate"]),
            pl.selectors.matches(question_text_re.pattern),
            pl.selectors.matches(question_code_re.pattern),
        )

        def process_c30(frame: pl.DataFrame) -> pl.DataFrame:
            text_cols = [c for c in frame.columns if question_text_re.fullmatch(c)]
            code_cols = [c for c in frame.columns if question_code_re.fullmatch(c)]

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    event_name=pl.col("C30_EventName").cast(pl.Utf8, strict=False).str.strip_chars(),
                    date=PolarsParsers.parse_date_column(pl.col("C30_EventDate")),
                    *[pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars().alias(c) for c in text_cols],
                    *[pl.col(c).cast(pl.Int64, strict=False).alias(c) for c in code_cols],
                )
                .select(
                    "SubjectId",
                    "date",
                    "event_name",
                    *text_cols,
                    *code_cols,
                )
            )

            return out

        processed = process_c30(frame=base)
        value_cols = processed.select(pl.all().exclude("SubjectId")).columns

        packed = self.pack_structs(
            df=processed,
            subject_col="SubjectId",
            value_cols=value_cols,
            order_by_cols=["date"],
            items_col="items",
        )

        def build_c30(pid: str, s: Mapping[str, Any]) -> Optional[C30]:
            obj = C30(patient_id=pid)
            obj.date = s["date"]
            obj.event_name = s["event_name"]
            for k, v in s.items():
                m = question_text_re.search(k)
                if m:
                    qn = int(m.group(1))
                    setattr(obj, f"q{qn}", v)
                c = question_code_re.fullmatch(k)
                if c:
                    setattr(obj, f"q{int(c.group(1))}_code", v)
            return obj

        self.hydrate_list_field(
            patients=self.patient_data,
            packed=packed,
            builder=build_c30,
            skip_missing=False,
            target_attr="c30_list",
        )

    # TODO: refactor to not use regex later
    def _process_eq5d(self):
        question_col_re = re.compile(r"^EQ5D_EQ5D([1-5])$")
        question_code_re = re.compile(r"^(?:EQ5D_)?EQ5D([1-5])CD$")

        base = self.data.select(
            pl.col(["SubjectId", "EQ5D_EventName", "EQ5D_EQ5DVAS", "EQ5D_EventDate"]),
            pl.selectors.matches(question_col_re.pattern),
            pl.selectors.matches(question_code_re.pattern),
        )

        def process_eq5d(frame: pl.DataFrame) -> pl.DataFrame:
            text_cols = [c for c in frame.columns if question_col_re.fullmatch(c)]
            code_cols = [c for c in frame.columns if question_code_re.fullmatch(c)]

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    event_name=pl.col("EQ5D_EventName").cast(pl.Utf8, strict=False).str.strip_chars(),
                    date=PolarsParsers.parse_date_column(pl.col("EQ5D_EventDate")),
                    qol_metric=pl.col("EQ5D_EQ5DVAS").cast(pl.Int64, strict=False),
                    *[pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars().alias(c) for c in text_cols],
                    *[pl.col(c).cast(pl.Int64, strict=False).alias(c) for c in code_cols],
                )
                .select(
                    "SubjectId",
                    "date",
                    "event_name",
                    "qol_metric",
                    *text_cols,
                    *code_cols,
                )
            )

            return out

        processed = process_eq5d(frame=base)
        value_cols = processed.select(pl.all().exclude("SubjectId")).columns

        packed = self.pack_structs(
            df=processed,
            subject_col="SubjectId",
            value_cols=value_cols,
            order_by_cols=["date"],
            items_col="items",
        )

        def build_eq5d(pid: str, s: Mapping[str, Any]) -> Optional[EQ5D]:
            obj = EQ5D(patient_id=pid)
            obj.date = s["date"]
            obj.event_name = s["event_name"]
            obj.qol_metric = s["qol_metric"]
            for k, v in s.items():
                m = question_col_re.search(k)
                if m:
                    qn = int(m.group(1))
                    setattr(obj, f"q{qn}", v)
                c = question_code_re.fullmatch(k)
                if c:
                    setattr(obj, f"q{int(c.group(1))}_code", v)
            return obj

        self.hydrate_list_field(
            patients=self.patient_data,
            packed=packed,
            builder=build_eq5d,
            skip_missing=False,
            target_attr="eq5d_list",
        )

    def _process_best_overall_response(self):
        """
        Takes the lowest value of the response code across all tumor assessments for each patient,
        i.e. selects the best response across entire treatment duration.
        Also assumes tumor evaluations are mutually exclusive.
        Removes unconfirmed iRecist responses, and takes best response across Recist and iRecist when
        rows have both evaluations.
        """
        # TODO: make sure not evaluable status maps to evaluability field in Patient:
        #   'Not Evaluable (NE)', 'RA_RATIMRESCD': 96,
        base = self.data.select(
            "SubjectId",
            "RA_RATIMRES",
            "RA_RATIMRESCD",
            "RA_RAiMOD",
            "RA_RAiMODCD",
            "RA_EventDate",
            "RNRSP_RNRSPCL",
            "RNRSP_RNRSPCLCD",
            "RNRSP_EventDate",
        ).filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))

        def process(frame: pl.DataFrame) -> pl.DataFrame:
            result = (
                frame.with_columns(
                    [
                        # map irecist code to recist scale
                        pl.when(pl.col("RA_RAiMODCD").cast(pl.Int64, strict=False) == 4)
                        .then(None)
                        .when(pl.col("RA_RAiMODCD").cast(pl.Int64, strict=False) == 5)
                        .then(4)
                        .when(pl.col("RA_RAiMODCD").cast(pl.Int64, strict=False) == 6)
                        .then(96)
                        .otherwise(pl.col("RA_RAiMODCD").cast(pl.Int64, strict=False))
                        .alias("irecist_normalized_code"),
                    ],
                )
                .with_columns(
                    [
                        # choose best response between recist and irecist
                        pl.when(pl.col("RA_RATIMRESCD").is_not_null() & pl.col("irecist_normalized_code").is_not_null())
                        .then(
                            pl.when(pl.col("RA_RATIMRESCD") <= pl.col("irecist_normalized_code"))
                            .then(pl.struct(["RA_RATIMRES", "RA_RATIMRESCD"]))
                            .otherwise(pl.struct(["RA_RAiMOD", "irecist_normalized_code"])),
                        )
                        .when(pl.col("RA_RATIMRESCD").is_not_null())
                        .then(pl.struct(["RA_RATIMRES", "RA_RATIMRESCD"]))
                        .when(pl.col("irecist_normalized_code").is_not_null())
                        .then(pl.struct(["RA_RAiMOD", "irecist_normalized_code"]))
                        .otherwise(
                            pl.struct(
                                [
                                    pl.lit(None).alias("response_text"),
                                    pl.lit(None).alias("response_code"),
                                ],
                            ),
                        )
                        .alias("best_recist_response"),
                    ],
                )
                .with_columns(
                    recist_text=(
                        pl.col("best_recist_response")
                        .struct.field("RA_RATIMRES")
                        .fill_null(pl.col("best_recist_response").struct.field("RA_RAiMOD"))
                        .fill_null(pl.col("best_recist_response").struct.field("response_text"))
                    ),
                    recist_code=(
                        pl.col("best_recist_response")
                        .struct.field("RA_RATIMRESCD")
                        .fill_null(pl.col("best_recist_response").struct.field("irecist_normalized_code"))
                        .fill_null(pl.col("best_recist_response").struct.field("response_code"))
                    ),
                )
                .with_columns(
                    # coalesce with rano, parse final cols
                    best_response_text=pl.coalesce("recist_text", "RNRSP_RNRSPCL").cast(pl.Utf8, strict=False).str.strip_chars(),
                    best_response_code=pl.coalesce("recist_code", "RNRSP_RNRSPCLCD").cast(pl.Int64, strict=False),
                    best_response_date=PolarsParsers.parse_date_column(pl.coalesce("RA_EventDate", "RNRSP_EventDate")),
                )
                .filter(pl.col("best_response_code").is_not_null())
                .sort(["SubjectId", "best_response_code"])
                .group_by("SubjectId", maintain_order=True)
                .first()
                .select(
                    "SubjectId",
                    "best_response_text",
                    "best_response_code",
                    "best_response_date",
                )
            )

            return result

        processed = process(base)

        def build_best_overall_response(pid: str, row) -> BestOverallResponse:
            bor = BestOverallResponse(pid)
            bor.response = row["best_response_text"]
            bor.code = row["best_response_code"]
            bor.date = row["best_response_date"]
            return bor

        self.hydrate_singleton(
            processed,
            patients=self.patient_data,
            builder=build_best_overall_response,
            target_attr="best_overall_response",
        )

    def _process_clinical_benefit(self):
        """
        Clinical benefit at W16 (visit 3).
        Note: If patient has iRecist *and* Recist at same assessment, iRecist evaluation currently has prevalence.
        """
        timepoint = "V03"

        base = (
            self.data.select(
                "SubjectId",
                "RA_RATIMRESCD",
                "RA_RAiMODCD",
                "RNRSP_RNRSPCLCD",
                "RNRSP_EventId",
                "RA_EventId",
            )
            .filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
            .filter((pl.col("RA_EventId") == timepoint) | (pl.col("RNRSP_EventId") == timepoint))
            .with_columns(
                benefit_w16=pl.when(pl.col("RA_RATIMRESCD").cast(pl.Int64, strict=False).le(3))
                .then(True)
                .when(pl.col("RA_RAiMODCD").cast(pl.Int64, strict=False).le(3))
                .then(True)
                .when(pl.col("RNRSP_RNRSPCLCD").cast(pl.Int64, strict=False).le(3))
                .then(True)
                .otherwise(False),
            )
        )

        for row in base.iter_rows(named=True):
            patient_id = row["SubjectId"]
            self.patient_data[patient_id].has_clinical_benefit_at_week16 = bool(row["benefit_w16"])

    def _process_eot_reason(self):
        filtered = (
            self.data.select("SubjectId", "EOT_EOTREOT")
            .filter(pl.col("EOT_EOTREOT").is_not_null())
            .with_columns(eot_reason=pl.col("EOT_EOTREOT").cast(pl.Utf8, strict=False).str.strip_chars())
        )

        for row in filtered.iter_rows(named=True):
            patient_id = row["SubjectId"]
            self.patient_data[patient_id].end_of_treatment_reason = str(row["eot_reason"])

    def _process_eot_date(self):
        """
        Note: Docs mention progression date and EventDate as well,
        but all patients in EOT source has EOTDAT,
        EventDate is date recorded (so it's wrong) and progression date is tracked in TumorAssessment class.
        """
        filtered = (
            self.data.select("SubjectId", "EOT_EOTDAT")
            .filter(pl.col("EOT_EOTDAT").is_not_null())
            .with_columns(eot_date=PolarsParsers.parse_date_column(pl.col("EOT_EOTDAT")))
        )

        for row in filtered.iter_rows(named=True):
            patient_id = row["SubjectId"]
            self.patient_data[patient_id].end_of_treatment_date = row["eot_date"]
