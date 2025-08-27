# harmonization/impress.py
from collections import defaultdict
from typing import Optional, List, Dict
from warnings import deprecated
import polars as pl
import datetime as dt
from logging import getLogger
from deprecated import deprecated

from omop_etl.harmonization.parsing.coercion import TypeCoercion
from omop_etl.harmonization.parsing.core import CoreParsers, PolarsParsers
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer
from omop_etl.harmonization.datamodels import (
    HarmonizedData,
    Patient,
    TumorType,
    StudyDrugs,
    Biomarkers,
    FollowUp,
    Ecog,
    MedicalHistory,
    PreviousTreatments,
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
        self._process_medical_history()
        self._process_treatment_start_date()

        # flatten patient values
        patients = list(self.patient_data.values())

        for idx in range(len(patients)):
            print(f"Patient {idx}: {patients[idx]} \n")

        output = HarmonizedData(patients=patients, trial_id=self.trial_id)
        # print(f"Impress output: {output}")

        return output
        # medical_histories=self.medical_histories,
        # ecog_assessments=self.ecog_assessments,
        # previous_treatments=self.previous_treatments,
        # adverse_events=self.adverse_events,
        # response_assessments=self.response_assessments,
        # clinical_benefits=self.clinical_benefits,
        # quality_of_life_assessments=self.quality_of_life_assessments,

    # todo: add collapsing of multiple records per patient,
    #   just unsure how to handle these additional events in datamodel,
    #   what exact data is shared, and under what conditions do subjects get two seperate records?
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
        cohort_data = self.data.filter(pl.col("COH_COHORTNAME").is_not_null())

        for row in cohort_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            cohort_name = row["COH_COHORTNAME"]

            if patient_id in self.patient_data:
                self.patient_data[patient_id].cohort_name = cohort_name

    def _process_gender(self):
        gender_data = (self.data.select(["SubjectId", "DM_SEX"])).filter(
            pl.col("DM_SEX").is_not_null()
        )

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

    def _process_age(self):
        """Process and calculate age at treatment start and update patient object"""
        age_data = (
            self.data.lazy()
            .group_by("SubjectId")
            .agg(
                [
                    pl.col("DM_BRTHDAT").drop_nulls().first().alias("birth_date"),
                    pl.col("TR_TRC1_DT").drop_nulls().min().alias("first_treatment"),
                ]
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
            pl.col("COH_ICD10COD").is_not_null()
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

            tumor_type = TumorType(patient_id=patient_id)
            tumor_type.icd10_code = icd10_code
            tumor_type.icd10_description = icd10_description
            tumor_type.main_tumor_type = main_tumor_type
            tumor_type.main_tumor_type_code = main_tumor_type_code
            tumor_type.cohort_tumor_type = cohort_tumor_type
            tumor_type.other_tumor_type = other_tumor_type

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
            (pl.col("COH_COHALLO1").is_not_null())
            | (pl.col("COH_COHALLO1__2").is_not_null())
            | (pl.col("COH_COHALLO1__3").is_not_null())
            | (pl.col("COH_COHALLO2").is_not_null())
            | (pl.col("COH_COHALLO2__2").is_not_null())
            | (pl.col("COH_COHALLO2__3").is_not_null())
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

            study_drugs = StudyDrugs(patient_id=patient_id)
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
            (pl.col("COH_GENMUT1").is_not_null())
            | (pl.col("COH_GENMUT1CD").is_not_null())
            | (pl.col("COH_COHCTN").is_not_null())
            | (pl.col("COH_COHTMN").is_not_null())
        )

        for row in biomarker_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            biomarkers = Biomarkers(patient_id=patient_id)
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
                row["COH_COHTMN"]
            )
            biomarkers.cohort_target_name = TypeCoercion.to_optional_string(
                row["COH_COHCTN"]
            )

            self.patient_data[patient_id].biomarker = biomarkers

    def _process_date_of_death(self):
        death_data = self.data.select(
            "SubjectId", "EOS_DEATHDTC", "FU_FUPDEDAT"
        ).filter(
            (pl.col("EOS_DEATHDTC").is_not_null())
            | (pl.col("FU_FUPDEDAT").is_not_null())
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

            followup = FollowUp(patient_id=patient_id)
            followup.lost_to_followup = lost_to_followup_status
            followup.date_lost_to_followup = date_lost_to_followup

            self.patient_data[patient_id].lost_to_followup = followup

    def _process_evaluability(self):
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
                evaluability_data.select(
                    ["SubjectId", "TR_TRO_STDT", "TR_TROSTPDT", "TR_TRCYNCD"]
                )
                .with_columns(
                    start=pl.col("TR_TRO_STDT").str.strptime(pl.Date, strict=False),
                    stop=pl.col("TR_TROSTPDT").str.strptime(pl.Date, strict=False),
                    not_recieved_treatment_this_cycle=pl.col("TR_TRCYNCD") != 1,
                )
                .filter(~pl.col("not_recieved_treatment_this_cycle"))
                .with_columns(
                    treatment_duration=(
                        (pl.col("stop") - pl.col("start")).dt.total_days()
                    )
                )
                .group_by("SubjectId")
                .agg(
                    (pl.col("treatment_duration").fill_null(-1) >= 28)
                    .any()
                    .alias("oral_sufficient_treatment_length")
                )
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
                        pl.col(["TR_TRO_STDT", "TR_TROSTPDT"])
                        .cast(pl.Utf8, strict=False)
                        .str.len_bytes()
                        .fill_null(0)
                        > 0
                    ),
                    start=pl.col("TR_TRC1_DT")
                    .cast(pl.Utf8, strict=False)
                    .str.strptime(pl.Date, strict=False),
                    not_recieved_treatment_this_cycle=pl.col("TR_TRCYNCD") != 1,
                )
                .filter(
                    ~pl.col("oral_present")
                    & ~pl.col("not_recieved_treatment_this_cycle")
                )
                .drop_nulls("start")
                .sort(["SubjectId", "TR_TRTNO", "start"])
                # partitioned shift to make next start
                .with_columns(
                    pl.col("start")
                    .shift(-1)
                    .over(["SubjectId", "TR_TRTNO"])
                    .alias("next_start")
                )
                # compute gap days
                .with_columns(
                    (pl.col("next_start") - pl.col("start"))
                    .dt.total_days()
                    .alias("gap_days")
                )
                .group_by("SubjectId")
                .agg(
                    pl.col("gap_days")
                    .ge(21)
                    .fill_null(False)
                    .any()
                    .alias("iv_sufficient_treatment_length")
                )
            )

            return iv_sufficient_treatment_length

        @deprecated
        def eot_filter() -> pl.DataFrame:
            has_ended_treatment = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(pl.col(["EOT_EventDate"]).str.len_bytes() > 0)
                .any()
                .alias("has_clinical_assessment")
            )
            return has_ended_treatment

        @deprecated
        def tumor_assessment() -> pl.DataFrame:
            # need to add V04 filter
            has_tumor_assessment_week_4 = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(
                    pl.col(
                        [
                            "RA_EventDate",
                            "RNRSP_EventDate",
                            "RCNT_EventDate",
                            "RNTMNT_EventDate",
                        ]
                    ).str.len_bytes()
                    > 0
                )
                .any()
                .alias("has_tumor_assessment")
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
                .with_columns(
                    is_evaluable=(
                        pl.col("oral_sufficient_treatment_length")
                        | pl.col("iv_sufficient_treatment_length")
                    )
                )
            )

            return _merged_df

        # hydrate
        merged_evaluability: pl.DataFrame = _merge_evaluability()
        for row in merged_evaluability.iter_rows(named=True):
            patient_id = row["SubjectId"]
            self.patient_data[patient_id].evaluable_for_efficacy_analysis = bool(
                row["is_evaluable"]
            )

    def _process_ecog(self):
        """
        Parses dates with defaults, strips description data, casts to correct types.
        Only select one baseline ECOG event per patient, using latest available date.
        """

        ecog_base = self.data.select(
            "SubjectId", "ECOG_EventId", "ECOG_ECOGS", "ECOG_ECOGSCD", "ECOG_ECOGDAT"
        ).filter(
            pl.col("ECOG_EventId") == "V00"
        )  # filter in base to compare only baseline

        def parse_ecog_data(ecog_data: pl.DataFrame) -> pl.DataFrame:
            filtered_ecog_data = (
                ecog_data.with_columns(
                    date=PolarsParsers.parse_date_column(pl.col("ECOG_ECOGDAT")),
                    grade=pl.col("ECOG_ECOGSCD").cast(pl.Int64, strict=False),
                    _stripped_description=pl.col("ECOG_ECOGS")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars(),
                )
                .with_columns(
                    description=pl.when(
                        pl.col("_stripped_description")
                        .str.to_lowercase()
                        .is_in(PolarsParsers.NA_VALUES)
                    )
                    .then(None)
                    .otherwise(pl.col("_stripped_description"))
                )
                .select("SubjectId", "date", "description", "grade")
            )
            return filtered_ecog_data

        def select_latest_baseline(data: pl.DataFrame) -> pl.DataFrame:
            _latest = data.sort(["SubjectId", "date"]).group_by("SubjectId").tail(1)
            return _latest

        def filter_all_nulls(data: pl.DataFrame) -> pl.DataFrame:
            return data.with_columns(
                has_ecog=pl.any_horizontal(
                    pl.col(["description"]).is_not_null()
                ).fill_null(False)
            )

        def merge_ecog(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            return base.join(processed, on="SubjectId", how="left")

        # process
        parsed = parse_ecog_data(ecog_data=ecog_base)
        latest = select_latest_baseline(parsed)
        valid = filter_all_nulls(latest)
        joined = merge_ecog(base=ecog_base, processed=valid)
        labeled = filter_all_nulls(joined).select(
            "SubjectId", "date", "description", "grade", "has_ecog"
        )

        # and hydrate
        for pid, date, desc, grade, has_ecog in labeled.iter_rows():
            ecog = Ecog(patient_id=pid)
            ecog.date = date
            ecog.description = desc
            ecog.grade = int(grade) if grade is not None else None
            self.patient_data[pid].ecog = ecog

    def _process_medical_history(self):
        # TODO: unsure how to process, latest update is to omit this
        #   currently just converts types and validates

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
                term=(PolarsParsers.null_if_na(pl.col("MH_MHTERM")))
                .cast(pl.Utf8, strict=False)
                .str.strip_chars(),
                sequence_id=(pl.col("MH_MHSPID")).cast(pl.Int64, strict=False),
                start_date=PolarsParsers.parse_date_column(pl.col("MH_MHSTDAT")),
                end_date=PolarsParsers.parse_date_column(pl.col("MH_MHENDAT")),
                status=(PolarsParsers.null_if_na(pl.col("MH_MHONGO")))
                .cast(pl.Utf8, strict=False)
                .str.strip_chars(),
                status_code=(pl.col("MH_MHONGOCD")).cast(pl.Int64, strict=False),
            ).filter(pl.col("term").is_not_null())

            return filtered_data

        def merge_medical_history(
            base: pl.DataFrame, processed: pl.DataFrame
        ) -> pl.DataFrame:
            return base.join(processed, on="SubjectId", how="left")

        filtered = filter_medical_histories(mh_base)
        merged = merge_medical_history(base=mh_base, processed=filtered).select(
            "SubjectId",
            "term",
            "sequence_id",
            "start_date",
            "end_date",
            "status",
            "status_code",
        )

        # instantiate mh
        mh_instances: dict[str, list[MedicalHistory]] = defaultdict(list)
        for (
            pid,
            term,
            sequence_id,
            start_date,
            end_date,
            status,
            status_code,
        ) in merged.iter_rows():
            mh = MedicalHistory(patient_id=pid)
            mh.term = term
            mh.sequence_id = sequence_id
            mh.start_date = start_date
            mh.end_date = end_date
            mh.status = status
            mh.status_code = status_code
            mh_instances[pid].append(mh)

        # hydrate to patient
        for pid, ins in mh_instances.items():
            if ins:
                self.patient_data[pid].medical_history = ins
                print(f"mh instance {mh}")

    # this uses old approach: collect, process, instantiate with validation:
    def _process_previous_treatment_lines(self):
        treatment_lines_data = self.data.select(
            "SubjectId",
            "CT_CTTYPE",
            "CT_CTTYPECD",
            "CT_CTSPID",
            "CT_CTSTDAT",
            "CT_CTENDAT",
            "CT_CTTYPESP",
        ).filter((pl.col("CT_CTTYPE").is_not_null()))

        for row in treatment_lines_data.iter_rows(named=True):
            patient_id = row["SubjectId"]

            pt = PreviousTreatments(patient_id=patient_id)
            pt.treatment = TypeCoercion.to_optional_string(row["CT_CTTYPE"])
            pt.treatment_code = TypeCoercion.to_optional_int(row["CT_CTTYPECD"])
            pt.treatment_sequence_number = TypeCoercion.to_optional_int(
                row["CT_CTSPID"]
            )
            pt.start_date = CoreParsers.parse_date_flexible(row["CT_CTSTDAT"])
            pt.end_date = CoreParsers.parse_date_flexible(row["CT_CTENDAT"])
            pt.additional_treatment = TypeCoercion.to_optional_string(
                row["CT_CTTYPESP"]
            )

            self.patient_data[patient_id].previous_treatments = pt

    def _process_treatment_start_date(self):
        treatment_start_data = (
            self.data.lazy()
            .select(["SubjectId", "TR_TRTNO", "TR_TRNAME", "TR_TRC1_DT", "TR_TRCNO1"])
            .filter(pl.col("TR_TRNAME").is_not_null())
            .group_by("SubjectId")
            .agg(pl.col("TR_TRC1_DT").drop_nulls().min().alias("treatment_start_date"))
            .collect()
            .select(["SubjectId", "treatment_start_date"])
        )

        # todo: refactor to parse in polars later
        for row in treatment_start_data.iter_rows(named=True):
            patient_id = row["SubjectId"]
            treatment_start_date = CoreParsers.parse_date_flexible(
                row["treatment_start_date"]
            )
            self.patient_data[patient_id].treatment_start_date = treatment_start_date

    def _process_treatment_end(self):
        pass

    def _process_start_last_cycle(self):
        # either grab from self or calculate
        pass

    # todo: make new datamodel for each cycle
    """
    treatment_name: TR_TRNAME 
    cycle_type: 
        - IV or oral
        - use ID cols to define 
    treatment_number: TR_TRTNO 
    cycle_start_date: TR_TRC1_DT
    cycle_end_date:
        - use TR_TRSTDT & TR_TRSTPDT for oral
        - calculate for IV
        - if any dates missing, set as None 
    dose_delivered: 
    dose_delivered_unit
    dose_prescribed
    was_dose_delivered_this_cycle
    treatment_name
    study_drug_number
    cycle_number
    
    
    *Intravenous: TRIVDS1, TRIVU1, TRIVDELYN1, TRDSDEL1  (date: TRC1_DT), TRNAME                           
    *Oral: TRO_YN (CD), TRODSTOT, TRODSU(CD),TRO_STDT (date), TROSTPDT (date), TROTAKE (CD), TROTABNO, TROSPE 
    TROREA, TROREACD, TROOTH, TRODSU, TRODSUCD, TRODSUOT, TRO_STDT, TROSTPDT, TROTAKE, TROTAKECD, TROTABNO, TRNAME
    """

    def _process_concomitant_medication(self):
        treatment_lines_data = self.data.select(
            "SubjectId",
            "CT_CTTYPE",
            "CT_CTTYPECD",
            "CT_CTSPID",
            "CT_CTSTDAT",
            "CT_CTENDAT",
            "CT_CTTYPESP",
        ).filter((pl.col("CT_CTTYPE") is not None))

        # for row in treatment_lines_data.iter_rows(named=True):
        #     patient_id = row["SubjectId"]
        #     pass
        # pass
