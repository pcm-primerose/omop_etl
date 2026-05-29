import re
import polars as pl
from logging import getLogger

from omop_etl.harmonization.core.parsers import PolarsParsers
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer, scalar, singleton, collection
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.best_overall_response import BestOverallResponse
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.clinical_benefit import ClinicalBenefit
from omop_etl.harmonization.models.domain.cohort import Cohort
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.end_of_treatment import EndOfTreatment, TrialOutcomeStatus
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.harmonization.models.patient import Patient

log = getLogger(__name__)


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    def process(self) -> HarmonizedData:
        """Run harmonization and return HarmonizedData."""
        self.run()
        return HarmonizedData(
            patients=list(self.patient_data.values()),
            trial_id=self.trial_id,
        )

    def _create_patients(self) -> None:
        """Create Patient instances from unique SubjectIds."""
        patient_ids = self.data.select("SubjectId").unique().to_series().to_list()
        for pid in patient_ids:
            self.patient_data[pid] = Patient(trial_id=self.trial_id, patient_id=pid)

    @singleton(Cohort)
    def _process_cohort(self) -> pl.DataFrame | None:
        """
        Harmonized cohort singleton. Built from the cohort COH columns
        (not the aggregated COHORTNAME col, can't be safely parsed).

        - raw_name: COH_COHORTNAME
        - target_biomarker: COH_COHCTN, harmonized using the biomarker dictionary
        - cancer_type: COH_COHTT, harmonized using the tumor-type dictionary
        - drugs: non-null COH_COHALLO* cols allocations (not harmonized)
        - normalized_name: "{biomarker}/{cancer_type}/{drugs}" when both
          biomarker and cancer type resolve, else None
        """
        cols = Cohort.Fields
        drug_cols = [
            "COH_COHALLO1",
            "COH_COHALLO1__2",
            "COH_COHALLO1__3",
            "COH_COHALLO2",
            "COH_COHALLO2__2",
            "COH_COHALLO2__3",
        ]

        biomarker_map = self.cohort_lookups.biomarker
        cancer_type_map = self.cohort_lookups.cancer_type

        df = (
            self.data.select(
                "SubjectId",
                PolarsParsers.to_optional_date(pl.col("COH_EventDate")).alias("_event_date"),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHORTNAME")).str.strip_chars().alias(cols.RAW_NAME),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHCTN")).str.strip_chars().alias("_raw_biomarker"),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTT")).str.strip_chars().alias("_raw_cancer_type"),
                *[PolarsParsers.to_optional_utf8(pl.col(c)).str.strip_chars().alias(c) for c in drug_cols],
            )
            .filter(pl.col(cols.RAW_NAME).is_not_null())
            # one cohort per patient: keep the latest by COH event date
            .sort(["SubjectId", "_event_date"])
            .unique(subset=["SubjectId"], keep="last")
        )

        if df.is_empty():
            return None

        df = df.with_columns(
            pl.col("_raw_biomarker").str.to_lowercase().replace_strict(biomarker_map, default=None).alias(cols.TARGET_BIOMARKER),
            pl.col("_raw_cancer_type").str.to_lowercase().replace_strict(cancer_type_map, default=None).alias(cols.CANCER_TYPE),
            # drugs: collect non-null allocation columns into a list, verbatim
            pl.concat_list([pl.col(c) for c in drug_cols]).list.drop_nulls().alias(cols.DRUGS),
        )

        base = pl.concat_str([pl.col(cols.TARGET_BIOMARKER), pl.col(cols.CANCER_TYPE)], separator="/")
        normalized = (
            pl.when(pl.col(cols.TARGET_BIOMARKER).is_not_null() & pl.col(cols.CANCER_TYPE).is_not_null())
            .then(pl.when(pl.col(cols.DRUGS).list.len() > 0).then(pl.concat_str([base, pl.col(cols.DRUGS).list.join(" + ")], separator="/")).otherwise(base))
            .otherwise(None)
            .alias(cols.NORMALIZED_NAME)
        )
        df = df.with_columns(normalized)

        return df.select("SubjectId", cols.RAW_NAME, cols.NORMALIZED_NAME, cols.TARGET_BIOMARKER, cols.CANCER_TYPE, cols.DRUGS)

    @scalar()
    def _process_sex(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.SEX
        return (
            self.data.select(
                "SubjectId",
                (
                    pl.when(PolarsParsers.to_optional_utf8(pl.col("DM_SEX")).str.to_lowercase().is_in(["m", "male"]))
                    .then(pl.lit("male"))
                    .when(PolarsParsers.to_optional_utf8(pl.col("DM_SEX")).str.to_lowercase().is_in(["f", "female"]))
                    .then(pl.lit("female"))
                    .otherwise(None)
                ).alias(colname),
            )
            .filter(pl.col(colname).is_not_null())
            .unique(subset=["SubjectId"], keep="first")
        )

    @scalar()
    def _process_date_of_birth(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.DATE_OF_BIRTH
        return (
            self.data.group_by("SubjectId")
            .agg(pl.col("DM_BRTHDAT").drop_nulls().first().alias("birth_date"))
            .with_columns((PolarsParsers.to_optional_date(pl.col("birth_date"))).alias(colname))
        )

    @scalar()
    def _process_age(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.AGE
        return (
            self.data.group_by("SubjectId")
            .agg(
                [
                    pl.col("DM_BRTHDAT").drop_nulls().first().alias("birth_date"),
                    pl.col("TR_TRC1_DT").drop_nulls().max().alias("last_treatment"),
                ],
            )
            .with_columns(
                birth_date=(PolarsParsers.to_optional_date(pl.col("birth_date"))),
                last_treatment=(PolarsParsers.to_optional_date(pl.col("last_treatment"))),
            )
            .with_columns(
                ((pl.col("last_treatment") - pl.col("birth_date")).dt.total_days().cast(pl.Int64) / 365.25).cast(pl.Int64).alias(colname),
            )
        )

    @scalar()
    def _process_date_of_death(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.DATE_OF_DEATH
        death_df = (
            self.data.select(
                "SubjectId",
                eos=PolarsParsers.to_optional_date(pl.col("EOS_DEATHDTC")),
                fu=PolarsParsers.to_optional_date(pl.col("FU_FUPDEDAT")),
            )
            .with_columns(
                pl.max_horizontal("eos", "fu").alias(colname),
            )
            .group_by("SubjectId")
            .agg(pl.max(colname))
            .filter(pl.col(colname).is_not_null())
        )
        return death_df

    @scalar()
    def _process_has_any_adverse_events(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.HAS_ANY_ADVERSE_EVENTS
        ae_status = (
            self.data.with_columns(
                ae_text_present=PolarsParsers.to_optional_utf8("AE_AECTCAET").str.len_chars().fill_null(0) > 0,
                ae_date_present=PolarsParsers.to_optional_utf8("AE_AESTDAT").str.len_chars().fill_null(0) > 0,
                ae_grade_present=PolarsParsers.to_optional_utf8("AE_AETOXGRECD").str.len_chars().fill_null(0) > 0,
            )
            .with_columns(
                row_has_ae=pl.any_horizontal(
                    [
                        pl.col("ae_text_present"),
                        pl.col("ae_date_present"),
                        pl.col("ae_grade_present"),
                    ],
                ),
            )
            .group_by("SubjectId")
            .agg(pl.col("row_has_ae").any().alias(colname))
        )
        return ae_status

    @scalar()
    def _process_number_of_adverse_events(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.NUMBER_OF_ADVERSE_EVENTS
        ae_num = (
            self.data.with_columns(
                ae_num=pl.any_horizontal(
                    [
                        (PolarsParsers.to_optional_utf8(pl.col("AE_AECTCAET")).str.len_chars().fill_null(0) > 0),
                        (PolarsParsers.to_optional_utf8(pl.col("AE_AESTDAT")).str.len_chars().fill_null(0) > 0),
                        (PolarsParsers.to_optional_utf8(pl.col("AE_AETOXGRECD")).str.len_chars().fill_null(0)),
                    ],
                ),
            )
            .group_by("SubjectId")
            .agg(pl.col("ae_num").sum().alias(colname))
        )
        return ae_num

    @scalar()
    def _process_number_of_serious_adverse_events(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.NUMBER_OF_SERIOUS_ADVERSE_EVENTS
        sae_counts = (
            self.data.with_columns(
                is_serious=(PolarsParsers.to_optional_int64("AE_AESERCD") == 1).fill_null(False),
            )
            .group_by("SubjectId")
            .agg(pl.col("is_serious").sum().cast(pl.Int64).alias(colname))
        )
        return sae_counts

    @singleton(ClinicalBenefit)
    def _process_clinical_benefit(self) -> pl.DataFrame:
        """
        Clinical benefit at W16 at visit 3.
        Priority for the answer and its date: iRecist (RA_RAiMODCD) > Recist
        (RA_RATIMRESCD) > RNRSP_RNRSPCLCD. iRecist and Recist both date from
        RA_EventDate, RNRSP uses RNRSP_EventDate. When no source registers a
        benefit, the row is False and the date falls back to whichever V03
        date is available (coalesce RA_EventDate, RNRSP_EventDate). Collapsed
        to one row per SubjectId.
        """
        cols = ClinicalBenefit.Fields
        timepoint = "V03"

        benefit = (
            self.data.select(
                "SubjectId",
                "RA_RATIMRESCD",
                "RA_RAiMODCD",
                "RA_EventId",
                "RA_EventDate",
                "RNRSP_RNRSPCLCD",
                "RNRSP_EventId",
                "RNRSP_EventDate",
            )
            .filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
            .filter((pl.col("RA_EventId") == timepoint) | (pl.col("RNRSP_EventId") == timepoint))
            .with_columns(
                row_has_benefit=pl.when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).le(3))
                .then(True)
                .when(PolarsParsers.to_optional_int64(pl.col("RA_RATIMRESCD")).le(3))
                .then(True)
                .when(PolarsParsers.to_optional_int64(pl.col("RNRSP_RNRSPCLCD")).le(3))
                .then(True)
                .otherwise(False),
                row_date=pl.when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).le(3))
                .then(PolarsParsers.to_optional_date("RA_EventDate"))
                .when(PolarsParsers.to_optional_int64(pl.col("RA_RATIMRESCD")).le(3))
                .then(PolarsParsers.to_optional_date("RA_EventDate"))
                .when(PolarsParsers.to_optional_int64(pl.col("RNRSP_RNRSPCLCD")).le(3))
                .then(PolarsParsers.to_optional_date("RNRSP_EventDate"))
                .otherwise(
                    pl.coalesce(
                        PolarsParsers.to_optional_date("RA_EventDate"),
                        PolarsParsers.to_optional_date("RNRSP_EventDate"),
                    )
                ),
            )
            .group_by("SubjectId")
            .agg(
                pl.col("row_has_benefit").any().alias(cols.HAS_BENEFIT),
                pl.col("row_date").filter(pl.col("row_has_benefit")).first().alias("date_from_benefit"),
                pl.col("row_date").first().alias("date_fallback"),
            )
            .with_columns(
                pl.coalesce("date_from_benefit", "date_fallback").alias(cols.DATE),
                pl.lit(16, dtype=pl.Int64).alias(cols.WEEK),
            )
            .select("SubjectId", cols.WEEK, cols.HAS_BENEFIT, cols.DATE)
        )

        return benefit

    @scalar()
    def _process_evaluable_for_efficacy_analysis(self) -> pl.DataFrame | None:
        """
        Filtering criteria:
        Any patient having valid treatment for sufficient length (21 days IV, 28 days oral).
        For IV cycles, the cycle end is modeled as the day before the next cycles start.
        Inclusive length = next_start − start days. Length ≥ 21 qualifies.
        For oral cycles, length = stop − start days; ≥ 28 qualifies.

        For subjects with oral drugs, the start and end date per cycle is checked directly.
        If a subject has any cycle lasting 28 days or more they are marked as having sufficient treatment length

        For subjects without oral drugs, cycle stop date is set to start date of next cycle and needs to last 21 days or more.
        Note: this means subjects with just one cycle are marked as non-evaluable since cycle end cannot be determined.
        each cycle is grouped by treatment number, any treatment having a cycle with sufficient length marks subject as evaluable.
        assumes no malformed dates, because imputing would change the length.
        """
        colname = Patient.Scalars.EVALUABLE_FOR_EFFICACY_ANALYSIS
        evaluability_data = self.data.select(
            "SubjectId",
            "TR_TROSTPDT",
            "TR_TRO_STDT",
            "TR_TRTNO",
            "TR_TRC1_DT",
            "TR_TRCYNCD",
        )

        def oral_treatment_lengths() -> pl.DataFrame:
            oral_sufficient_treatment_length = (
                evaluability_data.select(["SubjectId", "TR_TRO_STDT", "TR_TROSTPDT", "TR_TRCYNCD"])
                .with_columns(
                    start=PolarsParsers.to_optional_utf8(pl.col("TR_TRO_STDT")).str.strptime(pl.Date, strict=False),
                    stop=PolarsParsers.to_optional_utf8(pl.col("TR_TROSTPDT")).str.strptime(pl.Date, strict=False),
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
                        PolarsParsers.to_optional_utf8(pl.col(["TR_TRO_STDT", "TR_TROSTPDT"])).str.len_bytes().fill_null(0) > 0,
                    ),
                    start=PolarsParsers.to_optional_utf8(pl.col("TR_TRC1_DT")).str.strptime(pl.Date, strict=False),
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

        def _merge_evaluability() -> pl.DataFrame:
            base = evaluability_data.select("SubjectId").unique()
            _merged_df: pl.DataFrame = (
                base.join(oral_treatment_lengths(), on="SubjectId", how="left")
                .join(iv_treatment_lengths(), on="SubjectId", how="left")
                .with_columns(
                    pl.col("oral_sufficient_treatment_length").fill_null(False),
                    pl.col("iv_sufficient_treatment_length").fill_null(False),
                )
                .with_columns((pl.col("oral_sufficient_treatment_length") | pl.col("iv_sufficient_treatment_length")).alias(colname))
            )

            return _merged_df

        return _merge_evaluability()

    @scalar()
    def _process_treatment_start_date(self) -> pl.DataFrame | None:
        colname = Patient.Scalars.TREATMENT_START_DATE
        treatment_start_data = (
            self.data.lazy()
            .select(["SubjectId", "TR_TRNAME", "TR_TRC1_DT"])
            .with_columns(
                PolarsParsers.to_optional_utf8(pl.col("TR_TRNAME")).str.strip_chars().alias("tr_name"),
                PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")).alias(colname),
            )
            # keep only real names: non-null & len > 0
            .filter(pl.col("tr_name").is_not_null() & (pl.col("tr_name").str.len_chars() > 0))
            .group_by("SubjectId")
            .agg(pl.col("treatment_start_date").drop_nulls().min().alias("treatment_start_date"))
            .collect()
            .select(["SubjectId", "treatment_start_date"])
        )

        return treatment_start_data

    @singleton(EndOfTreatment)
    def _process_end_of_treatment(self) -> pl.DataFrame:
        """
        Build the EndOfTreatment singleton per patient.

        Status (TrialOutcomeStatus enum):
        - COMPLETED when the EOT reason text matches IMPRESS's
          completion signal: "Normal completion according to
          cohort-specific manual" (case-insensitive, whitespace-stripped).
        - WITHDRAWN when any other non-empty reason is present.
        - None when no EOT reason is recorded.

        Reason: raw text from EOT_EOTREOT (whitespace-stripped).

        Date precedence:
        EOT_EOTDAT > latest valid TR_TROSTPDT (oral stop) > latest valid
        TR_TRC1_DT (IV start). A row is emitted as long as a date OR a
        reason is present: the singleton may have date=None when only a
        reason exists, or status=None when only a date is inferred from
        treatment cycles.
        """
        cols = EndOfTreatment.Fields
        completion_text = "normal completion according to cohort-specific manual"

        eot = (
            self.data.select(
                "SubjectId",
                "EOT_EOTREOT",
                "TR_TRCYNCD",
                "TR_TROSTPDT",
                "TR_TRC1_DT",
                "EOT_EOTDAT",
            )
            .with_columns(
                row_reason=PolarsParsers.to_optional_utf8(pl.col("EOT_EOTREOT")).str.strip_chars(),
                valid=PolarsParsers.to_optional_int64(pl.col("TR_TRCYNCD")).eq(1),
                eot_date=PolarsParsers.to_optional_date(pl.col("EOT_EOTDAT").cast(pl.Utf8)),
                oral_stop=PolarsParsers.to_optional_date(pl.col("TR_TROSTPDT").cast(pl.Utf8)),
                iv_start=PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT").cast(pl.Utf8)),
            )
            .with_columns(
                oral_stop_valid=pl.when(pl.col("valid")).then(pl.col("oral_stop")).otherwise(None),
                iv_start_valid=pl.when(pl.col("valid")).then(pl.col("iv_start")).otherwise(None),
            )
            .group_by("SubjectId")
            .agg(
                # last non-null reason per patient
                reason=pl.col("row_reason").drop_nulls().last(),
                last_eot=pl.col("eot_date").max(),
                last_oral=pl.col("oral_stop_valid").max(),
                last_iv=pl.col("iv_start_valid").max(),
            )
            .with_columns(
                pl.coalesce([pl.col("last_eot"), pl.col("last_oral"), pl.col("last_iv")]).alias(cols.DATE),
                pl.when(pl.col("reason").is_null())
                .then(pl.lit(None, dtype=pl.Utf8))
                .when(pl.col("reason").str.to_lowercase() == completion_text)
                .then(pl.lit(TrialOutcomeStatus.COMPLETED.value))
                .otherwise(pl.lit(TrialOutcomeStatus.WITHDRAWN.value))
                .alias(cols.STATUS),
            )
            .rename({"reason": cols.REASON})
            # skip patients with no EOT info: neither reason nor any date
            .filter(pl.col(cols.REASON).is_not_null() | pl.col(cols.DATE).is_not_null())
            .select("SubjectId", cols.STATUS, cols.REASON, cols.DATE)
        )

        return eot

    @scalar()
    def _process_treatment_start_last_cycle(self) -> pl.DataFrame | None:
        """
        Note: currently not filtering for valid cycles, just selecting latest treatment starts.
        Set enforce_valid=True if TR_TRCYNCD must be 1 (i.e. filtering for valid cycles only)
        """
        enforce_valid = False
        colname = Patient.Scalars.TREATMENT_START_LAST_CYCLE

        last_cycle_data = (
            self.data.select("SubjectId", "TR_TRC1_DT", "TR_TRCYNCD")
            .with_columns(
                cycle_start=PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")),
                valid=PolarsParsers.to_optional_int64(pl.col("TR_TRCYNCD")).eq(1),
            )
            # null-out only if enforce_valid and row invalid
            .with_columns(
                cycle_start=pl.when(pl.lit(enforce_valid) & ~pl.col("valid")).then(None).otherwise(pl.col("cycle_start")),
            )
            .group_by("SubjectId")
            .agg(pl.col("cycle_start").max().alias(colname))
        )

        return last_cycle_data

    @singleton(TumorType)
    def _process_tumor_type(self) -> pl.DataFrame:
        # COHTTYPE__3/CD is present in source, but has no data
        cols = TumorType.Fields
        df = (
            self.data.with_row_index("_row")
            .select(
                "_row",
                "SubjectId",
                PolarsParsers.to_optional_date(pl.col("COH_EventDate")).alias("event_date"),
                PolarsParsers.to_optional_utf8(pl.col("COH_ICD10COD")).str.strip_chars().alias(cols.ICD10_CODE),
                PolarsParsers.to_optional_utf8(pl.col("COH_ICD10DES")).str.strip_chars().alias(cols.ICD10_DESCRIPTION),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTT")).str.strip_chars().alias(cols.COHORT_TUMOR_TYPE),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTTOSP")).str.strip_chars().alias(cols.OTHER_TUMOR_TYPE),
                # main tumor-type
                t1=PolarsParsers.to_optional_utf8(pl.col("COH_COHTTYPE")).str.strip_chars(),
                t1cd=PolarsParsers.to_optional_int64(pl.col("COH_COHTTYPECD")),
                t2=PolarsParsers.to_optional_utf8(pl.col("COH_COHTTYPE__2")).str.strip_chars(),
                t2cd=PolarsParsers.to_optional_int64(pl.col("COH_COHTTYPE__2CD")),
            )
            # keep rows where any relevant field is populated
            .filter(
                pl.any_horizontal(
                    pl.col(cols.ICD10_CODE).is_not_null(),
                    pl.col(cols.COHORT_TUMOR_TYPE).is_not_null(),
                    pl.col("t1").is_not_null(),
                    pl.col("t2").is_not_null(),
                    pl.col(cols.OTHER_TUMOR_TYPE).is_not_null(),
                ),
            )
            # detect complete pairs per slot and collisions across slots
            .with_columns(
                t1_has=(pl.col("t1").is_not_null() & pl.col("t1cd").is_not_null()).cast(pl.Int8),
                t2_has=(pl.col("t2").is_not_null() & pl.col("t2cd").is_not_null()).cast(pl.Int8),
            )
            .with_columns(collisions=(pl.sum_horizontal(["t1_has", "t2_has"]) > 1))
            # pick first complete slot if no collision
            .with_columns(
                m_type_raw=pl.coalesce(
                    [
                        pl.when(pl.col("t1_has") == 1).then(pl.col("t1")),
                        pl.when(pl.col("t2_has") == 1).then(pl.col("t2")),
                    ],
                ),
                m_code_raw=pl.coalesce(
                    [
                        pl.when(pl.col("t1_has") == 1).then(pl.col("t1cd")),
                        pl.when(pl.col("t2_has") == 1).then(pl.col("t2cd")),
                    ],
                ),
            )
            .with_columns(
                pl.when(~pl.col("collisions")).then(pl.col("m_type_raw")).otherwise(None).alias(cols.MAIN_TUMOR_TYPE),
                pl.when(~pl.col("collisions")).then(pl.col("m_code_raw")).otherwise(None).alias(cols.MAIN_TUMOR_TYPE_CODE),
            )
            # last write wins per SubjectId
            .sort("_row")
            .rename({"event_date": cols.DATE})
            .unique(subset=["SubjectId"], keep="last")
            .select(
                "SubjectId",
                cols.ICD10_CODE,
                cols.ICD10_DESCRIPTION,
                cols.MAIN_TUMOR_TYPE,
                cols.MAIN_TUMOR_TYPE_CODE,
                cols.COHORT_TUMOR_TYPE,
                cols.OTHER_TUMOR_TYPE,
                cols.DATE,
            )
        )

        return df

    @singleton(StudyDrugs)
    def _process_study_drugs(self) -> pl.DataFrame:
        cols = StudyDrugs.Fields
        df = (
            self.data.with_row_index("_row")
            .select(
                "_row",
                "SubjectId",
                p1=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO1")).str.strip_chars(),
                p1cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO1CD")),
                p2=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO1__2")).str.strip_chars(),
                p2cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO1__2CD")),
                p3=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO1__3")).str.strip_chars(),
                p3cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO1__3CD")),
                s1=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO2")).str.strip_chars(),
                s1cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO2CD")),
                s2=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO2__2")).str.strip_chars(),
                s2cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO2__2CD")),
                s3=PolarsParsers.to_optional_utf8(pl.col("COH_COHALLO2__3")).str.strip_chars(),
                s3cd=PolarsParsers.to_optional_int64(pl.col("COH_COHALLO2__3CD")),
                date=PolarsParsers.to_optional_date(pl.col("COH_EventDate")),
            )
            # require at least one present
            .filter(
                pl.any_horizontal(
                    pl.col("p1").is_not_null(),
                    pl.col("p2").is_not_null(),
                    pl.col("p3").is_not_null(),
                    pl.col("s1").is_not_null(),
                    pl.col("s2").is_not_null(),
                    pl.col("s3").is_not_null(),
                ),
            )
            # more than one slot used within primary or secondary
            .with_columns(
                p1_has=pl.any_horizontal(pl.col("p1").is_not_null(), pl.col("p1cd").is_not_null()).cast(pl.Int8),
                p2_has=pl.any_horizontal(pl.col("p2").is_not_null(), pl.col("p2cd").is_not_null()).cast(pl.Int8),
                p3_has=pl.any_horizontal(pl.col("p3").is_not_null(), pl.col("p3cd").is_not_null()).cast(pl.Int8),
                s1_has=pl.any_horizontal(pl.col("s1").is_not_null(), pl.col("s1cd").is_not_null()).cast(pl.Int8),
                s2_has=pl.any_horizontal(pl.col("s2").is_not_null(), pl.col("s2cd").is_not_null()).cast(pl.Int8),
                s3_has=pl.any_horizontal(pl.col("s3").is_not_null(), pl.col("s3cd").is_not_null()).cast(pl.Int8),
            )
            .with_columns(
                primary_collision=(pl.sum_horizontal(["p1_has", "p2_has", "p3_has"]) > 1),
                secondary_collision=(pl.sum_horizontal(["s1_has", "s2_has", "s3_has"]) > 1),
            )
            # choose first non-null slot only if no collision
            .with_columns(
                pl.when(~pl.col("primary_collision"))
                .then(pl.coalesce([pl.col("p1"), pl.col("p2"), pl.col("p3")]))
                .otherwise(None)
                .alias(cols.PRIMARY_TREATMENT_DRUG),
                pl.when(~pl.col("primary_collision"))
                .then(pl.coalesce([pl.col("p1cd"), pl.col("p2cd"), pl.col("p3cd")]))
                .otherwise(None)
                .alias(cols.PRIMARY_TREATMENT_DRUG_CODE),
                pl.when(~pl.col("secondary_collision"))
                .then(pl.coalesce([pl.col("s1"), pl.col("s2"), pl.col("s3")]))
                .otherwise(None)
                .alias(cols.SECONDARY_TREATMENT_DRUG),
                pl.when(~pl.col("secondary_collision"))
                .then(pl.coalesce([pl.col("s1cd"), pl.col("s2cd"), pl.col("s3cd")]))
                .otherwise(None)
                .alias(cols.SECONDARY_TREATMENT_DRUG_CODE),
            )
            # drop colliding rows entirely
            .filter(~pl.col("primary_collision") & ~pl.col("secondary_collision"))
            # last write wins per SubjectId
            .sort("_row")
            .unique(subset=["SubjectId"], keep="last")
            .select(
                "SubjectId",
                cols.PRIMARY_TREATMENT_DRUG,
                cols.PRIMARY_TREATMENT_DRUG_CODE,
                cols.SECONDARY_TREATMENT_DRUG,
                cols.SECONDARY_TREATMENT_DRUG_CODE,
                cols.DATE,
            )
        )

        return df

    @singleton(Biomarkers)
    def _process_biomarkers(self) -> pl.DataFrame:
        cols = Biomarkers.Fields
        df = (
            self.data.select(
                "SubjectId",
                PolarsParsers.to_optional_date(pl.col("COH_EventDate")).alias("event_date"),
                PolarsParsers.to_optional_utf8(pl.col("COH_GENMUT1")).str.strip_chars().alias(cols.GENE_AND_MUTATION),
                PolarsParsers.to_optional_int64(pl.col("COH_GENMUT1CD")).alias(cols.GENE_AND_MUTATION_CODE),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHCTN")).str.strip_chars().alias(cols.COHORT_TARGET_NAME),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTMN")).str.strip_chars().alias(cols.COHORT_TARGET_MUTATION),
            )
            .filter(
                pl.any_horizontal(
                    pl.col(cols.GENE_AND_MUTATION).is_not_null(),
                    pl.col(cols.GENE_AND_MUTATION_CODE).is_not_null(),
                    pl.col(cols.COHORT_TARGET_NAME).is_not_null(),
                    pl.col(cols.COHORT_TARGET_MUTATION).is_not_null(),
                ),
            )
            # latest event per SubjectId
            .sort(["SubjectId", "event_date"])
            .rename({"event_date": cols.DATE})
            .unique(subset=["SubjectId"], keep="last")
            .select("SubjectId", cols.GENE_AND_MUTATION, cols.GENE_AND_MUTATION_CODE, cols.COHORT_TARGET_NAME, cols.COHORT_TARGET_MUTATION, cols.DATE)
        )

        return df

    @singleton(FollowUp)
    def _process_lost_to_followup(self) -> pl.DataFrame:
        cols = FollowUp.Fields
        lost_to_followup = (
            self.data.select("SubjectId", "FU_FUPSST", "FU_FUPALDAT")
            .with_columns(fu_status=PolarsParsers.to_optional_utf8("FU_FUPSST"))
            .with_columns(status_lc=pl.col("fu_status").str.to_lowercase())
            .with_columns(ltfu_row=(pl.col("status_lc").is_not_null() & ~pl.col("status_lc").is_in(["alive", "death"])))
            .with_columns(
                ltfu_date=pl.when(pl.col("ltfu_row")).then(PolarsParsers.to_optional_date("FU_FUPALDAT")).otherwise(None),
            )
            .group_by("SubjectId")
            .agg(
                pl.col("ltfu_row").any().alias(cols.LOST_TO_FOLLOWUP),
                pl.col("ltfu_date").max().alias(cols.DATE_LOST_TO_FOLLOWUP),
            )
        ).select("SubjectId", cols.LOST_TO_FOLLOWUP, cols.DATE_LOST_TO_FOLLOWUP)

        return lost_to_followup

    @singleton(EcogBaseline)
    def _process_ecog_baseline(self) -> pl.DataFrame:
        """
        Parses dates with defaults, strips description data, casts to correct types.
        Only select one baseline ECOG event per patient, using latest available date.
        """
        cols = EcogBaseline.Fields

        ecog_base = self.data.select("SubjectId", "ECOG_EventId", "ECOG_ECOGS", "ECOG_ECOGSCD", "ECOG_ECOGDAT").filter(
            pl.col("ECOG_EventId") == "V00",
        )

        def parse_ecog_data(ecog_data: pl.DataFrame) -> pl.DataFrame:
            filtered_ecog_data = ecog_data.with_columns(
                PolarsParsers.to_optional_date(pl.col("ECOG_ECOGDAT")).alias(cols.DATE),
                PolarsParsers.to_optional_int64(pl.col("ECOG_ECOGSCD")).alias(cols.GRADE),
                PolarsParsers.to_optional_utf8(pl.col("ECOG_ECOGS")).str.strip_chars().alias(cols.DESCRIPTION),
            ).select("SubjectId", cols.DATE, cols.DESCRIPTION, cols.GRADE)
            return filtered_ecog_data

        def select_latest_baseline(data: pl.DataFrame) -> pl.DataFrame:
            _latest = data.sort(["SubjectId", cols.DATE]).group_by("SubjectId").tail(1)
            return _latest

        def filter_all_nulls(data: pl.DataFrame) -> pl.DataFrame:
            return data.with_columns(has_ecog=pl.any_horizontal(pl.col([cols.DESCRIPTION]).is_not_null()).fill_null(False))

        def merge_ecog(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            return base.join(processed, on="SubjectId", how="left")

        # process
        parsed = parse_ecog_data(ecog_data=ecog_base)
        latest = select_latest_baseline(parsed)
        valid = filter_all_nulls(latest)
        joined = merge_ecog(base=ecog_base, processed=valid)
        labeled = filter_all_nulls(joined).select("SubjectId", cols.DATE, cols.DESCRIPTION, cols.GRADE)

        return labeled

    @collection(
        MedicalHistory,
        order_by=("start_date",),
        require_order_by=True,
    )
    def _process_medical_histories(self) -> pl.DataFrame | None:
        cols = MedicalHistory.Fields
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
                PolarsParsers.to_optional_utf8(pl.col("MH_MHTERM")).str.strip_chars().alias(cols.TERM),
                PolarsParsers.to_optional_int64(pl.col("MH_MHSPID")).alias(cols.SEQUENCE_ID),
                PolarsParsers.to_optional_date(pl.col("MH_MHSTDAT")).alias(cols.START_DATE),
                PolarsParsers.to_optional_date(pl.col("MH_MHENDAT")).alias(cols.END_DATE),
                PolarsParsers.to_optional_utf8(pl.col("MH_MHONGO")).str.strip_chars().alias(cols.STATUS),
                PolarsParsers.to_optional_int64(pl.col("MH_MHONGOCD")).alias(cols.STATUS_CODE),
            ).filter(pl.col(cols.TERM).is_not_null())

            return filtered_data

        def merge_medical_history(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col([cols.TERM, cols.SEQUENCE_ID, cols.START_DATE, cols.END_DATE, cols.STATUS, cols.STATUS_CODE]).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_medical_histories(mh_base)
        merged = merge_medical_history(base=mh_base, processed=filtered).select(
            "SubjectId", cols.TERM, cols.SEQUENCE_ID, cols.START_DATE, cols.END_DATE, cols.STATUS, cols.STATUS_CODE
        )

        return merged

    @collection(
        PreviousTreatment,
        order_by=("start_date",),
        require_order_by=True,
    )
    def _process_previous_treatments(self) -> pl.DataFrame | None:
        cols = PreviousTreatment.Fields
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
                PolarsParsers.to_optional_utf8(pl.col("CT_CTTYPE")).str.strip_chars().alias(cols.TREATMENT),
                PolarsParsers.to_optional_int64(pl.col("CT_CTTYPECD")).alias(cols.TREATMENT_CODE),
                PolarsParsers.to_optional_int64(pl.col("CT_CTSPID")).alias(cols.TREATMENT_SEQUENCE_NUMBER),
                PolarsParsers.to_optional_date(pl.col("CT_CTSTDAT")).alias(cols.START_DATE),
                PolarsParsers.to_optional_date(pl.col("CT_CTENDAT")).alias(cols.END_DATE),
                PolarsParsers.to_optional_utf8(pl.col("CT_CTTYPESP")).str.strip_chars().alias(cols.ADDITIONAL_TREATMENT),
            ).filter(pl.col(cols.TREATMENT).is_not_null())
            return filtered_data

        def merge_previous_treatments(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col(
                        [cols.TREATMENT, cols.TREATMENT_CODE, cols.TREATMENT_SEQUENCE_NUMBER, cols.START_DATE, cols.END_DATE, cols.ADDITIONAL_TREATMENT]
                    ).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_previous_treatments(ct_base)
        merged = merge_previous_treatments(base=ct_base, processed=filtered).select(
            "SubjectId", cols.TREATMENT, cols.TREATMENT_CODE, cols.TREATMENT_SEQUENCE_NUMBER, cols.START_DATE, cols.END_DATE, cols.ADDITIONAL_TREATMENT
        )
        return merged

    @collection(
        TreatmentCycleComponent,
        order_by=("start_date",),
        require_order_by=True,
    )
    def _process_treatment_cycle(self) -> pl.DataFrame | None:
        cols = TreatmentCycleComponent.Fields
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

        def add_cycle_type(frame: pl.DataFrame) -> pl.DataFrame:
            """
            If any bytes in any oral-only cols, set to `oral`, if any in iv-only cols, set to `iv` else `None`.
            """

            oral_only_cols = ["TR_TRO_YN", "TR_TRODSTOT", "TR_TRO_STDT", "TR_TROSTPDT"]
            iv_only_cols = ["TR_TRIVDS1", "TR_TRIVU1", "TR_TRIVDELYN1"]

            oral_cols = [c for c in oral_only_cols if c in frame.columns]
            iv_cols = [c for c in iv_only_cols if c in frame.columns]

            def row_has_any(_cols: list[str]) -> pl.Expr:
                if not _cols:
                    return pl.lit(False)

                return pl.any_horizontal(pl.col(_cols).cast(pl.Utf8).str.strip_chars().str.len_bytes().fill_null(0) > 0)

            has_oral = row_has_any(oral_cols)
            has_iv = row_has_any(iv_cols)

            return frame.with_columns(
                pl.when(has_oral)
                .then(pl.lit("oral"))
                .when(has_iv)
                .then(pl.lit("iv"))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias(cols.CYCLE_TYPE)
                .str.to_lowercase(),
            )

        def add_iv_cycle_stop_dates(frame: pl.DataFrame) -> pl.DataFrame:
            """
            For IV cycles, selects next cycle start date - 1 day as current cycle end, set to `None` for last cycle.
            """
            iv_cycle_ends = (
                frame.with_columns(start=PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")))
                .sort(["SubjectId", "TR_TRTNO", "start"])
                .with_columns(
                    # apply shift to IV rows, others get None
                    next_start=pl.when(pl.col(cols.CYCLE_TYPE) == "iv").then(pl.col("start").shift(-1).over(["SubjectId", "TR_TRTNO"])).otherwise(None),
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
            coalesced = frame.with_columns(oral_cycle_end=PolarsParsers.to_optional_date("TR_TROSTPDT").alias("oral_cycle_end")).with_columns(
                # conflict = both present
                (pl.col("oral_cycle_end").is_not_null() & pl.col("iv_cycle_end").is_not_null()).alias("end_date_conflict"),
                # mutually exclusive coalesced result; None if both or neither
                pl.when(pl.col("oral_cycle_end").is_not_null() & pl.col("iv_cycle_end").is_null())
                .then(pl.col("oral_cycle_end"))
                .when(pl.col("iv_cycle_end").is_not_null() & pl.col("oral_cycle_end").is_null())
                .then(pl.col("iv_cycle_end"))
                .otherwise(pl.lit(None, dtype=pl.Date))
                .alias(cols.END_DATE),
            )
            return coalesced

        def filter_parse_treatment_cycles(frame: pl.DataFrame) -> pl.DataFrame:
            filtered_data = frame.with_columns(
                PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")).alias(cols.START_DATE),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TRCYNCD")).alias(cols.RECIEVED_TREATMENT_THIS_CYCLE),
                PolarsParsers.to_optional_bool(pl.col("TR_TRIVDELYN1")).alias(cols.WAS_TOTAL_DOSE_DELIVERED),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TRO_YNCD")).alias(cols.WAS_DOSE_ADMINISTERED_TO_SPEC),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TROTAKECD")).alias(cols.WAS_TABLET_TAKEN_TO_PRESCRIPTION_IN_PREVIOUS_CYCLE),
            ).filter(pl.col("TR_TRNAME").is_not_null())

            return filtered_data

        def coerce_types(frame: pl.DataFrame) -> pl.DataFrame:
            """Cast non-processed cols"""
            _coerced = frame.with_columns(
                pl.col("TR_TRNAME").cast(pl.Utf8).alias(cols.SOURCE_TREATMENT_NAME),
                pl.col("TR_TRTNO").cast(pl.Int64).alias(cols.TREATMENT_NUMBER),
                pl.col("TR_TRCNO1").cast(pl.Int64).alias(cols.CYCLE_NUMBER),
                pl.col("TR_TRIVDS1").cast(pl.Utf8).alias(cols.IV_DOSE_PRESCRIBED),
                pl.col("TR_TRIVU1").cast(pl.Utf8).alias(cols.IV_DOSE_PRESCRIBED_UNIT),
                pl.col("TR_TRODSTOT").cast(pl.Float64).alias(cols.ORAL_DOSE_PRESCRIBED_PER_DAY),
                pl.col("TR_TRODSU").cast(pl.Utf8).alias(cols.ORAL_DOSE_UNIT),
                pl.col("TR_TROREA").cast(pl.Utf8).alias(cols.REASON_NOT_ADMINISTERED_TO_SPEC),
                pl.col("TR_TROSPE").cast(pl.Utf8).alias(cols.REASON_TABLET_NOT_TAKEN),
                pl.col("TR_TROTABNO").cast(pl.Int64).alias(cols.NUMBER_OF_DAYS_TABLET_NOT_TAKEN),
            )

            return _coerced

        def parse_treatment_names(frame: pl.DataFrame) -> pl.DataFrame:
            """
            Parse treatment names into source_treatment_name, ingredient_name, and brand_name.
            Split combination drugs into per-ingredient rows with individual doses.

            source_treatment_name is always the raw source value (invariant).
            ingredient_name and brand_name are only populated when parseable from parens-data.

            Handles three formats:
            - "Brand (Ing1 and Ing2)" + "dose1/dose2": 2 rows, component_index 0/1
            - "Brand (Ingredient)": 1 row, brand + ingredient extracted
            - "Plain Name": 1 row, ingredient_name=None, brand_name=None
            """
            has_parens = pl.col(cols.SOURCE_TREATMENT_NAME).str.contains(r"\(")
            is_combo = pl.col(cols.SOURCE_TREATMENT_NAME).str.contains(r"\(.*\band\b.*\)") & pl.col(cols.IV_DOSE_PRESCRIBED).cast(
                pl.Utf8, strict=False
            ).str.contains("/")

            combo = frame.filter(is_combo)
            non_combo = frame.filter(~is_combo)

            # non-combination rows retained as one row
            non_combo = non_combo.with_columns(
                pl.when(has_parens)
                .then(pl.col(cols.SOURCE_TREATMENT_NAME).str.extract(r"^(.+?)\s*\(", 1).str.strip_chars())
                .otherwise(None)
                .alias(cols.BRAND_NAME),
                pl.when(has_parens)
                .then(pl.col(cols.SOURCE_TREATMENT_NAME).str.extract(r"\((.+)\)", 1).str.strip_chars())
                .otherwise(None)
                .alias(cols.INGREDIENT_NAME),
                pl.lit(None, dtype=pl.Int64).alias(cols.COMPONENT_INDEX),
                pl.col(cols.IV_DOSE_PRESCRIBED).cast(pl.Float64, strict=False),
            )

            if combo.height == 0:
                return non_combo

            # combination IV rows parsed to separate rows
            split = (
                combo.with_columns(
                    pl.col(cols.SOURCE_TREATMENT_NAME).str.extract(r"^(.+?)\s*\(", 1).str.strip_chars().alias(cols.BRAND_NAME),
                    pl.col(cols.SOURCE_TREATMENT_NAME).str.extract(r"\((.+)\)", 1).str.split(" and ").alias("_ingredients"),
                    pl.col(cols.IV_DOSE_PRESCRIBED).cast(pl.Utf8).str.split("/").alias("_doses"),
                )
                .with_columns(
                    pl.col("_ingredients").list.len().alias("_n_ingredients"),
                )
                .with_columns(
                    pl.when(pl.col("_n_ingredients") == pl.col("_doses").list.len())
                    .then(pl.int_ranges(pl.lit(0), pl.col("_n_ingredients")))
                    .otherwise(pl.lit(None))
                    .alias("_component_indices"),
                )
                .explode("_ingredients", "_doses", "_component_indices")
                .with_columns(
                    pl.col("_ingredients").str.strip_chars().alias(cols.INGREDIENT_NAME),
                    pl.col("_doses").cast(pl.Float64, strict=False).alias(cols.IV_DOSE_PRESCRIBED),
                    pl.col("_component_indices").cast(pl.Int64).alias(cols.COMPONENT_INDEX),
                )
                .drop("_ingredients", "_doses", "_component_indices", "_n_ingredients")
            )

            return pl.concat([non_combo, split], how="align")

        coerced = coerce_types(cycle_base)
        cycle_typed = add_cycle_type(coerced)
        iv_cycle_end_dates = add_iv_cycle_stop_dates(cycle_typed)
        combined_end_dates = coalesce_cycle_ends(iv_cycle_end_dates)
        filtered = filter_parse_treatment_cycles(combined_end_dates)
        parsed = parse_treatment_names(filtered)

        return parsed.select(
            "SubjectId",
            cols.SOURCE_TREATMENT_NAME,
            cols.INGREDIENT_NAME,
            cols.BRAND_NAME,
            cols.COMPONENT_INDEX,
            cols.CYCLE_TYPE,
            cols.TREATMENT_NUMBER,
            cols.CYCLE_NUMBER,
            cols.START_DATE,
            cols.END_DATE,
            cols.RECIEVED_TREATMENT_THIS_CYCLE,
            cols.WAS_TOTAL_DOSE_DELIVERED,
            cols.IV_DOSE_PRESCRIBED,
            cols.IV_DOSE_PRESCRIBED_UNIT,
            cols.WAS_DOSE_ADMINISTERED_TO_SPEC,
            cols.REASON_NOT_ADMINISTERED_TO_SPEC,
            cols.ORAL_DOSE_PRESCRIBED_PER_DAY,
            cols.ORAL_DOSE_UNIT,
            cols.NUMBER_OF_DAYS_TABLET_NOT_TAKEN,
            cols.REASON_TABLET_NOT_TAKEN,
            cols.WAS_TABLET_TAKEN_TO_PRESCRIPTION_IN_PREVIOUS_CYCLE,
        )

    @collection(
        ConcomitantMedication,
        order_by=("sequence_id", "start_date"),
        require_order_by=True,
    )
    def _process_concomitant_medication(self) -> pl.DataFrame | None:
        cols = ConcomitantMedication.Fields
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
                PolarsParsers.to_optional_utf8(pl.col("CM_CMTRT")).cast(pl.Utf8, strict=False).str.strip_chars().alias(cols.MEDICATION_NAME),
                PolarsParsers.int_to_bool(pl.col("CM_CMONGOCD")).alias(cols.MEDICATION_ONGOING),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("CM_CMMHYNCD")).alias(cols.WAS_TAKEN_DUE_TO_MEDICAL_HISTORY_EVENT),
                PolarsParsers.to_optional_bool(pl.col("CM_CMAEYN")).alias(cols.WAS_TAKEN_DUE_TO_ADVERSE_EVENT),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("CM_CMONGOCD")).alias(cols.IS_ADVERSE_EVENT_ONGOING),
                PolarsParsers.to_optional_date(pl.col("CM_CMSTDAT")).alias(cols.START_DATE),
                PolarsParsers.to_optional_date(pl.col("CM_CMENDAT")).alias(cols.END_DATE),
                PolarsParsers.to_optional_int64(pl.col("CM_CMSPID")).alias(cols.SEQUENCE_ID),
            ).filter(pl.col(cols.MEDICATION_NAME).is_not_null())

            return filtered_data

        filtered = filter_concomitant_data(cm_base).select(
            "SubjectId",
            cols.MEDICATION_NAME,
            cols.MEDICATION_ONGOING,
            cols.WAS_TAKEN_DUE_TO_MEDICAL_HISTORY_EVENT,
            cols.WAS_TAKEN_DUE_TO_ADVERSE_EVENT,
            cols.IS_ADVERSE_EVENT_ONGOING,
            cols.START_DATE,
            cols.END_DATE,
            cols.SEQUENCE_ID,
        )

        return filtered

    @collection(
        AdverseEvent,
        order_by=("start_date", "sequence_id"),
        require_order_by=True,
    )
    def _process_adverse_events(self) -> pl.DataFrame | None:
        cols = AdverseEvent.Fields
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
            "AE_AESPID",
            "FU_FUPDEDAT",
            "TR_TRNAME",
            "TR_TRTNO",
        ).filter(pl.col("AE_AECTCAET").str.strip_chars().is_not_null())

        def parse_events(frame: pl.DataFrame) -> pl.DataFrame:
            _parsed = frame.with_columns(
                PolarsParsers.to_optional_date(pl.col("AE_AESTDAT")).alias(cols.START_DATE),
                PolarsParsers.to_optional_date(pl.col("AE_AEENDAT")).alias(cols.END_DATE),
                PolarsParsers.to_optional_date(pl.col("AE_SAESTDAT")).alias(cols.TURNED_SERIOUS_DATE),
                PolarsParsers.to_optional_int64(pl.col("AE_AESPID")).alias(cols.SEQUENCE_ID),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=0,
                    x=pl.col("AE_AESERCD").cast(pl.Int8, strict=False),
                ).alias(cols.WAS_SERIOUS),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    x=pl.col("AE_SAEEXP1CD").cast(pl.Int8, strict=False),
                ).alias(cols.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_1),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    x=pl.col("AE_SAEEXP2CD").cast(pl.Int8, strict=False),
                ).alias(cols.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_2),
                ae_rel_code_1=PolarsParsers.to_optional_int64(pl.col("AE_AEREL1CD")),
                ae_rel_code_2=PolarsParsers.to_optional_int64(pl.col("AE_AEREL2CD")),
            ).with_columns(
                (
                    pl.when(pl.col("ae_rel_code_1") == 4)
                    .then(pl.lit("related"))
                    .when(pl.col("ae_rel_code_1") == 1)
                    .then(pl.lit("not_related"))
                    .when(pl.col("ae_rel_code_1").is_in([2, 3]))
                    .then(pl.lit("unknown"))
                    .otherwise(None)
                    .cast(pl.Enum(["related", "not_related", "unknown"]))
                ).alias(cols.RELATED_TO_TREATMENT_1_STATUS),
                (
                    pl.when(pl.col("ae_rel_code_2") == 4)
                    .then(pl.lit("related"))
                    .when(pl.col("ae_rel_code_2") == 1)
                    .then(pl.lit("not_related"))
                    .when(pl.col("ae_rel_code_2").is_in([2, 3]))
                    .then(pl.lit("unknown"))
                    .otherwise(None)
                    .cast(pl.Enum(["related", "not_related", "unknown"]))
                ).alias(cols.RELATED_TO_TREATMENT_2_STATUS),
            )
            return _parsed

        def locate_end_date_for_deceased(frame: pl.DataFrame) -> pl.DataFrame:
            end_date_frame = (
                frame.with_columns(death_date=PolarsParsers.to_optional_date(pl.col("FU_FUPDEDAT")))
                .with_columns(
                    pl.when(pl.col(cols.END_DATE).is_null() & pl.col(cols.WAS_SERIOUS).fill_null(False) & pl.col("death_date").is_not_null())
                    .then(pl.col("death_date"))
                    .otherwise(pl.col(cols.END_DATE))
                    .alias(cols.END_DATE),
                )
                .drop("death_date")
            )
            return end_date_frame

        def coerce(frame: pl.DataFrame) -> pl.DataFrame:
            return frame.with_columns(
                pl.col("AE_AECTCAET").cast(pl.Utf8).alias(cols.TERM),
                pl.col("AE_AETOXGRECD").cast(pl.Int64).alias(cols.GRADE),
                pl.col("AE_AEOUT").cast(pl.Utf8).alias(cols.OUTCOME),
                pl.col("AE_AETRT1").cast(pl.Utf8).alias(cols.TREATMENT_1_NAME),
                pl.col("AE_AETRT2").cast(pl.Utf8).alias(cols.TREATMENT_2_NAME),
            )

        parsed = parse_events(ae_base)
        annot = locate_end_date_for_deceased(parsed)
        coerced = (
            coerce(annot)
            .filter(pl.col(cols.TERM).is_not_null())
            .select(
                "SubjectId",
                cols.TERM,
                cols.GRADE,
                cols.OUTCOME,
                cols.START_DATE,
                cols.END_DATE,
                cols.WAS_SERIOUS,
                cols.TURNED_SERIOUS_DATE,
                cols.RELATED_TO_TREATMENT_1_STATUS,
                cols.TREATMENT_1_NAME,
                cols.RELATED_TO_TREATMENT_2_STATUS,
                cols.TREATMENT_2_NAME,
                cols.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_1,
                cols.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_2,
                cols.SEQUENCE_ID,
            )
        )

        return None if coerced.is_empty() else coerced

    def _subject_baseline_target_lesion_size(self) -> pl.DataFrame:
        """Per-subject baseline target lesion size in mm.

        Earliest non-null baseline size across RNRSP_TERNTBAS / RA_RARECBAS per subject,
        tie-broken by EventDate. Returns (SubjectId, baseline_size: Int64).
        """
        return (
            self.data.select("SubjectId", "RNRSP_TERNTBAS", "RA_RARECBAS", "RNRSP_EventDate", "RA_EventDate")
            .with_columns(
                _size=pl.coalesce(
                    [
                        PolarsParsers.to_optional_int64(pl.col("RNRSP_TERNTBAS")),
                        PolarsParsers.to_optional_int64(pl.col("RA_RARECBAS")),
                    ],
                ),
                _date=pl.coalesce(
                    [
                        PolarsParsers.to_optional_date(pl.col("RNRSP_EventDate")),
                        PolarsParsers.to_optional_date(pl.col("RA_EventDate")),
                    ],
                ),
            )
            .filter(pl.col("_size").is_not_null())
            .sort(["SubjectId", "_date"])
            .unique("SubjectId", keep="first")
            .select("SubjectId", pl.col("_size").alias("baseline_size"))
        )

    @singleton(TumorAssessmentBaseline)
    def _process_baseline_tumor_assessment(self) -> pl.DataFrame | None:
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
        cols = TumorAssessmentBaseline.Fields

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
                    vi_date=PolarsParsers.to_optional_date(pl.col("VI_EventDate")),
                )
                .with_columns(vi_ok=(pl.col("VI_EventId").eq("V00") & pl.col("vi_value").is_not_null()))
                .filter(pl.col("vi_ok"))
                .sort(["SubjectId"])
                .unique("SubjectId", keep="first")
                .select(
                    "SubjectId",
                    pl.col("vi_value").alias(cols.ASSESSMENT_TYPE),
                    pl.col("vi_date").alias(cols.ASSESSMENT_DATE),
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
                    rnt_date=pl.when(pl.col("rnt_ok")).then(PolarsParsers.to_optional_date(pl.col("RNTMNT_EventDate"))).otherwise(None),
                    rcnt_num=pl.when(pl.col("rcnt_ok")).then(pl.col("RCNT_RCNTNOB").cast(pl.Int64, strict=False)).otherwise(None),
                    rcnt_date=pl.when(pl.col("rcnt_ok")).then(PolarsParsers.to_optional_date(pl.col("RCNT_EventDate"))).otherwise(None),
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
                    pl.col("num_candidate").alias(cols.OFF_TARGET_LESIONS_NUMBER),
                    pl.col("date_candidate").alias(cols.OFF_TARGET_LESION_MEASUREMENT_DATE),
                )
            )

        # earliest row with value and date across RNRSP & RA; size sourced from shared helper
        # so _process_tumor_assessments sees the exact same per-subject baseline value.
        def target_lesions_baseline(df: pl.DataFrame) -> pl.DataFrame:
            nadir_and_date = (
                df.with_columns(
                    rnrsp_size=PolarsParsers.to_optional_int64(pl.col("RNRSP_TERNTBAS")),
                    rnrsp_nadir=PolarsParsers.to_optional_int64(pl.col("RNRSP_TERNAD")),
                    ra_size=PolarsParsers.to_optional_int64(pl.col("RA_RARECBAS")),
                    ra_nadir=PolarsParsers.to_optional_int64(pl.col("RA_RARECNAD")),
                    rnrsp_date=PolarsParsers.to_optional_date(pl.col("RNRSP_EventDate")),
                    ra_date=PolarsParsers.to_optional_date(pl.col("RA_EventDate")),
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
                    pl.col("date_candidate").alias(cols.TARGET_LESION_MEASUREMENT_DATE),
                    pl.col("nadir_candidate").alias(cols.TARGET_LESION_NADIR),
                )
            )
            size = self._subject_baseline_target_lesion_size().rename({"baseline_size": cols.TARGET_LESION_SIZE})
            return size.join(nadir_and_date, on="SubjectId", how="inner")

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
        joined = (
            subjects_with_any.join(ta, on="SubjectId", how="left")
            .join(
                ntl,
                on="SubjectId",
                how="left",
            )
            .join(
                tl,
                on="SubjectId",
                how="left",
            )
            .select(
                "SubjectId",
                cols.ASSESSMENT_TYPE,
                cols.ASSESSMENT_DATE,
                cols.TARGET_LESION_SIZE,
                cols.TARGET_LESION_NADIR,
                cols.TARGET_LESION_MEASUREMENT_DATE,
                cols.OFF_TARGET_LESIONS_NUMBER,
                cols.OFF_TARGET_LESION_MEASUREMENT_DATE,
            )
        )

        return joined

    @collection(
        TumorAssessment,
        order_by=("date",),
        require_order_by=True,
    )
    def _process_tumor_assessments(self) -> pl.DataFrame | None:
        cols = TumorAssessment.Fields
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

        baseline_size = self._subject_baseline_target_lesion_size()

        def process(frame: pl.DataFrame) -> pl.DataFrame:
            _processed = (
                frame.with_columns(
                    (
                        pl.when(pl.col("RA_RAASSESS2").is_not_null())
                        .then(pl.lit("irecist"))
                        # takes precendence over irecist, so if collision, overwrite
                        .when(pl.col("RA_RAASSESS1").is_not_null())
                        .then(pl.lit("recist"))
                        # infer from row since not separate variable
                        .when(pl.col("RNRSP_TERNCFB").is_not_null())
                        .then(pl.lit("rano"))
                        .otherwise(None)
                    ).alias(cols.ASSESSMENT_TYPE),
                )
                .with_columns(
                    pl.coalesce([pl.col("RA_RABASECH"), pl.col("RNRSP_TERNCFB")]).cast(pl.Float64, strict=False).alias("_tl_change_baseline"),
                    pl.coalesce([pl.col("RA_RARECCH"), pl.col("RNRSP_TERNCFN")]).cast(pl.Float64, strict=False).alias("_tl_change_nadir"),
                    pl.coalesce([pl.col("RA_EventId"), pl.col("RNRSP_EventId")]).cast(pl.Utf8, strict=False).alias(cols.EVENT_ID),
                )
                .with_columns(
                    (
                        pl.when(pl.col("_tl_change_baseline").is_null())
                        .then(None)
                        .when(pl.col("_tl_change_baseline") == 0)
                        .then(0)
                        .otherwise(pl.col("_tl_change_baseline") / 100)
                    ).alias(cols.TARGET_LESION_CHANGE_FROM_BASELINE),
                    (
                        pl.when(pl.col("_tl_change_nadir").is_null())
                        .then(None)
                        .when(pl.col("_tl_change_nadir") == 0)
                        .then(0)
                        .otherwise(pl.col("_tl_change_nadir") / 100)
                    ).alias(cols.TARGET_LESION_CHANGE_FROM_NADIR),
                )
                # absolute size per assessment: baseline * (1 + change_from_baseline).
                # null propagates for missing baseline or missing change.
                .join(baseline_size, on="SubjectId", how="left")
                .with_columns(
                    (pl.col("baseline_size").cast(pl.Float64, strict=False) * (pl.lit(1.0) + pl.col(cols.TARGET_LESION_CHANGE_FROM_BASELINE))).alias(
                        cols.TARGET_LESION_SIZE
                    ),
                )
                .with_columns(
                    PolarsParsers.int_to_bool(
                        x=pl.coalesce([pl.col("RA_RANLBASECD"), pl.col("RNRSP_RNRSPNLCD")]).cast(pl.Int64, strict=False),
                        true_int=1,
                        false_int=0,
                    ).alias(cols.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE),
                )
                .with_columns(PolarsParsers.to_optional_date(pl.coalesce([pl.col("RA_EventDate"), pl.col("RNRSP_EventDate")])).alias(cols.DATE))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("RA_RATIMRES")).str.strip_chars().alias(cols.RECIST_RESPONSE),
                    PolarsParsers.to_optional_utf8(pl.col("RA_RAiMOD")).str.strip_chars().alias(cols.IRECIST_RESPONSE),
                    PolarsParsers.to_optional_utf8(pl.col("RNRSP_RNRSPCL")).str.strip_chars().alias(cols.RANO_RESPONSE),
                    PolarsParsers.to_optional_date(pl.col("RA_RAPROGDT")).alias(cols.RECIST_DATE_OF_PROGRESSION),
                    PolarsParsers.to_optional_date(pl.col("RA_RAiUNPDT")).alias(cols.IRECIST_DATE_OF_PROGRESSION),
                )
                # keep only rows with real signal
                .with_columns(
                    has_any=pl.any_horizontal(
                        [
                            pl.col(cols.ASSESSMENT_TYPE).str.len_bytes() > 0,
                            pl.col(cols.TARGET_LESION_CHANGE_FROM_BASELINE).is_not_null(),
                            pl.col(cols.TARGET_LESION_CHANGE_FROM_NADIR).is_not_null(),
                            pl.col(cols.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE).is_not_null(),
                            pl.col(cols.DATE).is_not_null(),
                            pl.col(cols.RECIST_RESPONSE).str.len_bytes() > 0,
                            pl.col(cols.IRECIST_RESPONSE).str.len_bytes() > 0,
                            pl.col(cols.RANO_RESPONSE).str.len_bytes() > 0,
                            pl.col(cols.RECIST_DATE_OF_PROGRESSION).is_not_null(),
                            pl.col(cols.IRECIST_DATE_OF_PROGRESSION).is_not_null(),
                        ],
                    ),
                )
                .filter(pl.col("has_any"))
                .select(
                    "SubjectId",
                    cols.ASSESSMENT_TYPE,
                    cols.TARGET_LESION_SIZE,
                    cols.TARGET_LESION_CHANGE_FROM_BASELINE,
                    cols.TARGET_LESION_CHANGE_FROM_NADIR,
                    cols.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE,
                    cols.DATE,
                    cols.RECIST_RESPONSE,
                    cols.IRECIST_RESPONSE,
                    cols.RANO_RESPONSE,
                    cols.RECIST_DATE_OF_PROGRESSION,
                    cols.IRECIST_DATE_OF_PROGRESSION,
                    cols.EVENT_ID,
                )
            )

            return _processed

        return process(base)

    # TODO: refactor to not use regex later
    @collection(
        C30,
        order_by=("date",),
        require_order_by=True,
    )
    def _process_c30(self) -> pl.DataFrame | None:
        cols = C30.Fields
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

            # rename mapping:
            # C30_C30_Q1 -> q1
            # C30_C30_Q1CD -> q1_code
            def q_alias(col: str) -> str:
                m = question_text_re.fullmatch(col)
                if m:
                    return f"q{m.group(1)}"
                m = question_code_re.fullmatch(col)
                if m:
                    return f"q{m.group(1)}_code"
                return col

            # Build column list using Fields constants
            q_text_cols = [getattr(cols, f"Q{i}") for i in range(1, C30.Q_COUNT + 1)]
            q_code_cols = [getattr(cols, f"Q{i}_CODE") for i in range(1, C30.Q_COUNT + 1)]

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("C30_EventName")).str.strip_chars().alias(cols.EVENT_NAME),
                    PolarsParsers.to_optional_date(pl.col("C30_EventDate")).alias(cols.DATE),
                    *[PolarsParsers.to_optional_utf8(pl.col(c)).str.strip_chars().alias(q_alias(c)) for c in text_cols],
                    *[PolarsParsers.to_optional_int64(pl.col(c)).alias(q_alias(c)) for c in code_cols],
                )
                .select("SubjectId", cols.DATE, cols.EVENT_NAME, *q_text_cols, *q_code_cols)
            )

            return out

        processed = process_c30(frame=base)
        return processed

    # TODO: refactor to not use regex later
    @collection(
        EQ5D,
        order_by=("date",),
        require_order_by=True,
    )
    def _process_eq5d(self) -> pl.DataFrame | None:
        cols = EQ5D.Fields
        question_col_re = re.compile(r"^EQ5D_EQ5D([1-5])$")
        question_code_re = re.compile(r"^(?:EQ5D_)?EQ5D([1-5])CD$")

        # Build column lists using Fields constants
        q_text_cols = [getattr(cols, f"Q{i}") for i in range(1, EQ5D.Q_COUNT + 1)]
        q_code_cols = [getattr(cols, f"Q{i}_CODE") for i in range(1, EQ5D.Q_COUNT + 1)]

        base = self.data.select(
            pl.col(["SubjectId", "EQ5D_EventName", "EQ5D_EQ5DVAS", "EQ5D_EventDate"]),
            pl.selectors.matches(question_col_re.pattern),
            pl.selectors.matches(question_code_re.pattern),
        )

        def process_eq5d(frame: pl.DataFrame) -> pl.DataFrame:
            text_cols = [c for c in frame.columns if question_col_re.fullmatch(c)]
            code_cols = [c for c in frame.columns if question_code_re.fullmatch(c)]

            # build rename mapping: EQ5D_EQ5D1 -> q1, EQ5D1CD -> q1_code
            def q_alias(col: str) -> str:
                m = question_col_re.fullmatch(col)
                if m:
                    return getattr(cols, f"Q{m.group(1)}")
                m = question_code_re.fullmatch(col)
                if m:
                    return getattr(cols, f"Q{m.group(1)}_CODE")
                return col

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("EQ5D_EventName")).str.strip_chars().alias(cols.EVENT_NAME),
                    PolarsParsers.to_optional_date(pl.col("EQ5D_EventDate")).alias(cols.DATE),
                    PolarsParsers.to_optional_int64(pl.col("EQ5D_EQ5DVAS")).alias(cols.QOL_METRIC),
                    *[PolarsParsers.to_optional_utf8(pl.col(c)).str.strip_chars().alias(q_alias(c)) for c in text_cols],
                    *[PolarsParsers.to_optional_int64(pl.col(c)).alias(q_alias(c)) for c in code_cols],
                )
                .select("SubjectId", cols.DATE, cols.EVENT_NAME, cols.QOL_METRIC, *q_text_cols, *q_code_cols)
            )

            return out

        processed = process_eq5d(frame=base)
        return processed

    @singleton(BestOverallResponse)
    def _process_best_overall_response(self) -> pl.DataFrame | None:
        """
        Takes the lowest value of the response code across all tumor assessments for each patient,
        i.e. selects the best response across entire treatment duration.
        Also assumes tumor evaluations are mutually exclusive.
        Removes unconfirmed iRecist responses, and takes best response across Recist and iRecist when
        rows have both evaluations.
        """
        cols = BestOverallResponse.Fields
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
                    pl.col("RA_RAiMODCD").cast(pl.Int64),
                    pl.col("RA_RATIMRESCD").cast(pl.Int64),
                )
                .with_columns(
                    [
                        # map irecist code to recist scale
                        pl.when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).eq(4))
                        .then(None)
                        .when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).eq(5))
                        .then(4)
                        .when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).eq(6))
                        .then(96)
                        .otherwise(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")))
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
                    pl.coalesce("recist_text", "RNRSP_RNRSPCL").cast(pl.Utf8, strict=False).str.strip_chars().alias(cols.RESPONSE),
                    pl.coalesce("recist_code", "RNRSP_RNRSPCLCD").cast(pl.Int64, strict=False).alias(cols.CODE),
                    PolarsParsers.to_optional_date(pl.coalesce("RA_EventDate", "RNRSP_EventDate")).alias(cols.DATE),
                )
                .filter(pl.col(cols.CODE).is_not_null())
                .sort(["SubjectId", cols.CODE])
                .group_by("SubjectId", maintain_order=True)
                .first()
                .select("SubjectId", cols.RESPONSE, cols.CODE, cols.DATE)
            )

            return result

        return process(base)
