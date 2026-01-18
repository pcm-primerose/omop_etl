import re
from deprecated import deprecated
import polars as pl
from logging import getLogger

from omop_etl.harmonization.core.parsers import PolarsParsers
from omop_etl.harmonization.harmonizers.base import BaseHarmonizer, ScalarSpec, SingletonSpec, CollectionSpec
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.best_overall_response import BestOverallResponse
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.followup import FollowUp
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle import TreatmentCycle
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.harmonization.models.patient import Patient

log = getLogger(__name__)


class ImpressHarmonizer(BaseHarmonizer):
    def __init__(self, data: pl.DataFrame, trial_id: str):
        super().__init__(data, trial_id)

    SPECS = (
        # scalars
        ScalarSpec(
            name="cohort_name",
            process=lambda h: h._process_cohort_name(),
            target_attr="cohort_name",
            value_col="cohort_name",
        ),
        ScalarSpec(
            name="sex",
            process=lambda h: h._process_sex(),
            target_attr="sex",
            value_col="sex",
        ),
        ScalarSpec(
            name="date_of_birth",
            process=lambda h: h._process_date_of_birth(),
            target_attr="date_of_birth",
            value_col="date_of_birth",
        ),
        ScalarSpec(
            name="age",
            process=lambda h: h._process_age(),
            target_attr="age",
            value_col="age",
        ),
        ScalarSpec(
            name="date_of_death",
            process=lambda h: h._process_date_of_death(),
            target_attr="date_of_death",
            value_col="date_of_death",
        ),
        ScalarSpec(
            name="has_any_adverse_events",
            process=lambda h: h._process_has_any_adverse_events(),
            target_attr="has_any_adverse_events",
            value_col="has_any_adverse_events",
        ),
        ScalarSpec(
            name="number_of_adverse_events",
            process=lambda h: h._process_number_of_adverse_events(),
            target_attr="number_of_adverse_events",
            value_col="number_of_adverse_events",
        ),
        ScalarSpec(
            name="number_of_serious_adverse_events",
            process=lambda h: h._process_number_of_serious_adverse_events(),
            target_attr="number_of_serious_adverse_events",
            value_col="number_of_serious_adverse_events",
        ),
        ScalarSpec(
            name="treatment_start_last_cycle",
            process=lambda h: h._process_treatment_start_last_cycle(),
            target_attr="treatment_start_last_cycle",
            value_col="treatment_start_last_cycle",
        ),
        ScalarSpec(
            name="treatment_start_date",
            process=lambda h: h._process_treatment_start_date(),
            target_attr="treatment_start_date",
            value_col="treatment_start_date",
        ),
        ScalarSpec(
            name="evaluable_for_efficacy_analysis",
            process=lambda h: h._process_evaluable_for_efficacy_analysis(),
            target_attr="evaluable_for_efficacy_analysis",
            value_col="evaluable_for_efficacy_analysis",
        ),
        ScalarSpec(
            name="clinical_benefit",
            process=lambda h: h._process_clinical_benefit(),
            target_attr="has_clinical_benefit_at_week16",
            value_col="has_clinical_benefit_at_week16",
        ),
        ScalarSpec(
            name="eot_reason",
            process=lambda h: h._process_eot_reason(),
            target_attr="end_of_treatment_reason",
            value_col="end_of_treatment_reason",
        ),
        ScalarSpec(
            name="end_of_treatment_date",
            process=lambda h: h._process_end_of_treatment_date(),
            target_attr="end_of_treatment_date",
            value_col="end_of_treatment_date",
        ),
        # singletons
        SingletonSpec(
            name="study_drugs",
            process=lambda h: h._process_study_drugs(),
            target_domain=StudyDrugs,
        ),
        SingletonSpec(
            name="best_overall_response",
            process=lambda h: h._process_best_overall_response(),
            target_domain=BestOverallResponse,
        ),
        SingletonSpec(
            name="lost_to_followup",
            process=lambda h: h._process_lost_to_followup(),
            target_domain=FollowUp,
        ),
        SingletonSpec(
            name="biomarkers",
            process=lambda h: h._process_biomarkers(),
            target_domain=Biomarkers,
        ),
        SingletonSpec(
            name="baseline_tumor_assessment",
            process=lambda h: h._process_baseline_tumor_assessment(),
            target_domain=TumorAssessmentBaseline,
        ),
        SingletonSpec(
            name="tumor_type",
            process=lambda h: h._process_tumor_type(),
            target_domain=TumorType,
        ),
        SingletonSpec(
            name="ecog_baseline",
            process=lambda h: h._process_ecog_baseline(),
            target_domain=EcogBaseline,
        ),
        # collections
        CollectionSpec(
            name="adverse_events",
            process=lambda h: h._process_adverse_events(),
            target_domain=AdverseEvent,
            order_by=("start_date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="previous_treatments",
            process=lambda h: h._process_previous_treatments(),
            target_domain=PreviousTreatments,
            order_by=("start_date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="medical_histories",
            process=lambda h: h._process_medical_histories(),
            target_domain=MedicalHistory,
            order_by=("start_date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="treatment_cycle",
            process=lambda h: h._process_treatment_cycle(),
            target_domain=TreatmentCycle,
            order_by=("start_date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="concomitant_medication",
            process=lambda h: h._process_concomitant_medication(),
            target_domain=ConcomitantMedication,
            order_by=("start_date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="tumor_assessments",
            process=lambda h: h._process_tumor_assessments(),
            target_domain=TumorAssessment,
            order_by=("date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="c30",
            process=lambda h: h._process_c30(),
            target_domain=C30,
            order_by=("date",),
            require_order_by=True,
        ),
        CollectionSpec(
            name="eq5d",
            process=lambda h: h._process_eq5d(),
            target_domain=EQ5D,
            order_by=("date",),
            require_order_by=True,
        ),
    )

    def _create_patients(self) -> None:
        """Create Patient instances from unique SubjectIds."""
        patient_ids = self.data.select("SubjectId").unique().to_series().to_list()
        for pid in patient_ids:
            self.patient_data[pid] = Patient(trial_id=self.trial_id, patient_id=pid)

    def process(self) -> HarmonizedData:
        """Run harmonization and return HarmonizedData."""
        self.run()
        return HarmonizedData(
            patients=list(self.patient_data.values()),
            trial_id=self.trial_id,
        )

    def _process_cohort_name(self) -> pl.DataFrame | None:
        return self.data.select(
            "SubjectId",
            cohort_name=PolarsParsers.to_optional_utf8(pl.col("COH_COHORTNAME")),
        ).filter(pl.col("cohort_name").is_not_null())

    def _process_sex(self) -> pl.DataFrame | None:
        return (
            self.data.select(
                "SubjectId",
                sex=(
                    pl.when(PolarsParsers.to_optional_utf8(pl.col("DM_SEX")).str.to_lowercase().is_in(["m", "male"]))
                    .then(pl.lit("male"))
                    .when(PolarsParsers.to_optional_utf8(pl.col("DM_SEX")).str.to_lowercase().is_in(["f", "female"]))
                    .then(pl.lit("female"))
                    .otherwise(None)
                ),
            )
            .filter(pl.col("sex").is_not_null())
            .unique(subset=["SubjectId"], keep="first")
        )

    def _process_date_of_birth(self) -> pl.DataFrame | None:
        """Process date of birth and update patient objects"""
        return (
            self.data.group_by("SubjectId")
            .agg(pl.col("DM_BRTHDAT").drop_nulls().first().alias("birth_date"))
            .with_columns(date_of_birth=(PolarsParsers.to_optional_date(pl.col("birth_date"))))
        )

    def _process_age(self) -> pl.DataFrame | None:
        """Calculate age at treatment start and update patient object"""
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
                age=((pl.col("last_treatment") - pl.col("birth_date")).dt.total_days().cast(pl.Int64) / 365.25).cast(pl.Int64),
            )
        )

    def _process_date_of_death(self) -> pl.DataFrame | None:
        death_df = (
            self.data.select(
                "SubjectId",
                eos=PolarsParsers.to_optional_date(pl.col("EOS_DEATHDTC")),
                fu=PolarsParsers.to_optional_date(pl.col("FU_FUPDEDAT")),
            )
            .with_columns(
                date_of_death=pl.max_horizontal("eos", "fu"),
            )
            .group_by("SubjectId")
            .agg(pl.max("date_of_death").alias("date_of_death"))
            .filter(pl.col("date_of_death").is_not_null())
            .select("SubjectId", "date_of_death")
        )
        return death_df

    def _process_has_any_adverse_events(self) -> pl.DataFrame | None:
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
            .agg(has_any_adverse_events=pl.col("row_has_ae").any())
        )
        return ae_status

    def _process_number_of_adverse_events(self) -> pl.DataFrame | None:
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
            .agg(number_of_adverse_events=pl.col("ae_num").sum())
        )
        return ae_num

    def _process_number_of_serious_adverse_events(self) -> pl.DataFrame | None:
        sae_counts = (
            self.data.with_columns(
                is_serious=(PolarsParsers.to_optional_int64("AE_AESERCD") == 1).fill_null(False),
            )
            .group_by("SubjectId")
            .agg(number_of_serious_adverse_events=pl.col("is_serious").sum().cast(pl.Int64))
        )
        return sae_counts

    def _process_clinical_benefit(self) -> pl.DataFrame | None:
        """
        Clinical benefit at W16 (visit 3).
        Note: If patient has iRecist *and* Recist at same assessment,
        iRecist evaluation takes precedence as it's a more specific assessment.
        """
        timepoint = "V03"

        benefit = (
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
                has_clinical_benefit_at_week16=pl.when(PolarsParsers.to_optional_int64(pl.col("RA_RATIMRESCD")).le(3))
                .then(True)
                .when(PolarsParsers.to_optional_int64(pl.col("RA_RAiMODCD")).le(3))
                .then(True)
                .when(PolarsParsers.to_optional_int64(pl.col("RNRSP_RNRSPCLCD")).le(3))
                .then(True)
                .otherwise(False),
            )
        )

        return benefit

    def _process_eot_reason(self) -> pl.DataFrame | None:
        eot_reason = (
            self.data.select("SubjectId", "EOT_EOTREOT")
            .with_columns(
                end_of_treatment_reason=PolarsParsers.to_optional_utf8(pl.col("EOT_EOTREOT")).str.strip_chars(),
            )
            .filter(pl.col("end_of_treatment_reason").is_not_null())
        )
        return eot_reason

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

        @deprecated
        def eot_filter() -> pl.DataFrame:
            has_ended_treatment = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(PolarsParsers.to_optional_utf8(pl.col(["EOT_EventDate"])).str.len_bytes() > 0).any().alias("has_clinical_assessment"),
            )
            return has_ended_treatment

        @deprecated
        def tumor_assessment() -> pl.DataFrame:
            # need to add V04 filter (if this is to be used again)
            has_tumor_assessment_week_4 = evaluability_data.group_by("SubjectId").agg(
                pl.any_horizontal(
                    PolarsParsers.to_optional_utf8(
                        pl.col(
                            [
                                "RA_EventDate",
                                "RNRSP_EventDate",
                                "RCNT_EventDate",
                                "RNTMNT_EventDate",
                            ],
                        ),
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
                .with_columns(evaluable_for_efficacy_analysis=(pl.col("oral_sufficient_treatment_length") | pl.col("iv_sufficient_treatment_length")))
            )

            return _merged_df

        return _merge_evaluability()

    def _process_treatment_start_date(self) -> pl.DataFrame | None:
        treatment_start_data = (
            self.data.lazy()
            .select(["SubjectId", "TR_TRNAME", "TR_TRC1_DT"])
            .with_columns(
                tr_name=PolarsParsers.to_optional_utf8(pl.col("TR_TRNAME")).str.strip_chars(),
                treatment_start_date=PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")),
            )
            # keep only real names: non-null & len > 0
            .filter(pl.col("tr_name").is_not_null() & (pl.col("tr_name").str.len_chars() > 0))
            .group_by("SubjectId")
            .agg(pl.col("treatment_start_date").drop_nulls().min().alias("treatment_start_date"))
            .collect()
            .select(["SubjectId", "treatment_start_date"])
        )

        return treatment_start_data

    def _process_end_of_treatment_date(self) -> pl.DataFrame | None:
        treatment_stop_data = (
            self.data.select(
                "SubjectId",
                "TR_TRCYNCD",
                "TR_TROSTPDT",
                "TR_TRC1_DT",
                "EOT_EOTDAT",
            )
            .with_columns(
                valid=PolarsParsers.to_optional_int64(pl.col("TR_TRCYNCD")).eq(1),
                eot_date=PolarsParsers.to_optional_date(pl.col("EOT_EOTDAT").cast(pl.Utf8)),
                oral_stop=PolarsParsers.to_optional_date(pl.col("TR_TROSTPDT").cast(pl.Utf8)),
                iv_start=PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT").cast(pl.Utf8)),
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
                end_of_treatment_date=pl.coalesce([pl.col("last_eot"), pl.col("last_oral"), pl.col("last_iv")]),
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

        return treatment_stop_data

    def _process_treatment_start_last_cycle(self) -> pl.DataFrame | None:
        """
        Note: currently not filtering for valid cycles, just selecting latest treatment starts.
        Set enforce_valid=True if TR_TRCYNCD must be 1 (i.e. filtering for valid cycles only)
        """
        enforce_valid = False

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
            .agg(treatment_start_last_cycle=pl.col("cycle_start").max())
            .select("SubjectId", "treatment_start_last_cycle")
        )

        return last_cycle_data

    def _process_tumor_type(self) -> pl.DataFrame:
        # COHTTYPE__3/CD is present in source, but has no data
        C = TumorType.Cols
        df = (
            self.data.with_row_index("_row")
            .select(
                "_row",
                "SubjectId",
                PolarsParsers.to_optional_utf8(pl.col("COH_ICD10COD")).str.strip_chars().alias(C.ICD10_CODE),
                PolarsParsers.to_optional_utf8(pl.col("COH_ICD10DES")).str.strip_chars().alias(C.ICD10_DESCRIPTION),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTT")).str.strip_chars().alias(C.COHORT_TUMOR_TYPE),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTTOSP")).str.strip_chars().alias(C.OTHER_TUMOR_TYPE),
                # main tumor-type
                t1=PolarsParsers.to_optional_utf8(pl.col("COH_COHTTYPE")).str.strip_chars(),
                t1cd=PolarsParsers.to_optional_int64(pl.col("COH_COHTTYPECD")),
                t2=PolarsParsers.to_optional_utf8(pl.col("COH_COHTTYPE__2")).str.strip_chars(),
                t2cd=PolarsParsers.to_optional_int64(pl.col("COH_COHTTYPE__2CD")),
            )
            # keep rows where any relevant field is populated
            .filter(
                pl.any_horizontal(
                    pl.col(C.ICD10_CODE).is_not_null(),
                    pl.col(C.COHORT_TUMOR_TYPE).is_not_null(),
                    pl.col("t1").is_not_null(),
                    pl.col("t2").is_not_null(),
                    pl.col(C.OTHER_TUMOR_TYPE).is_not_null(),
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
                pl.when(~pl.col("collisions")).then(pl.col("m_type_raw")).otherwise(None).alias(C.MAIN_TUMOR_TYPE),
                pl.when(~pl.col("collisions")).then(pl.col("m_code_raw")).otherwise(None).alias(C.MAIN_TUMOR_TYPE_CODE),
            )
            # last write wins per SubjectId
            .sort("_row")
            .unique(subset=["SubjectId"], keep="last")
            .select("SubjectId", C.ICD10_CODE, C.ICD10_DESCRIPTION, C.MAIN_TUMOR_TYPE, C.MAIN_TUMOR_TYPE_CODE, C.COHORT_TUMOR_TYPE, C.OTHER_TUMOR_TYPE)
        )

        return df

    def _process_study_drugs(self) -> pl.DataFrame:
        C = StudyDrugs.Cols
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
                .alias(C.PRIMARY_TREATMENT_DRUG),
                pl.when(~pl.col("primary_collision"))
                .then(pl.coalesce([pl.col("p1cd"), pl.col("p2cd"), pl.col("p3cd")]))
                .otherwise(None)
                .alias(C.PRIMARY_TREATMENT_DRUG_CODE),
                pl.when(~pl.col("secondary_collision"))
                .then(pl.coalesce([pl.col("s1"), pl.col("s2"), pl.col("s3")]))
                .otherwise(None)
                .alias(C.SECONDARY_TREATMENT_DRUG),
                pl.when(~pl.col("secondary_collision"))
                .then(pl.coalesce([pl.col("s1cd"), pl.col("s2cd"), pl.col("s3cd")]))
                .otherwise(None)
                .alias(C.SECONDARY_TREATMENT_DRUG_CODE),
            )
            # drop colliding rows entirely
            .filter(~pl.col("primary_collision") & ~pl.col("secondary_collision"))
            # last write wins per SubjectId
            .sort("_row")
            .unique(subset=["SubjectId"], keep="last")
            .select("SubjectId", C.PRIMARY_TREATMENT_DRUG, C.PRIMARY_TREATMENT_DRUG_CODE, C.SECONDARY_TREATMENT_DRUG, C.SECONDARY_TREATMENT_DRUG_CODE)
        )

        return df

    def _process_biomarkers(self) -> pl.DataFrame:
        C = Biomarkers.Cols
        df = (
            self.data.select(
                "SubjectId",
                PolarsParsers.to_optional_date(pl.col("COH_EventDate")).alias("event_date"),
                PolarsParsers.to_optional_utf8(pl.col("COH_GENMUT1")).str.strip_chars().alias(C.GENE_AND_MUTATION),
                PolarsParsers.to_optional_int64(pl.col("COH_GENMUT1CD")).alias(C.GENE_AND_MUTATION_CODE),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHCTN")).str.strip_chars().alias(C.COHORT_TARGET_NAME),
                PolarsParsers.to_optional_utf8(pl.col("COH_COHTMN")).str.strip_chars().alias(C.COHORT_TARGET_MUTATION),
            )
            .filter(
                pl.any_horizontal(
                    pl.col(C.GENE_AND_MUTATION).is_not_null(),
                    pl.col(C.GENE_AND_MUTATION_CODE).is_not_null(),
                    pl.col(C.COHORT_TARGET_NAME).is_not_null(),
                    pl.col(C.COHORT_TARGET_MUTATION).is_not_null(),
                ),
            )
            # latest event per SubjectId
            .sort(["SubjectId", "event_date"])
            .rename({"event_date": C.DATE})
            .unique(subset=["SubjectId"], keep="last")
            .select("SubjectId", C.GENE_AND_MUTATION, C.GENE_AND_MUTATION_CODE, C.COHORT_TARGET_NAME, C.COHORT_TARGET_MUTATION, C.DATE)
        )

        return df

    def _process_lost_to_followup(self) -> pl.DataFrame:
        C = FollowUp.Cols
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
                pl.col("ltfu_row").any().alias(C.LOST_TO_FOLLOWUP),
                pl.col("ltfu_date").max().alias(C.DATE_LOST_TO_FOLLOWUP),
            )
        ).select("SubjectId", C.LOST_TO_FOLLOWUP, C.DATE_LOST_TO_FOLLOWUP)

        return lost_to_followup

    def _process_ecog_baseline(self) -> pl.DataFrame:
        """
        Parses dates with defaults, strips description data, casts to correct types.
        Only select one baseline ECOG event per patient, using latest available date.
        """
        C = EcogBaseline.Cols

        ecog_base = self.data.select("SubjectId", "ECOG_EventId", "ECOG_ECOGS", "ECOG_ECOGSCD", "ECOG_ECOGDAT").filter(
            pl.col("ECOG_EventId") == "V00",
        )

        def parse_ecog_data(ecog_data: pl.DataFrame) -> pl.DataFrame:
            filtered_ecog_data = ecog_data.with_columns(
                PolarsParsers.to_optional_date(pl.col("ECOG_ECOGDAT")).alias(C.DATE),
                PolarsParsers.to_optional_int64(pl.col("ECOG_ECOGSCD")).alias(C.GRADE),
                PolarsParsers.to_optional_utf8(pl.col("ECOG_ECOGS")).str.strip_chars().alias(C.DESCRIPTION),
            ).select("SubjectId", C.DATE, C.DESCRIPTION, C.GRADE)
            return filtered_ecog_data

        def select_latest_baseline(data: pl.DataFrame) -> pl.DataFrame:
            _latest = data.sort(["SubjectId", C.DATE]).group_by("SubjectId").tail(1)
            return _latest

        def filter_all_nulls(data: pl.DataFrame) -> pl.DataFrame:
            return data.with_columns(has_ecog=pl.any_horizontal(pl.col([C.DESCRIPTION]).is_not_null()).fill_null(False))

        def merge_ecog(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            return base.join(processed, on="SubjectId", how="left")

        # process
        parsed = parse_ecog_data(ecog_data=ecog_base)
        latest = select_latest_baseline(parsed)
        valid = filter_all_nulls(latest)
        joined = merge_ecog(base=ecog_base, processed=valid)
        labeled = filter_all_nulls(joined).select("SubjectId", C.DATE, C.DESCRIPTION, C.GRADE)

        return labeled

    def _process_medical_histories(self) -> pl.DataFrame | None:
        C = MedicalHistory.Cols
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
                PolarsParsers.to_optional_utf8(pl.col("MH_MHTERM")).str.strip_chars().alias(C.TERM),
                PolarsParsers.to_optional_int64(pl.col("MH_MHSPID")).alias(C.SEQUENCE_ID),
                PolarsParsers.to_optional_date(pl.col("MH_MHSTDAT")).alias(C.START_DATE),
                PolarsParsers.to_optional_date(pl.col("MH_MHENDAT")).alias(C.END_DATE),
                PolarsParsers.to_optional_utf8(pl.col("MH_MHONGO")).str.strip_chars().alias(C.STATUS),
                PolarsParsers.to_optional_int64(pl.col("MH_MHONGOCD")).alias(C.STATUS_CODE),
            ).filter(pl.col(C.TERM).is_not_null())

            return filtered_data

        def merge_medical_history(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col([C.TERM, C.SEQUENCE_ID, C.START_DATE, C.END_DATE, C.STATUS, C.STATUS_CODE]).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_medical_histories(mh_base)
        merged = merge_medical_history(base=mh_base, processed=filtered).select(
            "SubjectId", C.TERM, C.SEQUENCE_ID, C.START_DATE, C.END_DATE, C.STATUS, C.STATUS_CODE
        )

        return merged

    def _process_previous_treatments(self) -> pl.DataFrame | None:
        C = PreviousTreatments.Cols
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
                PolarsParsers.to_optional_utf8(pl.col("CT_CTTYPE")).str.strip_chars().alias(C.TREATMENT),
                PolarsParsers.to_optional_int64(pl.col("CT_CTTYPECD")).alias(C.TREATMENT_CODE),
                PolarsParsers.to_optional_int64(pl.col("CT_CTSPID")).alias(C.TREATMENT_SEQUENCE_NUMBER),
                PolarsParsers.to_optional_date(pl.col("CT_CTSTDAT")).alias(C.START_DATE),
                PolarsParsers.to_optional_date(pl.col("CT_CTENDAT")).alias(C.END_DATE),
                PolarsParsers.to_optional_utf8(pl.col("CT_CTTYPESP")).str.strip_chars().alias(C.ADDITIONAL_TREATMENT),
            ).filter(pl.col(C.TREATMENT).is_not_null())
            return filtered_data

        def merge_previous_treatments(base: pl.DataFrame, processed: pl.DataFrame) -> pl.DataFrame:
            subjects = base.select("SubjectId").unique()
            _merged = subjects.join(processed, on="SubjectId", how="left").filter(
                pl.any_horizontal(
                    pl.col([C.TREATMENT, C.TREATMENT_CODE, C.TREATMENT_SEQUENCE_NUMBER, C.START_DATE, C.END_DATE, C.ADDITIONAL_TREATMENT]).is_not_null(),
                ),
            )
            return _merged

        filtered = filter_previous_treatments(ct_base)
        merged = merge_previous_treatments(base=ct_base, processed=filtered).select(
            "SubjectId", C.TREATMENT, C.TREATMENT_CODE, C.TREATMENT_SEQUENCE_NUMBER, C.START_DATE, C.END_DATE, C.ADDITIONAL_TREATMENT
        )
        return merged

    def _process_treatment_cycle(self) -> pl.DataFrame | None:
        C = TreatmentCycle.Cols
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
                pl.when(has_oral).then(pl.lit("oral")).when(has_iv).then(pl.lit("IV")).otherwise(pl.lit(None, dtype=pl.Utf8)).alias(C.CYCLE_TYPE),
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
                    next_start=pl.when(pl.col(C.CYCLE_TYPE) == "IV").then(pl.col("start").shift(-1).over(["SubjectId", "TR_TRTNO"])).otherwise(None),
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
                .alias(C.END_DATE),
            )
            return coalesced

        def filter_parse_treatment_cycles(frame: pl.DataFrame) -> pl.DataFrame:
            filtered_data = frame.with_columns(
                PolarsParsers.to_optional_date(pl.col("TR_TRC1_DT")).alias(C.START_DATE),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TRCYNCD")).alias(C.RECIEVED_TREATMENT_THIS_CYCLE),
                PolarsParsers.to_optional_bool(pl.col("TR_TRIVDELYN1")).alias(C.WAS_TOTAL_DOSE_DELIVERED),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TRO_YNCD")).alias(C.WAS_DOSE_ADMINISTERED_TO_SPEC),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("TR_TROTAKECD")).alias(C.WAS_TABLET_TAKEN_TO_PRESCRIPTION_IN_PREVIOUS_CYCLE),
            ).filter(pl.col("TR_TRNAME").is_not_null())

            return filtered_data

        def coerce_types(frame: pl.DataFrame) -> pl.DataFrame:
            """Cast non-processed cols"""
            _coerced = frame.with_columns(
                pl.col("TR_TRNAME").cast(pl.Utf8).alias(C.TREATMENT_NAME),
                pl.col("TR_TRTNO").cast(pl.Int64).alias(C.TREATMENT_NUMBER),
                pl.col("TR_TRCNO1").cast(pl.Int64).alias(C.CYCLE_NUMBER),
                pl.col("TR_TRIVDS1").cast(pl.Utf8).alias(C.IV_DOSE_PRESCRIBED),
                pl.col("TR_TRIVU1").cast(pl.Utf8).alias(C.IV_DOSE_PRESCRIBED_UNIT),
                pl.col("TR_TRODSTOT").cast(pl.Float64).alias(C.ORAL_DOSE_PRESCRIBED_PER_DAY),
                pl.col("TR_TRODSU").cast(pl.Utf8).alias(C.ORAL_DOSE_UNIT),
                pl.col("TR_TROREA").cast(pl.Utf8).alias(C.REASON_NOT_ADMINISTERED_TO_SPEC),
                pl.col("TR_TROSPE").cast(pl.Utf8).alias(C.REASON_TABLET_NOT_TAKEN),
                pl.col("TR_TROTABNO").cast(pl.Int64).alias(C.NUMBER_OF_DAYS_TABLET_NOT_TAKEN),
            )

            return _coerced

        coerced = coerce_types(cycle_base)
        cycle_typed = add_cycle_type(coerced)
        iv_cycle_end_dates = add_iv_cycle_stop_dates(cycle_typed)
        combined_end_dates = coalesce_cycle_ends(iv_cycle_end_dates)
        filtered = filter_parse_treatment_cycles(combined_end_dates).select(
            "SubjectId",
            C.TREATMENT_NAME,
            C.CYCLE_TYPE,
            C.TREATMENT_NUMBER,
            C.CYCLE_NUMBER,
            C.START_DATE,
            C.END_DATE,
            C.RECIEVED_TREATMENT_THIS_CYCLE,
            C.WAS_TOTAL_DOSE_DELIVERED,
            C.IV_DOSE_PRESCRIBED,
            C.IV_DOSE_PRESCRIBED_UNIT,
            C.WAS_DOSE_ADMINISTERED_TO_SPEC,
            C.REASON_NOT_ADMINISTERED_TO_SPEC,
            C.ORAL_DOSE_PRESCRIBED_PER_DAY,
            C.ORAL_DOSE_UNIT,
            C.NUMBER_OF_DAYS_TABLET_NOT_TAKEN,
            C.REASON_TABLET_NOT_TAKEN,
            C.WAS_TABLET_TAKEN_TO_PRESCRIPTION_IN_PREVIOUS_CYCLE,
        )

        return filtered

    def _process_concomitant_medication(self) -> pl.DataFrame | None:
        C = ConcomitantMedication.Cols
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
                PolarsParsers.to_optional_utf8(pl.col("CM_CMTRT")).cast(pl.Utf8, strict=False).str.strip_chars().alias(C.MEDICATION_NAME),
                PolarsParsers.int_to_bool(pl.col("CM_CMONGOCD")).alias(C.MEDICATION_ONGOING),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("CM_CMMHYNCD")).alias(C.WAS_TAKEN_DUE_TO_MEDICAL_HISTORY_EVENT),
                PolarsParsers.to_optional_bool(pl.col("CM_CMAEYN")).alias(C.WAS_TAKEN_DUE_TO_ADVERSE_EVENT),
                PolarsParsers.int_to_bool(true_int=1, false_int=0, x=pl.col("CM_CMONGOCD")).alias(C.IS_ADVERSE_EVENT_ONGOING),
                PolarsParsers.to_optional_date(pl.col("CM_CMSTDAT")).alias(C.START_DATE),
                PolarsParsers.to_optional_date(pl.col("CM_CMENDAT")).alias(C.END_DATE),
                PolarsParsers.to_optional_int64(pl.col("CM_CMSPID")).alias(C.SEQUENCE_ID),
            ).filter(pl.col(C.MEDICATION_NAME).is_not_null())

            return filtered_data

        filtered = filter_concomitant_data(cm_base).select(
            "SubjectId",
            C.MEDICATION_NAME,
            C.MEDICATION_ONGOING,
            C.WAS_TAKEN_DUE_TO_MEDICAL_HISTORY_EVENT,
            C.WAS_TAKEN_DUE_TO_ADVERSE_EVENT,
            C.IS_ADVERSE_EVENT_ONGOING,
            C.START_DATE,
            C.END_DATE,
            C.SEQUENCE_ID,
        )

        return filtered

    def _process_adverse_events(self) -> pl.DataFrame | None:
        C = AdverseEvent.Cols
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
                PolarsParsers.to_optional_date(pl.col("AE_AESTDAT")).alias(C.START_DATE),
                PolarsParsers.to_optional_date(pl.col("AE_AEENDAT")).alias(C.END_DATE),
                PolarsParsers.to_optional_date(pl.col("AE_SAESTDAT")).alias(C.TURNED_SERIOUS_DATE),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=0,
                    x=pl.col("AE_AESERCD").cast(pl.Int8, strict=False),
                ).alias(C.WAS_SERIOUS),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    x=pl.col("AE_SAEEXP1CD").cast(pl.Int8, strict=False),
                ).alias(C.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_1),
                PolarsParsers.int_to_bool(
                    true_int=1,
                    false_int=2,
                    x=pl.col("AE_SAEEXP2CD").cast(pl.Int8, strict=False),
                ).alias(C.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_2),
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
                ).alias(C.RELATED_TO_TREATMENT_1_STATUS),
                (
                    pl.when(pl.col("ae_rel_code_2") == 4)
                    .then(pl.lit("related"))
                    .when(pl.col("ae_rel_code_2") == 1)
                    .then(pl.lit("not_related"))
                    .when(pl.col("ae_rel_code_2").is_in([2, 3]))
                    .then(pl.lit("unknown"))
                    .otherwise(None)
                    .cast(pl.Enum(["related", "not_related", "unknown"]))
                ).alias(C.RELATED_TO_TREATMENT_2_STATUS),
            )
            return _parsed

        def locate_end_date_for_deceased(frame: pl.DataFrame) -> pl.DataFrame:
            end_date_frame = (
                frame.with_columns(death_date=PolarsParsers.to_optional_date(pl.col("FU_FUPDEDAT")))
                .with_columns(
                    pl.when(pl.col(C.END_DATE).is_null() & pl.col(C.WAS_SERIOUS).fill_null(False) & pl.col("death_date").is_not_null())
                    .then(pl.col("death_date"))
                    .otherwise(pl.col(C.END_DATE))
                    .alias(C.END_DATE),
                )
                .drop("death_date")
            )
            return end_date_frame

        def coerce(frame: pl.DataFrame) -> pl.DataFrame:
            return frame.with_columns(
                pl.col("AE_AECTCAET").cast(pl.Utf8).alias(C.TERM),
                pl.col("AE_AETOXGRECD").cast(pl.Int64).alias(C.GRADE),
                pl.col("AE_AEOUT").cast(pl.Utf8).alias(C.OUTCOME),
                pl.col("AE_AETRT1").cast(pl.Utf8).alias(C.TREATMENT_1_NAME),
                pl.col("AE_AETRT2").cast(pl.Utf8).alias(C.TREATMENT_2_NAME),
            )

        parsed = parse_events(ae_base)
        annot = locate_end_date_for_deceased(parsed)
        coerced = (
            coerce(annot)
            .filter(pl.col(C.TERM).is_not_null())
            .select(
                "SubjectId",
                C.TERM,
                C.GRADE,
                C.OUTCOME,
                C.START_DATE,
                C.END_DATE,
                C.WAS_SERIOUS,
                C.TURNED_SERIOUS_DATE,
                C.RELATED_TO_TREATMENT_1_STATUS,
                C.TREATMENT_1_NAME,
                C.RELATED_TO_TREATMENT_2_STATUS,
                C.TREATMENT_2_NAME,
                C.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_1,
                C.WAS_SERIOUS_GRADE_EXPECTED_TREATMENT_2,
            )
        )

        return None if coerced.is_empty() else coerced

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
        C = TumorAssessmentBaseline.Cols

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
                    pl.col("vi_value").alias(C.ASSESSMENT_TYPE),
                    pl.col("vi_date").alias(C.ASSESSMENT_DATE),
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
                    pl.col("num_candidate").alias(C.OFF_TARGET_LESIONS_NUMBER),
                    pl.col("date_candidate").alias(C.OFF_TARGET_LESION_MEASUREMENT_DATE),
                )
            )

        # earliest row with value and date across RNRSP & RA
        def target_lesions_baseline(df: pl.DataFrame) -> pl.DataFrame:
            return (
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
                    pl.col("date_candidate").alias(C.TARGET_LESION_MEASUREMENT_DATE),
                    pl.col("size_candidate").alias(C.TARGET_LESION_SIZE),
                    pl.col("nadir_candidate").alias(C.TARGET_LESION_NADIR),
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
                C.ASSESSMENT_TYPE,
                C.ASSESSMENT_DATE,
                C.TARGET_LESION_SIZE,
                C.TARGET_LESION_NADIR,
                C.TARGET_LESION_MEASUREMENT_DATE,
                C.OFF_TARGET_LESIONS_NUMBER,
                C.OFF_TARGET_LESION_MEASUREMENT_DATE,
            )
        )

        return joined

    def _process_tumor_assessments(self) -> pl.DataFrame | None:
        C = TumorAssessment.Cols
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
                    ).alias(C.ASSESSMENT_TYPE),
                )
                .with_columns(
                    pl.coalesce([pl.col("RA_RABASECH"), pl.col("RNRSP_TERNCFB")]).cast(pl.Float64, strict=False).alias("_tl_change_baseline"),
                    pl.coalesce([pl.col("RA_RARECCH"), pl.col("RNRSP_TERNCFN")]).cast(pl.Float64, strict=False).alias("_tl_change_nadir"),
                    pl.coalesce([pl.col("RA_EventId"), pl.col("RNRSP_EventId")]).cast(pl.Utf8, strict=False).alias(C.EVENT_ID),
                )
                .with_columns(
                    (
                        pl.when(pl.col("_tl_change_baseline").is_null())
                        .then(None)
                        .when(pl.col("_tl_change_baseline") == 0)
                        .then(0)
                        .otherwise(pl.col("_tl_change_baseline") / 100)
                    ).alias(C.TARGET_LESION_CHANGE_FROM_BASELINE),
                    (
                        pl.when(pl.col("_tl_change_nadir").is_null())
                        .then(None)
                        .when(pl.col("_tl_change_nadir") == 0)
                        .then(0)
                        .otherwise(pl.col("_tl_change_nadir") / 100)
                    ).alias(C.TARGET_LESION_CHANGE_FROM_NADIR),
                )
                .with_columns(
                    PolarsParsers.int_to_bool(
                        x=pl.coalesce([pl.col("RA_RANLBASECD"), pl.col("RNRSP_RNRSPNLCD")]).cast(pl.Int64, strict=False),
                        true_int=1,
                        false_int=0,
                    ).alias(C.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE),
                )
                .with_columns(PolarsParsers.to_optional_date(pl.coalesce([pl.col("RA_EventDate"), pl.col("RNRSP_EventDate")])).alias(C.DATE))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("RA_RATIMRES")).str.strip_chars().alias(C.RECIST_RESPONSE),
                    PolarsParsers.to_optional_utf8(pl.col("RA_RAiMOD")).str.strip_chars().alias(C.IRECIST_RESPONSE),
                    PolarsParsers.to_optional_utf8(pl.col("RNRSP_RNRSPCL")).str.strip_chars().alias(C.RANO_RESPONSE),
                    PolarsParsers.to_optional_date(pl.col("RA_RAPROGDT")).alias(C.RECIST_DATE_OF_PROGRESSION),
                    PolarsParsers.to_optional_date(pl.col("RA_RAiUNPDT")).alias(C.IRECIST_DATE_OF_PROGRESSION),
                )
                # keep only rows with real signal
                .with_columns(
                    has_any=pl.any_horizontal(
                        [
                            pl.col(C.ASSESSMENT_TYPE).str.len_bytes() > 0,
                            pl.col(C.TARGET_LESION_CHANGE_FROM_BASELINE).is_not_null(),
                            pl.col(C.TARGET_LESION_CHANGE_FROM_NADIR).is_not_null(),
                            pl.col(C.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE).is_not_null(),
                            pl.col(C.DATE).is_not_null(),
                            pl.col(C.RECIST_RESPONSE).str.len_bytes() > 0,
                            pl.col(C.IRECIST_RESPONSE).str.len_bytes() > 0,
                            pl.col(C.RANO_RESPONSE).str.len_bytes() > 0,
                            pl.col(C.RECIST_DATE_OF_PROGRESSION).is_not_null(),
                            pl.col(C.IRECIST_DATE_OF_PROGRESSION).is_not_null(),
                        ],
                    ),
                )
                .filter(pl.col("has_any"))
                .select(
                    "SubjectId",
                    C.ASSESSMENT_TYPE,
                    C.TARGET_LESION_CHANGE_FROM_BASELINE,
                    C.TARGET_LESION_CHANGE_FROM_NADIR,
                    C.WAS_NEW_LESIONS_REGISTERED_AFTER_BASELINE,
                    C.DATE,
                    C.RECIST_RESPONSE,
                    C.IRECIST_RESPONSE,
                    C.RANO_RESPONSE,
                    C.RECIST_DATE_OF_PROGRESSION,
                    C.IRECIST_DATE_OF_PROGRESSION,
                    C.EVENT_ID,
                )
            )

            return _processed

        return process(base)

    # TODO: refactor to not use regex later
    def _process_c30(self) -> pl.DataFrame | None:
        C = C30.Cols
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

            # Build column list using Cols constants
            q_text_cols = [getattr(C, f"Q{i}") for i in range(1, C30.Q_COUNT + 1)]
            q_code_cols = [getattr(C, f"Q{i}_CODE") for i in range(1, C30.Q_COUNT + 1)]

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("C30_EventName")).str.strip_chars().alias(C.EVENT_NAME),
                    PolarsParsers.to_optional_date(pl.col("C30_EventDate")).alias(C.DATE),
                    *[PolarsParsers.to_optional_utf8(pl.col(c)).str.strip_chars().alias(q_alias(c)) for c in text_cols],
                    *[PolarsParsers.to_optional_int64(pl.col(c)).alias(q_alias(c)) for c in code_cols],
                )
                .select("SubjectId", C.DATE, C.EVENT_NAME, *q_text_cols, *q_code_cols)
            )

            return out

        processed = process_c30(frame=base)
        return processed

    # TODO: refactor to not use regex later
    def _process_eq5d(self) -> pl.DataFrame | None:
        C = EQ5D.Cols
        question_col_re = re.compile(r"^EQ5D_EQ5D([1-5])$")
        question_code_re = re.compile(r"^(?:EQ5D_)?EQ5D([1-5])CD$")

        # Build column lists using Cols constants
        q_text_cols = [getattr(C, f"Q{i}") for i in range(1, EQ5D.Q_COUNT + 1)]
        q_code_cols = [getattr(C, f"Q{i}_CODE") for i in range(1, EQ5D.Q_COUNT + 1)]

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
                    return getattr(C, f"Q{m.group(1)}")
                m = question_code_re.fullmatch(col)
                if m:
                    return getattr(C, f"Q{m.group(1)}_CODE")
                return col

            out = (
                frame.filter(pl.any_horizontal(pl.all().exclude("SubjectId").is_not_null()))
                .with_columns(
                    PolarsParsers.to_optional_utf8(pl.col("EQ5D_EventName")).str.strip_chars().alias(C.EVENT_NAME),
                    PolarsParsers.to_optional_date(pl.col("EQ5D_EventDate")).alias(C.DATE),
                    PolarsParsers.to_optional_int64(pl.col("EQ5D_EQ5DVAS")).alias(C.QOL_METRIC),
                    *[PolarsParsers.to_optional_utf8(pl.col(c)).str.strip_chars().alias(q_alias(c)) for c in text_cols],
                    *[PolarsParsers.to_optional_int64(pl.col(c)).alias(q_alias(c)) for c in code_cols],
                )
                .select("SubjectId", C.DATE, C.EVENT_NAME, C.QOL_METRIC, *q_text_cols, *q_code_cols)
            )

            return out

        processed = process_eq5d(frame=base)
        return processed

    def _process_best_overall_response(self) -> pl.DataFrame | None:
        """
        Takes the lowest value of the response code across all tumor assessments for each patient,
        i.e. selects the best response across entire treatment duration.
        Also assumes tumor evaluations are mutually exclusive.
        Removes unconfirmed iRecist responses, and takes best response across Recist and iRecist when
        rows have both evaluations.
        """
        C = BestOverallResponse.Cols
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
                    pl.coalesce("recist_text", "RNRSP_RNRSPCL").cast(pl.Utf8, strict=False).str.strip_chars().alias(C.RESPONSE),
                    pl.coalesce("recist_code", "RNRSP_RNRSPCLCD").cast(pl.Int64, strict=False).alias(C.CODE),
                    PolarsParsers.to_optional_date(pl.coalesce("RA_EventDate", "RNRSP_EventDate")).alias(C.DATE),
                )
                .filter(pl.col(C.CODE).is_not_null())
                .sort(["SubjectId", C.CODE])
                .group_by("SubjectId", maintain_order=True)
                .first()
                .select("SubjectId", C.RESPONSE, C.CODE, C.DATE)
            )

            return result

        return process(base)
