from dataclasses import dataclass, asdict
from typing import List

import pytest
import polars as pl

# TODO: also store full mock data for trials and use in intragation test
#   and maybe move fixture to conf test later if needed


@pytest.fixture
def subject_id_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
                "IMPRESS-X_0005_1",
                "IMPRESS-X_0005_2",
            ],
        },
    )


@pytest.fixture
def cohort_name_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_COHORTNAME": [
                "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib",
                "",
                "",
                "",
                "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab",
            ],
        },
    )


@pytest.fixture
def age_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "DM_BRTHDAT": ["1900-06-02", "1950", "2000-02-14", "1970", "1990-12-07"],
            "TR_TRC1_DT": ["1990-01-30", "1990-01-01", "2020-03-25", "2000", "2000-01"],
        },
    )


@pytest.fixture
def gender_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
                "IMPRESS-X_0006_1",
                "IMPRESS-X_0007_1",
                "IMPRESS-X_0008_1",
            ],
            "DM_SEX": ["Female", "Male", "f", "m", "error", "", "female", "male"],
        },
    )


@pytest.fixture
def tumor_type_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_ICD10COD": ["C30", "C40.50", "C07", "C70.1", "C23.20"],
            "COH_ICD10DES": ["tumor1", "CRC", "tumor2", "tumor3", "tumor4"],
            "COH_COHTTYPE": [
                "tumor1_subtype1",
                "",
                "tumor2_subtype1",
                "tumor3_subtype1",
                "",
            ],
            "COH_COHTTYPECD": [50, None, 70, 10, None],
            "COH_COHTTYPE__2": ["", "CRC_subtype", "", "", "tumor4_subtype1"],
            "COH_COHTTYPE__2CD": [None, 40, None, None, 30],
            "COH_COHTT": ["tumor1_subtype2", "", "tumor2_subtype2", "", ""],
            "COH_COHTTOSP": [
                "tumor1_subtype3",
                "",
                "",
                "tumor3_subtype2",
                "tumor4_subtype2",
            ],
        },
    )


@pytest.fixture
def study_drugs_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            # sd1
            "COH_COHALLO1": ["", "some drug", "mismatch_1", "", "collision"],
            "COH_COHALLO1CD": ["", "99", "10", "", ""],
            "COH_COHALLO1__2": ["Traztuzumab", "", "", "mismatch_2", ""],
            "COH_COHALLO1__2CD": ["31", "", "", "50", ""],
            "COH_COHALLO1__3": ["", "", "", "", "some_drug_3"],
            "COH_COHALLO1__3CD": ["", "", "", "", "99"],
            # sd2
            "COH_COHALLO2": ["", "some drug 2", "", "mismatch_2_1", ""],
            "COH_COHALLO2CD": ["", "1", "", "60", ""],
            "COH_COHALLO2__2": ["Tafinlar", "", "mismatch_1_2", "", ""],
            "COH_COHALLO2__2CD": ["10", "", "12", "", "5"],
            "COH_COHALLO2__3": ["", "", "", "", "some_drug_3_2"],
            "COH_COHALLO2__3CD": ["", "", "", "", "999"],
        },
    )


@pytest.fixture
def biomarker_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "COH_GENMUT1": [
                "BRAF activating mutations",
                "",
                "BRCA1 inactivating mutation",
                "SDHAF2 mutation",
                "",
            ],
            "COH_GENMUT1CD": [21, None, 2, -1, 10],
            "COH_COHCTN": [
                "BRAF Non-V600 activating mutations",
                "some info",
                "BRCA1 stop-gain del exon 11",
                "more info",
                "",
            ],
            "COH_COHTMN": [
                "BRAF Non-V600 activating mutations",
                "",
                "BRCA1 stop-gain deletion",
                "",
                "some other info",
            ],
            "COH_EventDate": [
                "1900-nk-nk",
                "1980-02-nk",
                "not a date",
                "1999-nk-11",
                "",
            ],
        },
    )


@pytest.fixture
def date_of_death_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "EOS_DEATHDTC": [
                "1990-nk-02",
                "1961-09-12",
                "",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": [
                "1990-nk-02",
                "2016-09-nk",
                "1900-01-01",
                "1999-NK-NK",
                "invalid date",
            ],
        },
    )


@pytest.fixture
def lost_to_followup_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "FU_FUPALDAT": [
                "1990-10-02",
                "",
                "1900-01-01",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": ["", "1980-09-12", "", "", "invalid date"],
            "FU_FUPSST": ["Alive", "Death", "lost to follow up", "alive", ""],
            "FU_FUPSSTCD": ["1", "2", "3", "", ""],
        },
    )


@dataclass(frozen=True, slots=True)
class EvaluabilityRow:
    # todo: refactor to str when processing method is refactored to polars
    SubjectId: str
    TR_TRTNO: int | None = None
    TR_TRC1_DT: str | None = None
    TR_TRO_STDT: str | None = None
    TR_TROSTPDT: str | None = None
    TR_TRCYNCD: int | None = None


@pytest.fixture
def evaluability_fixture() -> pl.DataFrame:
    rows: List[EvaluabilityRow] = [
        EvaluabilityRow(
            "iv_single",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_a",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_a",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-15",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_b",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_b",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-22",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_then_oral",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_then_oral",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="2001-01-31",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_then_oral_short",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_then_oral_short",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-02-05",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_then_oral_short",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="2001-01-10",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "oral_ongoing_a",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "oral_ongoing_b",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "oral_missing_start_a",
            TR_TRO_STDT="",
            TR_TROSTPDT="2001-02-05",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "oral_missing_start_b",
            TR_TRO_STDT="",
            TR_TROSTPDT="2001-02-05",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_then_iv_empty_date",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_then_iv_empty_date",
            TR_TRTNO=1,
            TR_TRC1_DT="",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_gap",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_rows_gap",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-21",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "oral_only",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="2001-01-20",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_courses",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_two_courses",
            TR_TRTNO=2,
            TR_TRC1_DT="2001-02-05",
            TR_TRCYNCD=1,
        ),
        EvaluabilityRow(
            "iv_with_cyclic_and_non",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-01-01",
            TR_TRCYNCD=0,
        ),
        EvaluabilityRow(
            "iv_with_cyclic_and_non",
            TR_TRTNO=1,
            TR_TRC1_DT="2001-02-05",
            TR_TRCYNCD=0,
        ),
        EvaluabilityRow(
            "oral_non_cyclic",
            TR_TRO_STDT="2001-01-01",
            TR_TROSTPDT="2001-02-10",
            TR_TRCYNCD=0,
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class EcogRow:
    SubjectId: str
    ECOG_EventId: str | None = None
    ECOG_ECOGS: str | None = None
    ECOG_ECOGSCD: str | None = None
    ECOG_ECOGDAT: str | None = None


@pytest.fixture
def ecog_fixture() -> pl.DataFrame:
    rows: List[EcogRow] = [
        EcogRow(
            "all_data",
            ECOG_EventId="V00",
            ECOG_ECOGS="all",
            ECOG_ECOGSCD="1",
            ECOG_ECOGDAT="1900-01-01",
        ),
        EcogRow(
            "eventid_no_code",
            ECOG_EventId="V00",
            ECOG_ECOGS="no code",
            ECOG_ECOGSCD="",
            ECOG_ECOGDAT="1900-nk-01",
        ),
        EcogRow(
            "eventid_no_desc",
            ECOG_EventId="V00",
            ECOG_ECOGS="",
            ECOG_ECOGSCD="2",
            ECOG_ECOGDAT="1900-01-nk",
        ),
        EcogRow(
            "wrong_event_id",
            ECOG_EventId="V02",
            ECOG_ECOGS="wrong ID",
            ECOG_ECOGSCD="3",
            ECOG_ECOGDAT="",
        ),
        EcogRow(
            "no_event_id",
            ECOG_EventId="",
            ECOG_ECOGS="",
            ECOG_ECOGSCD="",
            ECOG_ECOGDAT="",
        ),
        EcogRow(
            "partial_data",
            ECOG_EventId="V00",
            ECOG_ECOGS="",
            ECOG_ECOGSCD="1",
            ECOG_ECOGDAT="1900-nk-nk",
        ),
        EcogRow(
            "wrong_date",
            ECOG_EventId="V00",
            ECOG_ECOGS="code",
            ECOG_ECOGSCD="4",
            ECOG_ECOGDAT="not a date",
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class MedicalHistoryRow:
    SubjectId: str
    MH_MHTERM: str | None = None
    MH_MHSPID: str | None = None
    MH_MHSTDAT: str | None = None
    MH_MHENDAT: str | None = None
    MH_MHONGO: str | None = None
    MH_MHONGOCD: str | None = None


@pytest.fixture
def medical_history_fixture():
    rows: List[MedicalHistoryRow] = [
        MedicalHistoryRow(
            "two_rows",
            MH_MHTERM="pain",
            MH_MHSPID="01",
            MH_MHSTDAT="1900-09-NK",
            MH_MHENDAT="",
            MH_MHONGO="Current/active",
            MH_MHONGOCD="1",
        ),
        MedicalHistoryRow(
            "two_rows",
            MH_MHTERM="something",
            MH_MHSPID="05",
            MH_MHSTDAT="1900-nk-02",
            MH_MHENDAT="1990-01-01",
            MH_MHONGO="Past",
            MH_MHONGOCD="3",
        ),
        MedicalHistoryRow(
            "ended",
            MH_MHTERM="hypertension",
            MH_MHSPID="02",
            MH_MHSTDAT="1901-10-02",
            MH_MHENDAT="1901-11-02",
            MH_MHONGO="Past",
            MH_MHONGOCD="3",
        ),
        MedicalHistoryRow(
            "ended_term_mismatch",
            MH_MHTERM="pain",
            MH_MHSPID="01",
            MH_MHSTDAT="1840-02-02",
            MH_MHENDAT="",
            MH_MHONGO="Past",
            MH_MHONGOCD="3",
        ),
        MedicalHistoryRow(
            "ended_code_mismatch",
            MH_MHTERM="rigor mortis",
            MH_MHSPID="01",
            MH_MHSTDAT="1740-02-02",
            MH_MHENDAT="1940-02-02",
            MH_MHONGO="Past",
            MH_MHONGOCD="1",
        ),
        MedicalHistoryRow("missing"),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class AdverseEventNumberRow:
    SubjectId: str
    AE_AETOXGRECD: str | None = None
    AE_AECTCAET: str | None = None
    AE_AESTDAT: str | None = None


@pytest.fixture
def adverse_event_number_fixture():
    rows: List[AdverseEventNumberRow] = [
        AdverseEventNumberRow(
            "2_events",
            AE_AETOXGRECD="3",
            AE_AECTCAET="ouch",
            AE_AESTDAT="1900-01-01",
        ),
        AdverseEventNumberRow(
            "2_events",
            AE_AETOXGRECD="2",
            AE_AECTCAET="owe",
            AE_AESTDAT="",
        ),
        AdverseEventNumberRow(
            "3_events",
            AE_AETOXGRECD="1",
            AE_AECTCAET="",
            AE_AESTDAT="",
        ),
        AdverseEventNumberRow(
            "3_events",
            AE_AETOXGRECD="",
            AE_AECTCAET="something",
            AE_AESTDAT="1900-01-01",
        ),
        AdverseEventNumberRow(
            "3_events",
            AE_AETOXGRECD="",
            AE_AECTCAET="else",
            AE_AESTDAT="1889-02-23",
        ),
        AdverseEventNumberRow(
            "1_event_code_only",
            AE_AETOXGRECD="4",
            AE_AECTCAET="",
            AE_AESTDAT="",
        ),
        AdverseEventNumberRow(
            "1_event_term_only",
            AE_AETOXGRECD="",
            AE_AECTCAET="rash",
            AE_AESTDAT="1900-01-01",
        ),
        AdverseEventNumberRow(
            "missing_data",
            AE_AETOXGRECD="",
            AE_AECTCAET="",
            AE_AESTDAT="",
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class SeriousAdverseEventNumberRow:
    SubjectId: str
    AE_AESERCD: str | None = None
    AE_SAESTDAT: str | None = None


@pytest.fixture
def serious_adverse_event_number_fixture():
    rows: List[SeriousAdverseEventNumberRow] = [
        SeriousAdverseEventNumberRow(
            "1_event_two_rows",
            AE_AESERCD="1",
            AE_SAESTDAT="1900-01-01",
        ),
        SeriousAdverseEventNumberRow(
            "1_event_two_rows",
            AE_AESERCD="0",
            AE_SAESTDAT="1900-01-01",
        ),
        SeriousAdverseEventNumberRow(
            "2_events_with_missing_fields",
            AE_AESERCD="1",
            AE_SAESTDAT="",
        ),
        SeriousAdverseEventNumberRow(
            "2_events_with_missing_fields",
            AE_AESERCD="1",
            AE_SAESTDAT="1900-02-02",
        ),
        SeriousAdverseEventNumberRow(
            "2_events_with_missing_fields",
            AE_AESERCD="",
            AE_SAESTDAT="1900-03-03",
        ),
        SeriousAdverseEventNumberRow(
            "1_event_missing_date",
            AE_AESERCD="1",
            AE_SAESTDAT="",
        ),
        SeriousAdverseEventNumberRow(
            "0_events_missing_date",
            AE_AESERCD="0",
            AE_SAESTDAT="",
        ),
        SeriousAdverseEventNumberRow("0_events_no_data", AE_AESERCD="", AE_SAESTDAT=""),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class BaselineTumorAssessmentRow:
    SubjectId: str
    # VI
    VI_VITUMA: str | None = None
    VI_VITUMA__2: str | None = None
    VI_EventDate: str | None = None
    VI_EventId: str | None = None
    # RCNT / RNTMNT
    RCNT_RCNTNOB: str | None = None
    RCNT_EventDate: str | None = None
    RCNT_EventId: str | None = None
    RNTMNT_RNTMNTNOB: str | None = None
    RNTMNT_RNTMNTNO: str | None = None
    RNTMNT_EventId: str | None = None
    RNTMNT_EventDate: str | None = None
    # RNRSP / RA
    RNRSP_TERNTBAS: str | None = None
    RNRSP_TERNAD: str | None = None
    RNRSP_EventDate: str | None = None
    RNRSP_EventId: str | None = None
    RA_RARECBAS: str | None = None
    RA_RARECNAD: str | None = None
    RA_EventDate: str | None = None
    RA_EventId: str | None = None


@pytest.fixture
def baseline_tumor_assessment_fixture() -> pl.DataFrame:
    rows: List[BaselineTumorAssessmentRow] = [
        BaselineTumorAssessmentRow("missing_data"),
        # VI cases
        BaselineTumorAssessmentRow(
            "vituma_only",
            VI_VITUMA="PD",
            VI_EventDate="2020-01-02",
            VI_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "vituma__2_only",
            VI_VITUMA__2="CR",
            VI_EventDate="2020-01-03",
            VI_EventId="V00",
        ),
        BaselineTumorAssessmentRow("vi_none"),
        BaselineTumorAssessmentRow(
            "vi_no_date",
            VI_VITUMA="SD",
            VI_EventId="V00",
        ),
        # non-target lesions (RCNT/RNTMNT)
        BaselineTumorAssessmentRow("no_ntl"),
        BaselineTumorAssessmentRow(
            "both_ntl_cols",
            RNTMNT_RNTMNTNOB="5",
            RNTMNT_RNTMNTNO="7",
            RNTMNT_EventId="V00",
            RNTMNT_EventDate="2020-02-01",
        ),
        BaselineTumorAssessmentRow(
            "rntmnt_only",
            RNTMNT_RNTMNTNO="4",
            RNTMNT_EventId="V00",
            RNTMNT_EventDate="2020-02-02",
        ),
        BaselineTumorAssessmentRow(
            "rntmnt_ntl_wrong_event_id",
            RNTMNT_RNTMNTNOB="3",
            RNTMNT_EventId="V01",
            RNTMNT_EventDate="2020-02-03",
        ),
        BaselineTumorAssessmentRow(
            "rcnt_only",
            RCNT_RCNTNOB="3",
            RCNT_EventId="V00",
            RCNT_EventDate="2020-02-04",
        ),
        BaselineTumorAssessmentRow(
            "rcnt_invalid_int",
            RCNT_RCNTNOB="abc",
            RCNT_EventId="V00",
            RCNT_EventDate="2020-02-05",
        ),
        BaselineTumorAssessmentRow(
            "ntl_no_date",
            RNTMNT_RNTMNTNOB="6",
            RNTMNT_EventId="V00",
        ),
        # target lesions (RA/RNRSP)
        BaselineTumorAssessmentRow(
            "ra_valid",
            RA_RARECBAS="12",
            RA_RARECNAD="12",
            RA_EventDate="2018-07-27",
            RA_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "rnrsp_valid",
            RNRSP_TERNTBAS="20",
            RNRSP_TERNAD="18",
            RNRSP_EventDate="2019-01-01",
            RNRSP_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "ra_no_date",
            RA_RARECBAS="8",
            RA_RARECNAD="7",
            RA_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "rnrsp_no_date",
            RNRSP_TERNTBAS="9",
            RNRSP_TERNAD="8",
            RNRSP_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "missing_baseline_size",
            RA_RARECNAD="11",
            RA_EventDate="2020-03-01",
            RA_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "multiple_rows",
            RA_RARECBAS="10",
            RA_RARECNAD="10",
            RA_EventDate="2020-01-03",
            RA_EventId="V00",
        ),
        BaselineTumorAssessmentRow(
            "multiple_rows",
            RA_RARECBAS="9",
            RA_RARECNAD="9",
            RA_EventDate="2020-01-01",
            RA_EventId="V00",
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class PreviousTreatmentRow:
    SubjectId: str
    CT_CTTYPE: str | None = None
    CT_CTTYPECD: str | None = None
    CT_CTSPID: str | None = None
    CT_CTSTDAT: str | None = None
    CT_CTENDAT: str | None = None
    CT_CTTYPESP: str | None = None


@pytest.fixture
def previous_treatment_fixture() -> pl.DataFrame:
    rows: List[PreviousTreatmentRow] = [
        PreviousTreatmentRow("empty"),
        PreviousTreatmentRow(
            "has_treatment",
            CT_CTTYPE="abc",
            CT_CTTYPECD="2",
            CT_CTSPID="1",
            CT_CTSTDAT="1900-01-01",
            CT_CTENDAT="1900-01-02",
            CT_CTTYPESP="def",
        ),
        PreviousTreatmentRow(
            "missing_treatment",
            CT_CTTYPECD="2",
            CT_CTSPID="1",
            CT_CTSTDAT="1900-01-01",
            CT_CTENDAT="1900-01-02",
            CT_CTTYPESP="def",
        ),
        PreviousTreatmentRow(
            "missing_partial",
            CT_CTTYPE="abc",
            CT_CTSPID="1",
            CT_CTSTDAT="1900-01-01",
        ),
        PreviousTreatmentRow(
            "missing_partial",
            CT_CTTYPE="def",
            CT_CTSPID="2",
            CT_CTSTDAT="1900-01-03",
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class TreatmentStartRow:
    SubjectId: str
    TR_TRNAME: str | None = None
    TR_TRC1_DT: str | None = None


@pytest.fixture
def treatment_start_fixture() -> pl.DataFrame:
    rows: List[TreatmentStartRow] = [
        TreatmentStartRow("empty"),
        TreatmentStartRow(
            "single_row",
            TR_TRNAME="b",
            TR_TRC1_DT="1900-01-02",
        ),
        TreatmentStartRow(
            "multirow",
            TR_TRNAME="a",
            TR_TRC1_DT="1900-01-03",
        ),
        TreatmentStartRow(
            "multirow",
            TR_TRNAME="a",
            TR_TRC1_DT="2001-01-01",
        ),
        TreatmentStartRow(
            "multirow",
            TR_TRNAME="a",
            TR_TRC1_DT="1900-01-01",
        ),
        TreatmentStartRow(
            "missing_treatment_none",
            TR_TRNAME=None,
            TR_TRC1_DT="1900-01-03",
        ),
        TreatmentStartRow(
            "missing_treatment_empty_str",
            TR_TRNAME="",
            TR_TRC1_DT="1900-01-03",
        ),
    ]

    records = [asdict(r) for r in rows]
    return pl.from_dicts(records)


@pytest.fixture
def treatment_end_fixture():
    rows = []

    def base_row(pid):
        return {"SubjectId": pid, "TR_TRNAME": None, "TR_TRC1_DT": None}

    # all empty
    rows.append(base_row("empty"))
