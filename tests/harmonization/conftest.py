from dataclasses import dataclass, asdict
from typing import List
import pytest
import polars as pl


@dataclass(frozen=True, slots=True)
class SubjectIdRow:
    SubjectId: str


@pytest.fixture
def subject_id_fixture() -> pl.DataFrame:
    rows: List[SubjectIdRow] = [
        SubjectIdRow("unique_1"),
        SubjectIdRow("unique_2"),
        SubjectIdRow("unique_3"),
        SubjectIdRow("unique_4"),
        SubjectIdRow("duplicate_id"),
        SubjectIdRow("duplicate_id"),
        SubjectIdRow("duplicate_variant"),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class CohortNameRow:
    SubjectId: str
    COH_COHORTNAME: str | None = None


@pytest.fixture
def cohort_name_fixture() -> pl.DataFrame:
    rows: List[CohortNameRow] = [
        CohortNameRow(
            "cohort_hit_1",
            "BRAF Non-V600mut/Pancreatic/Trametinib+Dabrafenib",
        ),
        CohortNameRow(
            "cohort_empty_1",
            None,
        ),
        CohortNameRow(
            "cohort_empty_2",
            "",
        ),
        CohortNameRow(
            "cohort_empty_3",
        ),
        CohortNameRow(
            "cohort_hit_2",
            "HER2exp/Cholangiocarcinoma/Pertuzumab+Traztuzumab",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class AgeRow:
    SubjectId: str
    DM_BRTHDAT: str | None = None
    TR_TRC1_DT: str | None = None


@pytest.fixture
def age_fixture() -> pl.DataFrame:
    rows: List[AgeRow] = [
        AgeRow(
            "birth_full_tx_full",
            DM_BRTHDAT="1900-06-02",
            TR_TRC1_DT="1990-01-30",
        ),
        AgeRow(
            "birth_year_tx_full",
            DM_BRTHDAT="1950",
            TR_TRC1_DT="1990-01-01",
        ),
        AgeRow(
            "birth_full_tx_full_recent",
            DM_BRTHDAT="2000-02-14",
            TR_TRC1_DT="2020-03-25",
        ),
        AgeRow(
            "birth_year_tx_year",
            DM_BRTHDAT="1970",
            TR_TRC1_DT="2000",
        ),
        AgeRow(
            "birth_full_tx_year_month",
            DM_BRTHDAT="1990-12-07",
            TR_TRC1_DT="2000-01",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class GenderRow:
    SubjectId: str
    DM_SEX: str | None = None


@pytest.fixture
def gender_fixture() -> pl.DataFrame:
    rows: List[GenderRow] = [
        GenderRow(
            "female_titlecase",
            DM_SEX="Female",
        ),
        GenderRow(
            "male_titlecase",
            DM_SEX="Male",
        ),
        GenderRow(
            "female_short_f",
            DM_SEX="f",
        ),
        GenderRow(
            "male_short_m",
            DM_SEX="m",
        ),
        GenderRow(
            "invalid_value",
            DM_SEX="error",
        ),
        GenderRow(
            "empty_value",
            DM_SEX="",
        ),
        GenderRow(
            "female_lowercase",
            DM_SEX="female",
        ),
        GenderRow(
            "male_lowercase",
            DM_SEX="male",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class TumorTypeRow:
    SubjectId: str
    COH_ICD10COD: str | None = None
    COH_ICD10DES: str | None = None
    COH_COHTTYPE: str | None = None
    COH_COHTTYPECD: int | None = None
    COH_COHTTYPE__2: str | None = None
    COH_COHTTYPE__2CD: int | None = None
    COH_COHTT: str | None = None
    COH_COHTTOSP: str | None = None


@pytest.fixture
def tumor_type_fixture() -> pl.DataFrame:
    rows: List[TumorTypeRow] = [
        TumorTypeRow(
            "tumor1_multi_subtypes",
            COH_ICD10COD="C30",
            COH_ICD10DES="tumor1",
            COH_COHTTYPE="tumor1_subtype1",
            COH_COHTTYPECD=50,
            COH_COHTT="tumor1_subtype2",
            COH_COHTTOSP="tumor1_subtype3",
        ),
        TumorTypeRow(
            "crc_subtype_slot2",
            COH_ICD10COD="C40.50",
            COH_ICD10DES="CRC",
            COH_COHTTYPECD=None,
            COH_COHTTYPE__2="CRC_subtype",
            COH_COHTTYPE__2CD=40,
        ),
        TumorTypeRow(
            "tumor2_dual_subtypes",
            COH_ICD10COD="C07",
            COH_ICD10DES="tumor2",
            COH_COHTTYPE="tumor2_subtype1",
            COH_COHTTYPECD=70,
            COH_COHTT="tumor2_subtype2",
        ),
        TumorTypeRow(
            "tumor3_sp_subtype",
            COH_ICD10COD="C70.1",
            COH_ICD10DES="tumor3",
            COH_COHTTYPE="tumor3_subtype1",
            COH_COHTTYPECD=10,
            COH_COHTTOSP="tumor3_subtype2",
        ),
        TumorTypeRow(
            "tumor4_slot2_and_sp",
            COH_ICD10COD="C23.20",
            COH_ICD10DES="tumor4",
            COH_COHTTYPECD=None,
            COH_COHTTYPE__2="tumor4_subtype1",
            COH_COHTTYPE__2CD=30,
            COH_COHTTOSP="tumor4_subtype2",
        ),
    ]

    recrods = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(recrods)


@dataclass(frozen=True, slots=True)
class StudyDrugsRow:
    SubjectId: str
    # sd1
    COH_COHALLO1: str | None = None
    COH_COHALLO1CD: str | None = None
    COH_COHALLO1__2: str | None = None
    COH_COHALLO1__2CD: str | None = None
    COH_COHALLO1__3: str | None = None
    COH_COHALLO1__3CD: str | None = None
    # sd2
    COH_COHALLO2: str | None = None
    COH_COHALLO2CD: str | None = None
    COH_COHALLO2__2: str | None = None
    COH_COHALLO2__2CD: str | None = None
    COH_COHALLO2__3: str | None = None
    COH_COHALLO2__3CD: str | None = None


@pytest.fixture
def study_drugs_fixture() -> pl.DataFrame:
    rows: List[StudyDrugsRow] = [
        StudyDrugsRow(
            "sd_from_alt_slots",
            COH_COHALLO1__2="Traztuzumab",
            COH_COHALLO1__2CD="31",
            COH_COHALLO2__2="Tafinlar",
            COH_COHALLO2__2CD="10",
        ),
        StudyDrugsRow(
            "sd1_match_sd2_match",
            COH_COHALLO1="some drug",
            COH_COHALLO1CD="99",
            COH_COHALLO2="some drug 2",
            COH_COHALLO2CD="1",
        ),
        StudyDrugsRow(
            "sd1_mismatch1_sd2_mismatch1_2",
            COH_COHALLO1="mismatch_1",
            COH_COHALLO1CD="10",
            COH_COHALLO2__2="mismatch_1_2",
            COH_COHALLO2__2CD="12",
        ),
        StudyDrugsRow(
            "sd1_mismatch2_sd2_mismatch2_1",
            COH_COHALLO1__2="mismatch_2",
            COH_COHALLO1__2CD="50",
            COH_COHALLO2="mismatch_2_1",
            COH_COHALLO2CD="60",
        ),
        StudyDrugsRow(
            "sd_collision",
            COH_COHALLO1="collision",
            COH_COHALLO1__3="some_drug_3",
            COH_COHALLO1__3CD="99",
            COH_COHALLO2__2CD="5",
            COH_COHALLO2__3="some_drug_3_2",
            COH_COHALLO2__3CD="999",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class BiomarkersRow:
    SubjectId: str
    COH_GENMUT1: str | None = None
    COH_GENMUT1CD: int | None = None
    COH_COHCTN: str | None = None
    COH_COHTMN: str | None = None
    COH_EventDate: str | None = None


@pytest.fixture
def biomarkers_fixture() -> pl.DataFrame:
    rows: List[BiomarkersRow] = [
        BiomarkersRow(
            "mut_braf_activating",
            COH_GENMUT1="BRAF activating mutations",
            COH_GENMUT1CD=21,
            COH_COHCTN="BRAF Non-V600 activating mutations",
            COH_COHTMN="BRAF Non-V600 activating mutations",
            COH_EventDate="1900-nk-nk",
        ),
        BiomarkersRow(
            "some_info_no_mut",
            COH_GENMUT1="",
            COH_GENMUT1CD=None,
            COH_COHCTN="some info",
            COH_COHTMN="",
            COH_EventDate="1980-02-nk",
        ),
        BiomarkersRow(
            "brca1_inactivating",
            COH_GENMUT1="BRCA1 inactivating mutation",
            COH_GENMUT1CD=2,
            COH_COHCTN="BRCA1 stop-gain del exon 11",
            COH_COHTMN="BRCA1 stop-gain deletion",
            COH_EventDate="not a date",
        ),
        BiomarkersRow(
            "sdhaf2_mut",
            COH_GENMUT1="SDHAF2 mutation",
            COH_GENMUT1CD=-1,
            COH_COHCTN="more info",
            COH_COHTMN="",
            COH_EventDate="1999-nk-11",
        ),
        BiomarkersRow(
            "code_only_misc",
            COH_GENMUT1="",
            COH_GENMUT1CD=10,
            COH_COHCTN="",
            COH_COHTMN="some other info",
            COH_EventDate="",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class DateOfDeathRow:
    SubjectId: str
    EOS_DEATHDTC: str | None = None
    FU_FUPDEDAT: str | None = None


@pytest.fixture
def date_of_death_fixture() -> pl.DataFrame:
    rows: List[DateOfDeathRow] = [
        DateOfDeathRow(
            "both_partial_nk",
            EOS_DEATHDTC="1990-nk-02",
            FU_FUPDEDAT="1990-nk-02",
        ),
        DateOfDeathRow(
            "eos_valid_fu_partial_nk",
            EOS_DEATHDTC="1961-09-12",
            FU_FUPDEDAT="2016-09-nk",
        ),
        DateOfDeathRow(
            "fu_valid_only",
            EOS_DEATHDTC="",
            FU_FUPDEDAT="1900-01-01",
        ),
        DateOfDeathRow(
            "eos_valid_fu_partial_upper_nk",
            EOS_DEATHDTC="1999-09-09",
            FU_FUPDEDAT="1999-NK-NK",
        ),
        DateOfDeathRow(
            "both_invalid",
            EOS_DEATHDTC="not a date",
            FU_FUPDEDAT="invalid date",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class LostToFollowupRow:
    SubjectId: str | None = None
    FU_FUPALDAT: str | None = None
    FU_FUPDEDAT: str | None = None
    FU_FUPSST: str | None = None
    FU_FUPSSTCD: str | None = None


@pytest.fixture
def lost_to_followup_fixture() -> pl.DataFrame:
    rows: List[LostToFollowupRow] = [
        LostToFollowupRow(
            "alive_valid",
            FU_FUPALDAT="1990-10-02",
            FU_FUPSST="Alive",
            FU_FUPSSTCD="1",
        ),
        LostToFollowupRow(
            "death_valid",
            FU_FUPDEDAT="1980-09-12",
            FU_FUPSST="Death",
            FU_FUPSSTCD="2",
        ),
        LostToFollowupRow(
            "ltfu_valid",
            FU_FUPALDAT="1900-01-01",
            FU_FUPSST="lost to follow up",
            FU_FUPSSTCD="3",
        ),
        LostToFollowupRow(
            "alive_lowercase_code_missing",
            FU_FUPALDAT="1999-09-09",
            FU_FUPSST="alive",
            FU_FUPSSTCD="",
        ),
        LostToFollowupRow(
            "invalid_dates",
            FU_FUPALDAT="not a date",
            FU_FUPDEDAT="invalid date",
            FU_FUPSST="",
            FU_FUPSSTCD="",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class EvaluabilityRow:
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
            TR_TRTNO="1",  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
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

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class TreatmentStopRow:
    SubjectId: str
    TR_TRCYNCD: str | None = None
    TR_TROSTPDT: str | None = None
    TR_TRC1_DT: str | None = None
    EOT_EOTDAT: str | None = None


@pytest.fixture
def treatment_stop_fixture() -> pl.DataFrame:
    rows: List[TreatmentStopRow] = [
        TreatmentStopRow("empty"),
        TreatmentStopRow(
            "missing_treatment_empty_str",
            TR_TROSTPDT="",
            TR_TRCYNCD="1",
        ),
        TreatmentStopRow(
            "missing_treatment_eot_empty_str",
            EOT_EOTDAT="",
            TR_TRCYNCD="1",
        ),
        TreatmentStopRow(
            "multirow",
            TR_TROSTPDT="1899-01-01",
            TR_TRCYNCD="1",
        ),
        TreatmentStopRow(
            "multirow",
            TR_TROSTPDT="1900-01-01",
            TR_TRCYNCD="1",
        ),
        TreatmentStopRow(
            "eot_precedence",
            EOT_EOTDAT="1900-01-02",
        ),
        TreatmentStopRow(
            "eot_precedence",
            TR_TROSTPDT="1900-01-01",
            TR_TRCYNCD="1",
        ),
        TreatmentStopRow(
            "invalid_row_doesnt_count",
            TR_TROSTPDT="1900-01-02",
        ),
        TreatmentStopRow(
            "invalid_row_doesnt_count",
            TR_TROSTPDT="1900-01-01",
            TR_TRCYNCD="1",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class LastTreatmentStartRow:
    SubjectId: str
    TR_TRC1_DT: str | None = None
    TR_TRCYNCD: str | None = None


@pytest.fixture
def last_treatment_start_fixture() -> pl.DataFrame:
    rows: List[LastTreatmentStartRow] = [
        LastTreatmentStartRow(
            "empty",
        ),
        LastTreatmentStartRow(
            "two_rows_both_valid",
            TR_TRC1_DT="1900-01-01",
            TR_TRCYNCD="1",
        ),
        LastTreatmentStartRow(
            "two_rows_both_valid",
            TR_TRC1_DT="1900-01-02",
            TR_TRCYNCD="1",
        ),
        LastTreatmentStartRow(
            "one_invalid",
            TR_TRC1_DT="1900-01-01",
            TR_TRCYNCD="1",
        ),
        LastTreatmentStartRow(
            "one_invalid",
            TR_TRC1_DT="1900-01-02",
            TR_TRCYNCD="0",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class TreatmentCycleRow:
    SubjectId: str
    TR_TRNAME: str | None = None
    TR_TRTNO: int | None = None
    TR_TRCNO1: int | None = None
    TR_TRC1_DT: str | None = None
    TR_TRO_STDT: str | None = None
    TR_TROSTPDT: str | None = None
    TR_TRDSDEL1: str | None = None
    TR_TRCYN: str | None = None
    TR_TRO_YNCD: int | None = None
    TR_TRIVU1: str | None = None
    TR_TRIVDS1: str | None = None
    TR_TRCYNCD: int | None = None
    TR_TRIVDELYN1: str | None = None
    TR_TRO_YN: str | None = None
    TR_TROREA: str | None = None
    TR_TROOTH: str | None = None
    TR_TRODSU: str | None = None
    TR_TRODSUOT: str | None = None
    TR_TRODSTOT: float | None = None
    TR_TROTAKE: str | None = None
    TR_TROTAKECD: int | None = None
    TR_TROTABNO: int | None = None
    TR_TROSPE: str | None = None


@pytest.fixture
def treatment_cycle_fixture() -> pl.DataFrame:
    rows: List[TreatmentCycleRow] = [
        TreatmentCycleRow(
            "iv_two_cycles",
            TR_TRNAME="IV Drug",
            TR_TRTNO="1",  # type: ignore
            TR_TRCNO1="1",  # type: ignore
            TR_TRC1_DT="1900-01-01",
            TR_TRIVDS1="100",
            TR_TRIVU1="mg",
            TR_TRIVDELYN1="Yes",
            TR_TRCYNCD="1",  # type: ignore
        ),
        TreatmentCycleRow(
            "iv_two_cycles",
            TR_TRNAME="IV Drug",
            TR_TRTNO=1,
            TR_TRCNO1=2,
            TR_TRC1_DT="1900-01-10",
            TR_TRIVDS1="100",
            TR_TRIVU1="mg",
            TR_TRIVDELYN1="No",
            TR_TRCYNCD=1,
        ),
        TreatmentCycleRow(
            "oral_single",
            TR_TRNAME="Oral Drug",
            TR_TRTNO=1,
            TR_TRCNO1=1,
            TR_TRC1_DT="1900-01-01",
            TR_TROSTPDT="1900-01-20",
            TR_TRO_YNCD=1,
            TR_TROTAKECD=0,
            TR_TRODSTOT=200.0,
            TR_TRODSU="mg",
            TR_TROTABNO=3,
            TR_TROSPE="nausea",
            TR_TRCYNCD=1,
        ),
        TreatmentCycleRow(
            "both_modalities",
            TR_TRNAME="IV Part",
            TR_TRTNO=1,
            TR_TRCNO1=1,
            TR_TRC1_DT="1900-01-01",
            TR_TRIVDS1="50",
            TR_TRIVU1="mg",
            TR_TRIVDELYN1="Yes",
            TR_TRCYNCD=1,
        ),
        TreatmentCycleRow(
            "both_modalities",
            TR_TRNAME="Oral Part",
            TR_TRTNO=2,
            TR_TRCNO1=1,
            TR_TRC1_DT="1900-03-01",
            TR_TROSTPDT="1900-03-30",
            TR_TRO_YNCD=0,
            TR_TROTAKECD=1,
            TR_TRODSTOT=100,
            TR_TRODSU="mg",
            TR_TRCYNCD=1,
        ),
        TreatmentCycleRow(
            "drop_no_name",
            TR_TRNAME=None,
            TR_TRTNO=1,
            TR_TRCNO1=1,
            TR_TRC1_DT="1900-01-01",
            TR_TRCYNCD=1,
        ),
        TreatmentCycleRow(
            "both_in_row",
            TR_TRNAME="Mixed Row",
            TR_TRTNO=1,
            TR_TRCNO1=1,
            TR_TRC1_DT="1900-01-01",
            TR_TRO_STDT="1900-01-03",
            TR_TROSTPDT="1900-01-10",
            TR_TRIVDS1="10",
            TR_TRIVU1="mg",
            TR_TRCYNCD=1,
            TR_TRIVDELYN1="Yes",
            TR_TRO_YNCD=1,
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class ConcomitantMedicationRow:
    SubjectId: str
    CM_CMTRT: str | None = None
    CM_CMMHYNCD: int | None = None
    CM_CMAEYN: str | None = None
    CM_CMONGOCD: int | None = None
    CM_CMSTDAT: str | None = None
    CM_CMENDAT: str | None = None
    CM_CMSPID: int | None = None


@pytest.fixture
def concomitant_medication_fixture() -> pl.DataFrame:
    rows: List[ConcomitantMedicationRow] = [
        ConcomitantMedicationRow("drop_null_name", CM_CMTRT=None),
        ConcomitantMedicationRow(
            "name_is_na",
            CM_CMTRT="Na",
            CM_CMMHYNCD="0",  # type: ignore
            CM_CMAEYN="No",
            CM_CMONGOCD="0",  # type: ignore
            CM_CMSTDAT="not a date",
            CM_CMENDAT=None,
            CM_CMSPID=10,
        ),
        ConcomitantMedicationRow(
            "all_fields",
            CM_CMTRT="  Paracetamol  ",
            CM_CMMHYNCD="1",  # type: ignore
            CM_CMAEYN="Yes",
            CM_CMONGOCD="1",  # type: ignore
            CM_CMSTDAT="1900-01-01",
            CM_CMENDAT="1900-01-10",
            CM_CMSPID="2",  # type: ignore
        ),
        ConcomitantMedicationRow(
            "ordering",
            CM_CMTRT="Drug A",
            CM_CMMHYNCD=1,
            CM_CMAEYN="No",
            CM_CMONGOCD=1,
            CM_CMSTDAT="1900-02-01",
            CM_CMSPID=1,
        ),
        ConcomitantMedicationRow(
            "ordering",
            CM_CMTRT="Drug A",
            CM_CMMHYNCD=1,
            CM_CMAEYN="No",
            CM_CMONGOCD=1,
            CM_CMSTDAT="1900-01-01",
            CM_CMSPID=1,
        ),
        ConcomitantMedicationRow(
            "ordering",
            CM_CMTRT="Drug B",
            CM_CMMHYNCD=0,
            CM_CMAEYN="Yes",
            CM_CMONGOCD=0,
            CM_CMSTDAT="1899-12-12",
            CM_CMSPID=2,
        ),
        ConcomitantMedicationRow(
            "ongoing_none",
            CM_CMTRT="Drug C",
            CM_CMMHYNCD=None,
            CM_CMAEYN="",
            CM_CMONGOCD=None,
            CM_CMSTDAT="",
            CM_CMENDAT="",
            CM_CMSPID=3,
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class AdverseEventsFlagRow:
    SubjectId: str
    AE_AECTCAET: str | None = None
    AE_AESTDAT: str | None = None
    AE_AETOXGRECD: str | None = None


@pytest.fixture
def adverse_events_flag_fixture() -> pl.DataFrame:
    rows: List[AdverseEventsFlagRow] = [
        AdverseEventsFlagRow("none_all_empty"),
        AdverseEventsFlagRow(
            "none_whitespace_only",
            AE_AECTCAET="   ",
            AE_AESTDAT="  ",
            AE_AETOXGRECD="",
        ),
        AdverseEventsFlagRow(
            "text_only",
            AE_AECTCAET="headache",
        ),
        AdverseEventsFlagRow(
            "date_only",
            AE_AESTDAT="1900-01-01",
        ),
        AdverseEventsFlagRow(
            "grade_only",
            AE_AETOXGRECD="3",
        ),
        AdverseEventsFlagRow(
            "mixed",
            AE_AECTCAET=" pain ",
        ),
        AdverseEventsFlagRow("multirow_any_true"),
        AdverseEventsFlagRow(
            "multirow_any_true",
            AE_AETOXGRECD="2",
        ),
        AdverseEventsFlagRow("multirow_all_false"),
        AdverseEventsFlagRow("multirow_all_false"),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class AdverseEventRow:
    SubjectId: str
    AE_AECTCAET: str | None = None
    AE_AETOXGRECD: str | None = None
    AE_AEOUT: str | None = None
    AE_AESTDAT: str | None = None
    AE_AEENDAT: str | None = None
    AE_SAESTDAT: str | None = None
    AE_AEREL1: str | None = None
    AE_AEREL1CD: int | None = None
    AE_AETRT1: str | None = None
    AE_AEREL2: str | None = None
    AE_AEREL2CD: int | None = None
    AE_AETRT2: str | None = None
    AE_AESERCD: int | None = None
    AE_SAEEXP1CD: int | None = None
    AE_SAEEXP2CD: int | None = None
    FU_FUPDEDAT: str | None = None
    TR_TRNAME: str | None = None
    TR_TRTNO: int | None = None


@pytest.fixture
def adverse_events_fixture() -> pl.DataFrame:
    rows: List[AdverseEventRow] = [
        AdverseEventRow(
            "drop_null_term",
            AE_AECTCAET=None,
        ),
        AdverseEventRow(
            "simple",
            AE_AECTCAET="Headache",
            AE_AETOXGRECD="2",
            AE_AEOUT="Recovered",
            AE_AESTDAT="1900-01-01",
            AE_AEENDAT="1900-01-03",
            AE_AESERCD="0",  # type: ignore
            AE_SAEEXP1CD="1",  # type: ignore
            AE_SAEEXP2CD="2",  # type: ignore
            AE_AEREL1CD="4",  # type: ignore
            AE_AEREL2CD="3",  # type: ignore
            AE_AETRT1="Drug A",
            AE_AETRT2="Drug B",
            TR_TRNAME="Regimen X",
            TR_TRTNO="1",  # type: ignore
        ),
        AdverseEventRow(
            "serious_fill_end_from_death",
            AE_AECTCAET="Sepsis",
            AE_AETOXGRECD="5",
            AE_AEOUT="Fatal",
            AE_AESTDAT="1900-01-10",
            AE_SAESTDAT="1900-01-12",
            AE_AESERCD=1,
            AE_SAEEXP1CD=2,
            AE_SAEEXP2CD=1,
            AE_AEREL1CD=1,
            FU_FUPDEDAT="1900-02-01",
            TR_TRNAME="Regimen Y",
            TR_TRTNO=2,
        ),
        AdverseEventRow(
            "multi",
            AE_AECTCAET="Nausea",
            AE_AETOXGRECD="1",
            AE_AESTDAT="1900-03-01",
            AE_AEENDAT="1900-03-02",
            AE_AESERCD=0,
            AE_AEREL1CD=2,
            AE_AEREL2CD=4,
        ),
        AdverseEventRow(
            "multi",
            AE_AECTCAET="Vomiting",
            AE_AETOXGRECD="2",
            AE_AESTDAT="1900-03-05",
            AE_AESERCD=0,
            AE_SAEEXP1CD=2,
            AE_SAEEXP2CD=1,
            AE_AEREL2CD=1,
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class TumorAssessmentRow:
    SubjectId: str
    RA_RAASSESS1: str | None = None
    RA_RAASSESS2: str | None = None
    RA_RABASECH: str | None = None
    RNRSP_TERNCFB: str | None = None
    RA_RARECCH: str | None = None
    RNRSP_TERNCFN: str | None = None
    RA_RANLBASECD: int | None = None
    RNRSP_RNRSPNLCD: int | None = None
    RA_EventDate: str | None = None
    RNRSP_EventDate: str | None = None
    RA_RATIMRES: str | None = None
    RNRSP_RNRSPCL: str | None = None
    RA_RAiMOD: str | None = None
    RA_RAPROGDT: str | None = None
    RA_RAiUNPDT: str | None = None
    RA_EventId: str | None = None
    RNRSP_EventId: str | None = None


@pytest.fixture
def tumor_assessments_fixture() -> pl.DataFrame:
    rows: List[TumorAssessmentRow] = [
        TumorAssessmentRow("no_signal"),
        TumorAssessmentRow(
            "recist_full",
            RA_RAASSESS1="RECIST",
            RA_RABASECH="25",
            RA_RARECCH="0",
            RA_RANLBASECD="1",  # type: ignore
            RA_EventDate="1900-01-10",
            RA_RATIMRES="  PR  ",
            RA_RAPROGDT="1900-02-01",
            RA_EventId="V01",
        ),
        TumorAssessmentRow(
            "irecist_full",
            RA_RAASSESS2="iRECIST",
            RA_RAiMOD="iCR",
            RA_RAiUNPDT="1900-02-10",
            RA_EventDate="1900-01-15",
            RA_RABASECH="0",
            RA_EventId="V02",
        ),
        TumorAssessmentRow(
            "rano_full",
            RNRSP_TERNCFB="-30",
            RNRSP_TERNCFN="-10",
            RNRSP_RNRSPNLCD=0,
            RNRSP_RNRSPCL="RANO-PR",
            RNRSP_EventDate="1900-01-20",
            RNRSP_EventId="V03",
        ),
        TumorAssessmentRow(
            "collision_irecist_wins",
            RA_RAASSESS1="RECIST",
            RA_RAASSESS2="iRECIST",
            RA_EventDate="1900-03-01",
            RA_EventId="V04",
        ),
        TumorAssessmentRow(
            "recist_bad_date",
            RA_RAASSESS1="RECIST",
            RA_RATIMRES="SD",
            RA_EventDate="not a date",
            RA_EventId="V06",
        ),
        TumorAssessmentRow(
            "event_from_rnrsp",
            RNRSP_TERNCFB="10",
            RNRSP_RNRSPNLCD=1,
            RNRSP_EventDate="1900-04-01",
            RNRSP_EventId="V05",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class BestOverallResponseRow:
    SubjectId: str
    RA_RATIMRES: str | None = None
    RA_RATIMRESCD: int | None = None
    RA_RAiMOD: str | None = None
    RA_RAiMODCD: int | None = None
    RA_EventDate: str | None = None
    RNRSP_RNRSPCL: str | None = None
    RNRSP_RNRSPCLCD: int | None = None
    RNRSP_EventDate: str | None = None


@pytest.fixture
def best_overall_response_fixture() -> pl.DataFrame:
    rows: List[BestOverallResponseRow] = [
        BestOverallResponseRow(
            "recist_only",
            RA_RATIMRES=" PR ",
            RA_RATIMRESCD=20,
            RA_EventDate="1900-01-10",
        ),
        BestOverallResponseRow(
            "irecist_only",
            RA_RAiMOD="CR",
            RA_RAiMODCD=5,
            RA_EventDate="1900-01-05",
        ),
        BestOverallResponseRow(
            "both_pick_irecist",
            RA_RATIMRES="SD",
            RA_RATIMRESCD="30",  # type: ignore
            RA_RAiMOD="iCR",
            RA_RAiMODCD="5",  # type: ignore
            RA_EventDate="1900-02-01",
        ),
        BestOverallResponseRow(
            "irecist_unconfirmed_drop",
            RA_RATIMRES="PD",
            RA_RATIMRESCD=40,
            RA_RAiMOD="iUPD?",
            RA_RAiMODCD=4,
            RA_EventDate="1900-03-01",
        ),
        BestOverallResponseRow(
            "rano_only",
            RNRSP_RNRSPCL="RANO-PR",
            RNRSP_RNRSPCLCD=15,
            RNRSP_EventDate="1900-01-20",
        ),
        BestOverallResponseRow(
            "multi_best",
            RA_RATIMRES="SD",
            RA_RATIMRESCD=30,
            RA_EventDate="1900-01-01",
        ),
        BestOverallResponseRow(
            "multi_best",
            RA_RATIMRES="PR",
            RA_RATIMRESCD=20,
            RA_EventDate="1900-02-01",
        ),
        BestOverallResponseRow(
            "irecist_ne_maps_96",
            RA_RATIMRES="SD",
            RA_RATIMRESCD=30,
            RA_RAiMOD="iNE",
            RA_RAiMODCD=6,
            RA_EventDate="1900-04-01",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class ClinicalBenefitRow:
    SubjectId: str
    RA_RATIMRESCD: int | None = None
    RA_RAiMODCD: int | None = None
    RNRSP_RNRSPCLCD: int | None = None
    RNRSP_EventId: str | None = None
    RA_EventId: str | None = None


@pytest.fixture
def clinical_benefit_fixture() -> pl.DataFrame:
    rows: List[ClinicalBenefitRow] = [
        ClinicalBenefitRow(
            "recist_le3",
            RA_RATIMRESCD=3,
            RA_EventId="V03",
        ),
        ClinicalBenefitRow(
            "recist_gt3",
            RA_RATIMRESCD=4,
            RA_EventId="V03",
        ),
        ClinicalBenefitRow(
            "irecist_le3",
            RA_RAiMODCD=2,
            RA_EventId="V03",
        ),
        ClinicalBenefitRow(
            "rano_le3",
            RNRSP_RNRSPCLCD=3,
            RNRSP_EventId="V03",
        ),
        ClinicalBenefitRow(
            "both_present",
            RA_RATIMRESCD=4,
            RA_RAiMODCD=3,
            RA_EventId="V03",
        ),
        ClinicalBenefitRow("v03_no_codes", RA_EventId="V03"),
        ClinicalBenefitRow(
            "not_v03",
            RA_RATIMRESCD=2,
            RA_EventId="V02",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)


@dataclass(frozen=True, slots=True)
class EOTRow:
    SubjectId: str
    EOT_EOTREOT: str | None = None
    EOT_EOTDAT: str | None = None


@pytest.fixture
def eot_fixture() -> pl.DataFrame:
    rows: List[EOTRow] = [
        EOTRow(
            "reason_trim",
            EOT_EOTREOT="  Progression  ",
        ),
        EOTRow(
            "reason_empty_string",
            EOT_EOTREOT="",
        ),
        EOTRow(
            "reason_whitespace_only",
            EOT_EOTREOT="   ",
        ),
        EOTRow(
            "reason_none",
            EOT_EOTREOT=None,
        ),
        EOTRow(
            "reason_multi_overwrite",
            EOT_EOTREOT="Toxicity",
        ),
        EOTRow(
            "reason_multi_overwrite",
            EOT_EOTREOT="Patient decision",
        ),
        EOTRow(
            "date_valid",
            EOT_EOTDAT="1900-01-01",
        ),
        EOTRow(
            "date_empty_string",
            EOT_EOTDAT="",
        ),
        EOTRow(
            "date_invalid",
            EOT_EOTDAT="not a date",
        ),
        EOTRow("date_none"),
        EOTRow(
            "date_multi_overwrite",
            EOT_EOTDAT="1900-01-01",
        ),
        EOTRow(
            "date_multi_overwrite",
            EOT_EOTDAT="1901-01-01",
        ),
    ]

    records = [asdict(r) for r in rows]  # type: ignore
    return pl.from_dicts(records)
