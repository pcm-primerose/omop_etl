from typing import Optional, Any
import datetime as dt

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
        }
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
        }
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
        }
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
        }
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
        }
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
        }
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
        }
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
        }
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
        }
    )


@pytest.fixture
def evaluability_fixture() -> pl.DataFrame:
    """
    Multi-row fixture covering IV/oral sufficiency, invalid rows, and parsing edge-cases.
    """
    rows = []

    def _mk_df(_rows: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(_rows)

    def add_row(
        pid: str,
        *,
        trtno: int | None = None,
        trc1_dt: str = "",
        tro_stdt: str = "",
        tro_stpdt: str = "",
        trcyncd: int = 1,
    ):
        rows.append(
            {
                "SubjectId": pid,
                "TR_TRTNO": trtno,
                "TR_TRC1_DT": trc1_dt,
                "TR_TRO_STDT": tro_stdt,
                "TR_TROSTPDT": tro_stpdt,
                "TR_TRCYNCD": trcyncd,
            }
        )

    add_row("IMPRESS-X_0001_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0002_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0002_1", trtno=1, trc1_dt="2001-01-15")
    add_row("IMPRESS-X_0003_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0003_1", trtno=1, trc1_dt="2001-01-22")
    add_row("IMPRESS-X_0004_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0004_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-31")
    add_row("IMPRESS-X_0005_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0005_1", trtno=1, trc1_dt="2001-02-05")
    add_row("IMPRESS-X_0005_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-10")
    add_row("IMPRESS-X_0006_1", tro_stdt="2001-01-01", tro_stpdt="")
    add_row("IMPRESS-X_0007_1", tro_stdt="2001-01-01", tro_stpdt="")
    add_row("IMPRESS-X_0008_1", tro_stdt="", tro_stpdt="2001-02-05")
    add_row("IMPRESS-X_0009_1", tro_stdt="", tro_stpdt="2001-02-05")
    add_row("IMPRESS-X_0010_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0010_1", trtno=1, trc1_dt="")
    add_row("IMPRESS-X_0011_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0011_1", trtno=1, trc1_dt="2001-01-21")
    add_row("IMPRESS-X_0012_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-20")
    add_row("IMPRESS-X_0013_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0013_1", trtno=2, trc1_dt="2001-02-05")
    add_row("IMPRESS-X_0014_1", trtno=1, trc1_dt="2001-01-01", trcyncd=1)
    add_row("IMPRESS-X_0014_1", trtno=1, trc1_dt="2001-02-05", trcyncd=0)
    add_row(
        "IMPRESS-X_0015_1", tro_stdt="2001-01-01", tro_stpdt="2001-02-10", trcyncd=0
    )

    return _mk_df(rows)


@pytest.fixture
def ecog_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",  # all data
                "IMPRESS-X_0002_1",  # EventId no code
                "IMPRESS-X_0003_1",  # EventId no description
                "IMPRESS-X_0004_1",  # wrong event ID with data: None
                "IMPRESS-X_0005_1",  # no event ID with data: None
                "IMPRESS-X_0006_1",  # partial data
                "IMPRESS-X_0007_1",  # wrong date
            ],
            "ECOG_EventId": ["V00", "V00", "V00", "V02", "", "V00", "V00"],
            "ECOG_ECOGS": ["all", "no code", "", "wrong ID", "", "", "code"],
            "ECOG_ECOGSCD": ["1", "", "2", "3", "", "1", "4"],
            "ECOG_ECOGDAT": [
                "1900-01-01",
                "1900-nk-01",
                "1900-01-nk",
                "",
                "",
                "1900-nk-nk",
                "not a date",
            ],
        }
    )


@pytest.fixture
def medical_history_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
                "IMPRESS-X_0006_1",  # None
            ],
            "MH_MHTERM": [
                "pain",
                "something",
                "hypertension",
                "dizziness",
                "pain",
                "rigor mortis",
                "",
            ],
            "MH_MHSPID": ["01", "05", "02", "03", "01", "01", ""],
            "MH_MHSTDAT": [
                "1900-09-NK",
                "1900-nk-02",
                "1901-10-02",
                "1902-nk-nk",
                "1840-02-02",
                "1740-02-02",
                "",
            ],
            "MH_MHENDAT": [
                "",  # ongoing
                "1990-01-01",
                "1901-11-02",  # ended
                "1903-nk-nk",  # ongoing
                "",  # ongoing
                "1940-02-02",  # ended
                "",
            ],
            "MH_MHONGO": [
                "Current/active",
                "Past",
                "Past",  # ended
                "Present/dormant",  # ongoing
                "Past",  # conflict w. ongoing
                "Past",  # ended
                "",
            ],
            "MH_MHONGOCD": [
                "1",
                "3",
                "3",  # ended
                "2",  # ongoing
                "3",  # conflict w. ongoing
                "1",  # wrong code
                "",
            ],
        }
    )


@pytest.fixture
def adverse_event_number_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "AE_AETOXGRECD": [
                "3",
                "2",
                "1",
                "",
                "",
                "4",
                "5",
                "",
            ],
            "AE_AECTCAET": [
                "ouch",
                "owe",
                "",
                "something",
                "else",
                "",
                "rash",
                "",
            ],
            "AE_AESTDAT": [
                "1900-01-01",
                "",
                "",
                "1900-01-01",
                "1889-02-23",
                "",
                "1900-01-01",
                "",
            ],
        }
    )


@pytest.fixture
def serious_adverse_event_number_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",  # 1
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",  # 2
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",  # 1
                "IMPRESS-X_0004_1",  # 0
                "IMPRESS-X_0005_1",  # 0
            ],
            "AE_AESERCD": [
                "1",
                "0",
                "1",
                "1",
                "",
                "1",
                "0",
                "",
            ],
            "AE_SAESTDAT": [
                "1900-01-01",
                "1900-01-01",
                "",
                "1900-02-02",
                "1900-03-03",
                "",
                "1900-01-01",
                "",
            ],
        }
    )


@pytest.fixture
def baseline_tumor_assessment_fixture():
    """
    One big frame with many per-subject scenarios.
    Only columns consumed by _process_baseline_tumor_assessment are provided.
    """
    rows = []

    def base_row(pid):
        return {
            "SubjectId": pid,
            # VI
            "VI_VITUMA": None,
            "VI_VITUMA__2": None,
            "VI_EventDate": None,
            "VI_EventId": None,
            # RCNT / RNTMNT
            "RCNT_RCNTNOB": None,
            "RCNT_EventDate": None,
            "RCNT_EventId": None,
            "RNTMNT_RNTMNTNOB": None,
            "RNTMNT_RNTMNTNO": None,
            "RNTMNT_EventId": None,
            "RNTMNT_EventDate": None,
            # RNRSP / RA
            "RNRSP_TERNTBAS": None,
            "RNRSP_TERNAD": None,
            "RNRSP_EventDate": None,
            "RNRSP_EventId": None,
            "RA_RARECBAS": None,
            "RA_RARECNAD": None,
            "RA_EventDate": None,
            "RA_EventId": None,
        }

    # No data anywhere -> subject should not get a baseline instance
    rows.append(base_row("S00_NONE"))

    # VI cases
    # VI_VITUMA only
    r = base_row("S01_VI_VITUMA_ONLY")
    r["VI_VITUMA"] = "PD"
    r["VI_EventDate"] = "2020-01-02"
    r["VI_EventId"] = "V00"
    rows.append(r)

    # VI_VITUMA__2 only
    r = base_row("S02_VI_VITUMA2_ONLY")
    r["VI_VITUMA__2"] = "CR"
    r["VI_EventDate"] = "2020-01-03"
    r["VI_EventId"] = "V00"
    rows.append(r)

    # Neither VI value -> None
    rows.append(base_row("S03_VI_NONE"))

    # VI value but no date -> should be filtered out (None)
    r = base_row("S04_VI_VALUE_NO_DATE")
    r["VI_VITUMA"] = "SD"
    r["VI_EventId"] = "V00"
    rows.append(r)

    # Off-target lesions (RCNT/RNTMNT) — EventId must be V00
    # No off-target data
    rows.append(base_row("S05_OFF_NONE"))

    # RNTMNT both cols present -> pick RNTMNT_RNTMNTNOB
    r = base_row("S06_RNT_BOTH")
    r["RNTMNT_RNTMNTNOB"] = "5"
    r["RNTMNT_RNTMNTNO"] = "7"
    r["RNTMNT_EventId"] = "V00"
    r["RNTMNT_EventDate"] = "2020-02-01"
    rows.append(r)

    # RNTMNT only second col
    r = base_row("S07_RNT_ONE")
    r["RNTMNT_RNTMNTNO"] = "4"
    r["RNTMNT_EventId"] = "V00"
    r["RNTMNT_EventDate"] = "2020-02-02"
    rows.append(r)

    # Off-target present but wrong EventId -> None
    r = base_row("S08_RNT_WRONG_EVENT")
    r["RNTMNT_RNTMNTNOB"] = "3"
    r["RNTMNT_EventId"] = "V01"
    r["RNTMNT_EventDate"] = "2020-02-03"
    rows.append(r)

    # RCNT only
    r = base_row("S09_RCNT_ONLY")
    r["RCNT_RCNTNOB"] = "3"
    r["RCNT_EventId"] = "V00"
    r["RCNT_EventDate"] = "2020-02-04"
    rows.append(r)

    # RCNT invalid integer -> None
    r = base_row("S10_RCNT_INVALID")
    r["RCNT_RCNTNOB"] = "abc"
    r["RCNT_EventId"] = "V00"
    r["RCNT_EventDate"] = "2020-02-05"
    rows.append(r)

    # Valid off-target size but no date -> keep size, date None
    r = base_row("S11_OFF_SIZE_NO_DATE")
    r["RNTMNT_RNTMNTNOB"] = "6"
    r["RNTMNT_EventId"] = "V00"
    # date missing
    rows.append(r)

    # Target lesions (RA/RNRSP)
    # RA valid
    r = base_row("S12_RA_VALID")
    r["RA_RARECBAS"] = "12"
    r["RA_RARECNAD"] = "12"
    r["RA_EventDate"] = "2018-07-27"
    r["RA_EventId"] = "V00"
    rows.append(r)

    # RNRSP valid
    r = base_row("S13_RNRSP_VALID")
    r["RNRSP_TERNTBAS"] = "20"
    r["RNRSP_TERNAD"] = "18"
    r["RNRSP_EventDate"] = "2019-01-01"
    r["RNRSP_EventId"] = "V00"
    rows.append(r)

    # RA missing date but value present -> expect parsed size, date None (intended)
    r = base_row("S14_RA_NO_DATE")
    r["RA_RARECBAS"] = "8"
    r["RA_RARECNAD"] = "7"
    r["RA_EventId"] = "V00"
    rows.append(r)

    # RNRSP missing date but value present -> expect parsed size, date None (intended)
    r = base_row("S15_RNRSP_NO_DATE")
    r["RNRSP_TERNTBAS"] = "9"
    r["RNRSP_TERNAD"] = "8"
    r["RNRSP_EventId"] = "V00"
    rows.append(r)

    # Missing baseline, have nadir + date -> should NOT parse (size None)
    r = base_row("S16_NO_BASELINE_HAS_NADIR")
    r["RA_RARECNAD"] = "11"
    r["RA_EventDate"] = "2020-03-01"
    r["RA_EventId"] = "V00"
    rows.append(r)

    # Multiple valid rows -> take earliest by date
    r1 = base_row("S17_MULTIPLE")
    r1["RA_RARECBAS"] = "10"
    r1["RA_RARECNAD"] = "10"
    r1["RA_EventDate"] = "2020-01-03"
    r1["RA_EventId"] = "V00"
    rows.append(r1)
    r2 = base_row("S17_MULTIPLE")
    r2["RA_RARECBAS"] = "9"
    r2["RA_RARECNAD"] = "9"
    r2["RA_EventDate"] = "2020-01-01"
    r2["RA_EventId"] = "V00"
    rows.append(r2)

    return pl.from_dicts(rows)


@pytest.fixture
def previous_treatment_fixture():
    rows = []

    def base_row(pid):
        return {
            "SubjectId": pid,
            "CT_CTTYPE": None,
            "CT_CTTYPECD": None,
            "CT_CTSPID": None,
            "CT_CTSTDAT": None,
            "CT_CTENDAT": None,
            "CT_CTTYPESP": None,
        }

    rows.append(base_row("empty"))

    # base case
    r = base_row("has_treatment")
    r["CT_CTTYPE"] = "abc"
    r["CT_CTTYPECD"] = "2"
    r["CT_CTSPID"] = "1"
    r["CT_CTSTDAT"] = "1900-01-01"
    r["CT_CTENDAT"] = "1900-01-02"
    r["CT_CTTYPESP"] = "def"
    rows.append(r)

    # missing treatment (not initialized)
    r = base_row("missing_treatment")
    r["CT_CTTYPECD"] = "2"
    r["CT_CTSPID"] = "1"
    r["CT_CTSTDAT"] = "1900-01-01"
    r["CT_CTENDAT"] = "1900-01-02"
    r["CT_CTTYPESP"] = "def"
    rows.append(r)

    # missing partial
    r = base_row("missing_partial")
    r["CT_CTTYPE"] = "abc"
    r["CT_CTSPID"] = "1"
    r["CT_CTSTDAT"] = "1900-01-01"
    rows.append(r)

    r = base_row("missing_partial")
    r["CT_CTTYPE"] = "def"
    r["CT_CTSPID"] = "2"
    r["CT_CTSTDAT"] = "1900-01-03"
    rows.append(r)

    return pl.from_dicts(rows)


@pytest.fixture
def treatment_start_fixture():
    rows = []

    def base_row(pid):
        return {"SubjectId": pid, "TR_TRNAME": None, "TR_TRC1_DT": None}

    # all empty
    rows.append(base_row("empty"))

    # one entry
    r = base_row("single_row")
    r["TR_TRNAME"] = "b"
    r["TR_TRC1_DT"] = "1900-01-02"
    rows.append(r)

    # multiple rows
    r = base_row("multirow")
    r["TR_TRNAME"] = "a"
    r["TR_TRC1_DT"] = "1900-01-03"
    rows.append(r)

    r = base_row("multirow")
    r["TR_TRNAME"] = "a"
    r["TR_TRC1_DT"] = "2001-01-01"
    rows.append(r)

    r = base_row("multirow")
    r["TR_TRNAME"] = "a"
    r["TR_TRC1_DT"] = "1900-01-01"
    rows.append(r)

    # missing treatment name
    r = base_row("missing_treatment_none")
    r["TR_TRNAME"] = None
    r["TR_TRC1_DT"] = "1900-01-03"
    rows.append(r)

    r = base_row("missing_treatment_empty_str")
    r["TR_TRNAME"] = ""
    r["TR_TRC1_DT"] = "1900-01-03"
    rows.append(r)

    return pl.from_dicts(rows)


@pytest.fixture
def treatment_end_fixture():
    rows = []

    def base_row(pid):
        return {"SubjectId": pid, "TR_TRNAME": None, "TR_TRC1_DT": None}

    # all empty
    rows.append(base_row("empty"))
