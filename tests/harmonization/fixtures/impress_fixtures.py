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
                "NA",
                "NA",
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
            ],
            "DM_SEX": ["Female", "Male", "f", "m", "error"],
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
                "NA",
                "tumor2_subtype1",
                "tumor3_subtype1",
                "NA",
            ],
            "COH_COHTTYPECD": ["50", "NA", "70", "10", "NA"],
            "COH_COHTTYPE__2": ["NA", "CRC_subtype", "NA", "NA", "tumor4_subtype1"],
            "COH_COHTTYPE__2CD": ["NA", "40", "NA", "NA", "30"],
            "COH_COHTT": ["tumor1_subtype2", "NA", "tumor2_subtype2", "NA", "NA"],
            "COH_COHTTOSP": [
                "tumor1_subtype3",
                "NA",
                "NA",
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
            # note: collision in mutually exclusive fields are logged and returned as None (source data is wrong)
            "COH_COHALLO1": ["NA", "some drug", "mismatch_1", "NA", "collision"],
            "COH_COHALLO1CD": ["NA", "99", "10", "NA", "NA"],
            "COH_COHALLO1__2": ["Traztuzumab", "NA", "NA", "mismatch_2", "NA"],
            "COH_COHALLO1__2CD": ["31", "NA", "NA", "50", "NA"],
            "COH_COHALLO1__3": ["NA", "NA", "NA", "NA", "some_drug_3"],
            "COH_COHALLO1__3CD": ["NA", "NA", "NA", "NA", "99"],
            # sd2
            "COH_COHALLO2": ["NA", "some drug 2", "NA", "mismatch_2_1", "NA"],
            "COH_COHALLO2CD": ["NA", "1", "NA", "60", "NA"],
            "COH_COHALLO2__2": ["Tafinlar", "NA", "mismatch_1_2", "NA", "NA"],
            "COH_COHALLO2__2CD": ["10", "NA", "12", "NA", "5"],
            "COH_COHALLO2__3": ["NA", "NA", "NA", "NA", "some_drug_3_2"],
            "COH_COHALLO2__3CD": ["NA", "NA", "NA", "NA", "999"],
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
                "NA",
                "BRCA1 inactivating mutation",
                "SDHAF2 mutation",
                "NA",
            ],
            "COH_GENMUT1CD": ["21", "NA", "2", "-1", "10"],
            "COH_COHCTN": [
                "BRAF Non-V600 activating mutations",
                "some info",
                "BRCA1 stop-gain del exon 11",
                "more info",
                "NA",
            ],
            "COH_COHTMN": [
                "BRAF Non-V600 activating mutations",
                "NA",
                "BRCA1 stop-gain deletion",
                "NA",
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
                "1990-10-02",
                "1961-09-12",
                "NA",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": [
                "1990-10-02",
                "2016-09-12",
                "1900-01-01",
                "NA",
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
                "NA",
                "1900-01-01",
                "1999-09-09",
                "not a date",
            ],
            "FU_FUPDEDAT": ["NA", "1980-09-12", "NA", "NA", "invalid date"],
            "FU_FUPSST": ["Alive", "Death", "lost to follow up", "alive", "NA"],
            "FU_FUPSSTCD": ["1", "2", "3", "NA", "NA"],
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

    # case 1: one IV row -> not evaluable
    add_row("IMPRESS-X_0001_1", trtno=1, trc1_dt="2001-01-01")

    # case 2: two IV rows, insufficient gap (<21) -> not evaluable
    add_row("IMPRESS-X_0002_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0002_1", trtno=1, trc1_dt="2001-01-15")  # 14d

    # case 3: two IV rows, sufficient gap (>=21) -> evaluable
    add_row("IMPRESS-X_0003_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0003_1", trtno=1, trc1_dt="2001-01-22")  # 21d

    # case 4: one IV row (no gap) + one oral row sufficient -> evaluable
    add_row("IMPRESS-X_0004_1", trtno=1, trc1_dt="2001-01-01")
    add_row(
        "IMPRESS-X_0004_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-31"
    )  # 30d oral

    # case 5: two IV rows sufficient + oral insufficient -> evaluable
    add_row("IMPRESS-X_0005_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0005_1", trtno=1, trc1_dt="2001-02-05")  # 35d
    add_row(
        "IMPRESS-X_0005_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-10"
    )  # 9d oral

    # case 6: one oral row, missing end date -> not evaluable
    add_row("IMPRESS-X_0006_1", tro_stdt="2001-01-01", tro_stpdt="")

    # case 7: one oral row, end-date not a date -> not evaluable
    add_row("IMPRESS-X_0007_1", tro_stdt="2001-01-01", tro_stpdt="NA")

    # case 8: one oral row, start not a date -> not evaluable
    add_row("IMPRESS-X_0008_1", tro_stdt="NA", tro_stpdt="2001-02-05")

    # case 9: one oral row, missing start -> not evaluable
    add_row("IMPRESS-X_0009_1", tro_stdt="", tro_stpdt="2001-02-05")

    # case 10: two IV rows, missing start in one -> not evaluable
    add_row("IMPRESS-X_0010_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0010_1", trtno=1, trc1_dt="")  # null after parse

    # case 11: two IV rows, insufficient length (≤21) -> not evaluable
    add_row("IMPRESS-X_0011_1", trtno=1, trc1_dt="2001-01-01")
    add_row("IMPRESS-X_0011_1", trtno=1, trc1_dt="2001-01-21")  # 20d diff

    # case 12: one oral row, insufficient length (≤28) -> not evaluable
    add_row("IMPRESS-X_0012_1", tro_stdt="2001-01-01", tro_stpdt="2001-01-20")  # 19d

    # case 13: two IV rows, sufficient gap but different TR_TRTNO -> not evaluable
    add_row("IMPRESS-X_0013_1", trtno=1, trc1_dt="2001-01-01")
    add_row(
        "IMPRESS-X_0013_1", trtno=2, trc1_dt="2001-02-05"
    )  # cross-drug; no within-drug gap

    # case 14: two IV rows, sufficient gap, but one cycle invalid -> not evaluable
    add_row("IMPRESS-X_0014_1", trtno=1, trc1_dt="2001-01-01", trcyncd=1)
    add_row(
        "IMPRESS-X_0014_1", trtno=1, trc1_dt="2001-02-05", trcyncd=0
    )  # filtered out

    # case 15: one oral row, sufficient length, but invalid -> not evaluable
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
                "IMPRESS-X_0004_1",  # wrong event ID with data
                "IMPRESS-X_0005_1",  # no event ID with data
            ],
            "ECOG_EventId": ["V00", "V00", "V00", "V02", "NA"],
            "ECOG_ECOGS": ["all", "no code", "NA", "wrong ID", "NA data"],
            "ECOG_ECOGSCD": ["1", "NA", "2", "3", "NA"],
        }
    )


"""
SubjectId,MHSPID,MHTERM,MHSTDAT,MHONGO,MHONGOCD,MHENDAT
Y_3466_1,01,Abdominal pain,1998-03-18,Current/active,3,
P_8841_1,01,Hypertension,1965-02-23,Current/active,3,
P_8841_1,02,Pain right leg,1923-12-21,Past,1,1978-01-12
"""


@pytest.fixture
def medical_history_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",
                "IMPRESS-X_0002_1",
                "IMPRESS-X_0003_1",
                "IMPRESS-X_0004_1",
                "IMPRESS-X_0005_1",
            ],
            "MH_MHTERM": ["pain", "hypertension", "dizziness", "pain", "rigor mortis"],
            "MH_MHSPID": ["01", "02", "03", "01", "01"],
            "MH_MHSTDAT": [
                "1900-09-NK",
                "1901-10-02",
                "1902-nk-nk",
                "1840-02-02",
                "1740-02-02",
            ],
            "MH_MHENDAT": [
                "",  # ongoing
                "1901-11-02",  # ended
                "1903-nk-nk",  # ongoing
                "1940-nk-02",  # ongoing
                "1940-02-02",  # ended
            ],
            "MH_MHONGO": [
                "Current/active",
                "Past",  # ended
                "Present/dormant",  # ongoing
                "Past",  # conflict w. ongoing
                "Past",  # ended
            ],
            "MH_MHONGOCD": [
                "1",
                "3",  # ended
                "2",  # ongoing
                "3",  # conflict w. ongoing
                "1",  # wrong code
            ],
        }
    )
