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
