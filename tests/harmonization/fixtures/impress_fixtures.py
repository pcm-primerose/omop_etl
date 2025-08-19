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
            "DM_SEX": ["Female", "Male", "female", "male", "error"],
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
                "BRAF Non-V600activating mutations",
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
def evaluability_fixture():
    return pl.DataFrame(
        data={
            "SubjectId": [
                "IMPRESS-X_0001_1",  # evaluable no tumor assessment
                "IMPRESS-X_0002_1",  # evaluable no EOT
                "IMPRESS-X_0003_1",  # evaluable all
                "IMPRESS-X_0004_1",  # not evaluable wrong treatment length
                "IMPRESS-X_0005_1",  # not evaluable missing assessment and EOT
                "IMPRESS-X_0006_1",  # not evaluable missing treatment dates
                "IMPRESS-X_0007_1",  # not evaluable negative treatment length
                "IMPRESS-X_0008_1",  # not evaluable all data missing
            ],
            "TR_TRO_STDT": [
                "2001-01-01",
                "2001-01-02",
                "2001-01-01",
                "2001-01-01",
                "2001-01-01",
                "NA",
                "2001-01-01",
                "NA",
            ],
            "TR_TROSTPDT": [
                "2001-03-01",
                "2001-05-01",
                "2001-04-10",
                "2001-01-05",
                "2001-03-01",
                "NA",
                "1999-01-01",
                "NA",
            ],
            "RA_EventDate": [
                "NA",
                "2001-01-02",
                "2001-01-01",
                "NA",
                "NA",
                "NA",
                "2000-01-01",
                "NA",
            ],
            "RNRSP_EventDate": [
                "NA",
                "NA",
                "2002-01-01",
                "2001-01-01",
                "NA",
                "NA",
                "NA",
                "NA",
            ],
            "RCNT_EventDate": ["NA", "NA", "2003-01-01", "NA", "NA", "NA", "NA", "NA"],
            "RNTMNT_EventDate": [
                "NA",
                "NA",
                "2004-01-01",
                "NA",
                "NA",
                "2002-05-01",
                "NA",
                "NA",
            ],
            "LUGRSP_EventDate": [
                "NA",
                "NA",
                "2005-01-01",
                "NA",
                "NA",
                "NA",
                "NA",
                "NA",
            ],
            "EOT_EventDate": [
                "2007-03-01",
                "NA",
                "2006-01-01",
                "2002-01-01",
                "NA",
                "2003-01-01",
                "2001-02-01",
                "NA",
            ],
        }
    )


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
        }
    )
