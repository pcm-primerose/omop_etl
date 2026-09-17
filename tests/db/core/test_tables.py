import re

from omop_etl.db.core.tables import ALL_TABLES, VOCAB_TABLES
from omop_etl.db.service import DDL_PHASES, DEFAULT_DDL_DIR


def test_vocab_tables_is_a_subset_of_all_tables():
    assert VOCAB_TABLES <= ALL_TABLES


def test_all_tables_matches_the_real_ddl_files():
    # if DDL changes this fails instead of `ALL_TABLES` being out of sync
    combined = (DEFAULT_DDL_DIR / "cdm5.5_ddl_clinical.sql").read_text() + (DEFAULT_DDL_DIR / "cdm5.5_ddl_vocab.sql").read_text()
    declared = frozenset(re.findall(r"CREATE TABLE public\.(\w+)", combined))

    assert declared == ALL_TABLES


def test_vocab_tables_all_appear_in_the_vocab_ddl_file_not_the_clinical_one():
    vocab_text = (DEFAULT_DDL_DIR / "cdm5.5_ddl_vocab.sql").read_text()
    clinical_text = (DEFAULT_DDL_DIR / "cdm5.5_ddl_clinical.sql").read_text()
    declared_vocab = frozenset(re.findall(r"CREATE TABLE public\.(\w+)", vocab_text))
    declared_clinical = frozenset(re.findall(r"CREATE TABLE public\.(\w+)", clinical_text))

    assert VOCAB_TABLES == declared_vocab
    assert VOCAB_TABLES.isdisjoint(declared_clinical)


def test_every_ddl_phase_has_both_a_clinical_and_a_vocab_file():
    for phase in DDL_PHASES:
        assert (DEFAULT_DDL_DIR / f"{phase}_clinical.sql").is_file()
        assert (DEFAULT_DDL_DIR / f"{phase}_vocab.sql").is_file()
