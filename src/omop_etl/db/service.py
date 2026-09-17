from logging import getLogger
from pathlib import Path
import psycopg

from omop_etl.db.core.copy import copy_dir
from omop_etl.db.core.sql import apply_sql_file, drop_tables
from omop_etl.db.core.tables import ALL_TABLES, VOCAB_TABLES

log = getLogger(__name__)

DEFAULT_DDL_DIR = Path(__file__).parent / "ddl"

# applied in this order every load: clinical half then vocab if reloading it
DDL_PHASES = ("cdm5.5_ddl", "cdm5.5_primary_keys", "cdm5.5_constraints", "cdm5.5_indices")


class DbLoadService:
    """
    Loads and constrains an OMOP CDM database.
    Full reload of ETL-generated clinical data on every run.
    The Athena vocabulary is replaced if the database's vocabulary is
    a different version than the one used in the ETL (from checking `cdm_source`).
    """

    def __init__(self, dsn: str, ddl_dir: Path = DEFAULT_DDL_DIR):
        self._dsn = dsn
        self._ddl_dir = ddl_dir

    def load(self, *, omop_dir: Path, athena_dir: Path, athena_version: str) -> None:
        existing_version = self._existing_vocabulary_version()
        reuse_vocab = existing_version == athena_version

        if reuse_vocab:
            log.info("Vocabulary version %r already loaded, leaving vocab tables untouched", athena_version)
        elif existing_version is not None:
            log.info("Vocabulary version mismatch (loaded=%r, this run=%r), reloading vocab", existing_version, athena_version)
        else:
            log.info("No prior vocabulary version found, loading vocab")

        with psycopg.connect(self._dsn) as conn, conn.transaction():
            drop_tables(conn, ALL_TABLES if not reuse_vocab else ALL_TABLES - VOCAB_TABLES)
            for phase in DDL_PHASES:
                apply_sql_file(conn, self._ddl_dir / f"{phase}_clinical.sql")
                if not reuse_vocab:
                    apply_sql_file(conn, self._ddl_dir / f"{phase}_vocab.sql")
                if phase == "cdm5.5_ddl":
                    if not reuse_vocab:
                        copy_dir(conn, athena_dir, VOCAB_TABLES, fmt="text")
                    copy_dir(conn, omop_dir, ALL_TABLES - VOCAB_TABLES, fmt="csv")
            conn.execute("ANALYZE")

    def _existing_vocabulary_version(self) -> str | None:
        """
        The vocabulary_version already in the target database's cdm_source,
        or None if there's nothing to compare against (fresh db or missing `cdm_source`).
        Done with a separate connection so a missing table here don't abort the main load.
        """
        with psycopg.connect(self._dsn) as conn:
            try:
                row = conn.execute("SELECT vocabulary_version FROM cdm_source LIMIT 1").fetchone()
            except psycopg.errors.UndefinedTable:
                conn.rollback()
                return None
            return None if row is None else row[0]
