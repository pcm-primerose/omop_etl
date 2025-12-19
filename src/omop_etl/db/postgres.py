from dataclasses import is_dataclass
from typing import Sequence

import psycopg


class PostgresOmopWriter:
    """
    Minimal loader, currently:
      - one transaction
      - optional TRUNCATE
      - COPY FROM STDIN
    """

    def __init__(
        self,
        dsn: str,
        schema: str = "public",
        truncate_first: bool = False,
    ):
        self._dsn = dsn
        self._schema = schema
        self._truncate_first = truncate_first

    def write(self, tables) -> None:
        person_rows = list(getattr(tables, "person"))
        obs_rows = list(getattr(tables, "observation_period"))
        cdm_src = getattr(tables, "cdm_source")
        cdm_rows = list(cdm_src) if isinstance(cdm_src, list) else [cdm_src]

        with psycopg.connect(self._dsn) as conn:
            conn.execute(f"SET search_path TO {self._schema}")

            with conn.transaction():
                if self._truncate_first:
                    # fixme: choose schemas
                    conn.execute("TRUNCATE TABLE cdm_source")
                    conn.execute("TRUNCATE TABLE observation_period")
                    conn.execute("TRUNCATE TABLE person")

                if person_rows:
                    self._copy_dataclasses(conn, "person", person_rows)
                if obs_rows:
                    self._copy_dataclasses(conn, "observation_period", obs_rows)
                if cdm_rows and cdm_rows[0] is not None:
                    self._copy_dataclasses(conn, "cdm_source", cdm_rows)

    @staticmethod
    def _copy_dataclasses(conn: psycopg.Connection, table: str, rows: Sequence[object]) -> None:
        first = rows[0]
        if not is_dataclass(first):
            raise TypeError(f"Expected dataclass rows for {table}, got: {type(first)}")

        # takes dataclass fields in declared order
        cols = tuple(getattr(first, "__dataclass_fields__").keys())
        col_sql = ", ".join(cols)
        copy_sql = f"COPY {table} ({col_sql}) FROM STDIN"

        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for r in rows:
                    # psycopg3 handles NULLs and basic type formatting here.
                    values = [getattr(r, c) for c in cols]
                    copy.write_row(values)
