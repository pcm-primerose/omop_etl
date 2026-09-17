from pathlib import Path
from typing import cast

import psycopg

from omop_etl.db.core.sql import apply_sql_file, drop_tables


class _FakeConnection:
    """
    Duck-types psycopg.Connection's `execute` method, the loader's SQL
    helpers never touch anything else on the connection, cast to
    psycopg.Connection at each call site.
    """

    def __init__(self) -> None:
        self.executed: list[object] = []

    def execute(self, query: object) -> None:
        self.executed.append(query)


def test_drop_tables_is_a_no_op_for_an_empty_set():
    conn = _FakeConnection()

    drop_tables(cast(psycopg.Connection, cast(object, conn)), frozenset())

    assert conn.executed == []


def test_drop_tables_executes_one_statement_for_a_non_empty_set():
    conn = _FakeConnection()

    drop_tables(cast(psycopg.Connection, cast(object, conn)), frozenset({"person", "measurement"}))

    assert len(conn.executed) == 1


def test_drop_tables_names_every_table_and_cascades():
    conn = _FakeConnection()

    drop_tables(cast(psycopg.Connection, cast(object, conn)), frozenset({"person", "measurement"}))

    statement = conn.executed[0].as_string(None)
    assert statement == 'DROP TABLE IF EXISTS "public"."measurement", "public"."person" CASCADE'


def test_apply_sql_file_executes_the_files_full_content(tmp_path: Path):
    conn = _FakeConnection()
    sql_file = tmp_path / "test.sql"
    sql_file.write_text("CREATE TABLE public.x (a int); CREATE TABLE public.y (b int);")

    apply_sql_file(cast(psycopg.Connection, cast(object, conn)), sql_file)

    assert conn.executed == [sql_file.read_bytes()]
