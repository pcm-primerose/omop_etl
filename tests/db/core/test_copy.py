from pathlib import Path
from typing import cast
import psycopg

from omop_etl.db.core.copy import copy_dir


class _FakeCopy:
    def __init__(self) -> None:
        self.written = b""

    def write(self, chunk: bytes) -> None:
        self.written += chunk

    def __enter__(self) -> "_FakeCopy":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class _FakeCursor:
    def __init__(self) -> None:
        self.copy_calls: list[object] = []
        self.copies: list[_FakeCopy] = []

    def copy(self, sql: object) -> _FakeCopy:
        self.copy_calls.append(sql)
        copy = _FakeCopy()
        self.copies.append(copy)
        return copy


class _FakeConnection:
    """
    Duck-types psycopg.Connection's `cursor` method only, copy_dir never
    touches anything else on the connection, just cast to psycopg.Connection at
    call sites.
    """

    def __init__(self) -> None:
        self._cursor = _FakeCursor()

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_copy_dir_skips_files_not_in_table_names(tmp_path: Path):
    (tmp_path / "CONCEPT.csv").write_text("concept_id\tconcept_name\n1\tx\n")
    (tmp_path / "CONCEPT_CPT4.csv").write_text("some\tunrelated\theader\n")
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"concept"}), fmt="text")

    assert len(conn._cursor.copy_calls) == 1


def test_copy_dir_processes_every_matching_file(tmp_path: Path):
    (tmp_path / "PERSON.csv").write_text("person_id,gender_concept_id\n1,8507\n")
    (tmp_path / "MEASUREMENT.csv").write_text("measurement_id,person_id\n1,1\n")
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"person", "measurement"}), fmt="csv")

    assert len(conn._cursor.copy_calls) == 2


def test_copy_dir_writes_the_files_full_content_after_the_header(tmp_path: Path):
    content = "person_id,gender_concept_id\n1,8507\n2,8532\n"
    (tmp_path / "PERSON.csv").write_text(content)
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"person"}), fmt="csv")

    assert conn._cursor.copies[0].written == content.encode()


def test_copy_dir_does_nothing_for_an_empty_directory(tmp_path: Path):
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"person"}), fmt="csv")

    assert conn._cursor.copy_calls == []


def test_copy_dir_names_the_table_and_format_in_the_copy_statement(tmp_path: Path):
    (tmp_path / "PERSON.csv").write_text("person_id,gender_concept_id\n1,8507\n")
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"person"}), fmt="csv")

    statement = conn._cursor.copy_calls[0].as_string(None)
    assert statement == 'COPY "public"."person" ("person_id", "gender_concept_id") FROM STDIN WITH (FORMAT csv, HEADER, NULL \'\')'


def test_copy_dir_uses_the_files_own_header_order_not_the_tables(tmp_path: Path):
    # the DDL declares person_id before gender_concept_id, this file reverses them,
    # should work regardless
    (tmp_path / "PERSON.csv").write_text("gender_concept_id,person_id\n8507,1\n")
    conn = _FakeConnection()

    copy_dir(cast(psycopg.Connection, cast(object, conn)), tmp_path, frozenset({"person"}), fmt="csv")

    statement = conn._cursor.copy_calls[0].as_string(None)
    assert statement == 'COPY "public"."person" ("gender_concept_id", "person_id") FROM STDIN WITH (FORMAT csv, HEADER, NULL \'\')'
