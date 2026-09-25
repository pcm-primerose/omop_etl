from pathlib import Path
from typing import cast
import psycopg
import pytest
from psycopg.sql import Composed

import omop_etl.db.service as service
from omop_etl.db.core.tables import ALL_TABLES, VOCAB_TABLES
from omop_etl.db.service import DDL_PHASES, DbLoadService


class _NullContext:
    def __enter__(self) -> "_NullContext":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class _FakeVersionConn:
    """
    `missing_table=True` simulates a fresh database with no `cdm_source`
    yet (psycopg raises UndefinedTable on the SELECT).
    """

    def __init__(self, *, version: str | None = None, missing_table: bool = False) -> None:
        self._version = version
        self._missing_table = missing_table
        self.rolled_back = False

    def execute(self, query: object) -> "_FakeVersionConn":
        if self._missing_table:
            raise psycopg.errors.UndefinedTable('relation "cdm_source" does not exist')
        return self

    def fetchone(self) -> tuple[str] | None:
        return None if self._version is None else (self._version,)

    def rollback(self) -> None:
        self.rolled_back = True

    def __enter__(self) -> "_FakeVersionConn":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


class _FakeLoadConn:
    def __init__(self) -> None:
        self.executed: list[object] = []

    def transaction(self) -> _NullContext:
        return _NullContext()

    def execute(self, query: object) -> None:
        self.executed.append(query)

    def __enter__(self) -> "_FakeLoadConn":
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def _install_fake_connect(monkeypatch: pytest.MonkeyPatch, version_conn: _FakeVersionConn, load_conn: _FakeLoadConn) -> None:
    connects = iter([version_conn, load_conn])
    monkeypatch.setattr(service.psycopg, "connect", lambda dsn: next(connects))


def _install_spies(monkeypatch: pytest.MonkeyPatch) -> list[tuple[object, ...]]:
    calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(service, "drop_tables", lambda conn, tables: calls.append(("drop", frozenset(tables))))
    monkeypatch.setattr(service, "apply_sql_file", lambda conn, path: calls.append(("ddl", path.name)))
    monkeypatch.setattr(
        service,
        "copy_dir",
        lambda conn, dir_path, tables, *, fmt: calls.append(("copy", dir_path, frozenset(tables), fmt)),
    )
    return calls


def _run_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, existing_version: str | None, missing_table: bool = False
) -> tuple[list[tuple[object, ...]], _FakeLoadConn]:
    calls = _install_spies(monkeypatch)
    version_conn = _FakeVersionConn(version=existing_version, missing_table=missing_table)
    load_conn = _FakeLoadConn()
    _install_fake_connect(monkeypatch, version_conn, load_conn)

    omop_dir = tmp_path / "omop"
    athena_dir = tmp_path / "athena"
    svc = DbLoadService(dsn="postgresql://test", ddl_dir=tmp_path / "ddl")
    svc.load(omop_dir=omop_dir, athena_dir=athena_dir, athena_version="v5.0")

    return calls, load_conn


def _ddl_calls(calls: list[tuple[object, ...]]) -> list[object]:
    return [call[1] for call in calls if call[0] == "ddl"]


def _copy_calls(calls: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    return [c for c in calls if c[0] == "copy"]


def test_fresh_database_drops_and_reloads_every_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version=None, missing_table=True)

    assert calls[0] == ("drop", ALL_TABLES)


def test_version_mismatch_drops_and_reloads_every_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version="v4.0")

    assert calls[0] == ("drop", ALL_TABLES)


def test_matching_version_only_drops_clinical_tables(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version="v5.0")

    assert calls[0] == ("drop", ALL_TABLES - VOCAB_TABLES)


def test_reload_applies_both_halves_of_every_ddl_phase_in_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version=None, missing_table=True)

    expected = [f"{phase}_{half}.sql" for phase in DDL_PHASES for half in ("clinical", "vocab")]
    assert _ddl_calls(calls) == expected


def test_reuse_vocab_skips_every_vocab_ddl_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version="v5.0")

    expected = [f"{phase}_clinical.sql" for phase in DDL_PHASES]
    assert _ddl_calls(calls) == expected


def test_reload_copies_vocab_then_clinical_data_right_after_the_ddl_phase(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version=None, missing_table=True)

    # data must land after tables exist (post cdm5.5_ddl) but before pk/constraints/indices
    ddl_index = calls.index(("ddl", "cdm5.5_ddl_clinical.sql"))
    next_ddl_index = calls.index(("ddl", "cdm5.5_primary_keys_clinical.sql"))
    copies = _copy_calls(calls)

    assert len(copies) == 2
    assert ddl_index < calls.index(copies[0]) < next_ddl_index
    assert copies[0] == ("copy", tmp_path / "athena", VOCAB_TABLES, "text")
    assert copies[1] == ("copy", tmp_path / "omop", ALL_TABLES - VOCAB_TABLES, "csv")


def test_reuse_vocab_only_copies_clinical_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls, _ = _run_load(monkeypatch, tmp_path, existing_version="v5.0")

    copies = _copy_calls(calls)
    assert copies == [("copy", tmp_path / "omop", ALL_TABLES - VOCAB_TABLES, "csv")]


def test_load_analyzes_after_everything_else(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _, load_conn = _run_load(monkeypatch, tmp_path, existing_version="v5.0")

    assert load_conn.executed[-1] == "ANALYZE"


def test_load_tunes_the_session_before_anything_else(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _, load_conn = _run_load(monkeypatch, tmp_path, existing_version="v5.0")

    assert len(load_conn.executed) == 4  # 3 tuning statements + ANALYZE


def test_tune_for_bulk_load_sets_expected_session_parameters():
    conn = _FakeLoadConn()

    DbLoadService._tune_for_bulk_load(cast(psycopg.Connection, cast(object, conn)))

    statements = [s.as_string(None) if isinstance(s, Composed) else s for s in conn.executed]
    assert statements == [
        f"SET maintenance_work_mem = '{service.MAINTENANCE_WORK_MEM}'",
        f"SET max_parallel_maintenance_workers = {service.MAX_PARALLEL_MAINTENANCE_WORKERS}",
        "SET synchronous_commit = off",
    ]


def test_existing_vocabulary_version_is_none_when_cdm_source_is_missing(monkeypatch: pytest.MonkeyPatch):
    version_conn = _FakeVersionConn(missing_table=True)
    monkeypatch.setattr(service.psycopg, "connect", lambda dsn: version_conn)

    result = DbLoadService(dsn="postgresql://test")._existing_vocabulary_version()

    assert result is None
    assert version_conn.rolled_back


def test_existing_vocabulary_version_is_none_for_an_empty_table(monkeypatch: pytest.MonkeyPatch):
    version_conn = _FakeVersionConn(version=None)
    monkeypatch.setattr(service.psycopg, "connect", lambda dsn: version_conn)

    result = DbLoadService(dsn="postgresql://test")._existing_vocabulary_version()

    assert result is None


def test_existing_vocabulary_version_returns_the_stored_version(monkeypatch: pytest.MonkeyPatch):
    version_conn = _FakeVersionConn(version="v5.0")
    monkeypatch.setattr(service.psycopg, "connect", lambda dsn: version_conn)

    result = DbLoadService(dsn="postgresql://test")._existing_vocabulary_version()

    assert result == "v5.0"
