from pathlib import Path

import psycopg
from psycopg.sql import SQL, Identifier


def drop_tables(conn: psycopg.Connection, table_names: frozenset[str]) -> None:
    if not table_names:
        return
    targets = SQL(", ").join(Identifier("public", t) for t in sorted(table_names))
    conn.execute(SQL("DROP TABLE IF EXISTS {} CASCADE").format(targets))


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    """Execute all SQL statements in `path` as a single query batch."""
    conn.execute(path.read_text().encode())
