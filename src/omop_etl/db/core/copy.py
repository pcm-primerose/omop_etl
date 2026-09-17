from pathlib import Path
from typing import Literal

import psycopg
from psycopg.sql import SQL, Identifier

_DELIMITER: dict[str, str] = {"csv": ",", "text": "\t"}


def copy_dir(conn: psycopg.Connection, dir_path: Path, table_names: frozenset[str], *, fmt: Literal["csv", "text"]) -> None:
    """
    COPY every "<table>.csv" in `dir_path` whose lowercased stem matches a
    table name in `table_names`, anything else is skipped.
    """
    for csv_path in sorted(dir_path.glob("*.csv")):
        table = csv_path.stem.lower()
        if table not in table_names:
            continue
        with csv_path.open("rb") as f:
            header = f.readline().decode().rstrip("\r\n")
            columns = header.split(_DELIMITER[fmt])
            f.seek(0)
            # HEADER only tells PostgreSQL to skip validation for the first input line,
            # it doesn't use the header to map or reorder columns
            # (bc omop row models don't declare fields in the same order).
            # NULLs are bare null-tokens in both sources, so is explicitly used here as well
            copy_sql = SQL("COPY {} ({}) FROM STDIN WITH (FORMAT {}, HEADER, NULL '')").format(
                Identifier("public", table),
                SQL(", ").join(Identifier(c) for c in columns),
                SQL(fmt),
            )
            with conn.cursor().copy(copy_sql) as copy:
                while chunk := f.read(1 << 20):
                    copy.write(chunk)
