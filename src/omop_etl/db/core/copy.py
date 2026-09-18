import io
from pathlib import Path
from typing import Literal

import polars as pl
import psycopg
from psycopg.sql import SQL, Identifier

_DELIMITER: dict[str, str] = {"csv": ",", "text": "\t"}
_PARQUET_CHUNK_SIZE = 200_000


def copy_dir(conn: psycopg.Connection, dir_path: Path, table_names: frozenset[str], *, fmt: Literal["csv", "text"]) -> None:
    """
    COPY every table in `table_names` present in `dir_path`.
    Prefers a "<TABLE>.parquet" file (streamed in batches via polars, transcoded to
    CSV over the "<TABLE>.csv" files when both are present. A table with neither
    file present is silently skipped.
    """
    for table in sorted(table_names):
        parquet_path = dir_path / f"{table.upper()}.parquet"
        if parquet_path.exists():
            _copy_parquet_file(conn, table, parquet_path)
            continue
        csv_path = dir_path / f"{table.upper()}.csv"
        if csv_path.exists():
            _copy_csv_file(conn, table, csv_path, fmt=fmt)


def _copy_csv_file(conn: psycopg.Connection, table: str, csv_path: Path, *, fmt: Literal["csv", "text"]) -> None:
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


def _copy_parquet_file(conn: psycopg.Connection, table: str, parquet_path: Path) -> None:
    """
    Stream `parquet_path` into `table` in chunks using
    `collect_batches` (currently unstable according to polars' docs),
    transcoding each batch to CSV in memory instead of materializing
    the whole file.
    """
    lf = pl.scan_parquet(parquet_path)
    columns = lf.collect_schema().names()
    copy_sql = SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER, NULL '')").format(
        Identifier("public", table),
        SQL(", ").join(Identifier(c) for c in columns),
    )
    with conn.cursor().copy(copy_sql) as copy:
        first_batch = True
        for batch in lf.collect_batches(chunk_size=_PARQUET_CHUNK_SIZE):
            buf = io.BytesIO()
            batch.write_csv(buf, include_header=first_batch)
            first_batch = False
            copy.write(buf.getvalue())
