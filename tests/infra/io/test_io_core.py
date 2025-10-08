import json
import datetime as dt
from pathlib import Path
import polars as pl
import pytest

from omop_etl.infra.io.io_core import (
    write_frame,
    write_frames_dir,
    write_json,
    write_manifest,
    TableMeta,
)
from omop_etl.infra.io.options import (
    CsvOptions,
    ParquetOptions,
    JsonOptions,
)


@pytest.fixture
def df_people() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2], "name": ["A", "B"], "age": [10, 20]})


@pytest.fixture
def frames_norm() -> dict[str, pl.DataFrame]:
    return {
        "patients": pl.DataFrame({"patient_id": ["p1", "p2"], "trial_id": ["t", "t"]}),
        "visits": pl.DataFrame({"patient_id": ["p1", "p2"], "row_index": [0, 0]}),
        "empty_table": pl.DataFrame({"x": []}),  # should skip
    }


def test_write_frame_csv(tmp_path: Path, df_people: pl.DataFrame):
    p = tmp_path / "out.csv"
    res = write_frame(df_people, p, "csv", CsvOptions(separator=","))
    assert p.is_file()
    read = pl.read_csv(p)
    assert read.shape == df_people.shape
    assert isinstance(res.tables["wide"], TableMeta)
    assert res.main_file == p


def test_write_frame_tsv_separator(tmp_path: Path, df_people: pl.DataFrame):
    # kinda pointless since polars has no sep attr
    p = tmp_path / "out.tsv"
    res = write_frame(df_people, p, "tsv", CsvOptions(separator="\t"))
    assert p.is_file()
    read = pl.read_csv(p, separator="\t")
    assert read.shape == df_people.shape
    assert res.main_file == p


def test_write_frame_parquet(tmp_path: Path, df_people: pl.DataFrame):
    p = tmp_path / "out.parquet"
    res = write_frame(df_people, p, "parquet", ParquetOptions(compression="zstd"))
    assert p.is_file()
    read = pl.read_parquet(p)
    assert read.shape == df_people.shape
    assert res.main_file == p


def test_write_frame_invalid_raises(tmp_path: Path, df_people: pl.DataFrame):
    with pytest.raises(ValueError, match="Unsupported tabular fmt"):
        write_frame(df_people, tmp_path / "x.xlsx", "xlsx")  # type: ignore


def test_write_frames_dir_tabular_csv(tmp_path: Path, frames_norm: dict[str, pl.DataFrame]):
    outdir = tmp_path / "dir_csv"
    res = write_frames_dir(frames_norm, outdir, "csv", CsvOptions(separator=","))
    # empty_table should be skipped
    assert (outdir / "patients.csv").is_file()
    assert (outdir / "visits.csv").is_file()
    assert not (outdir / "empty_table.csv").exists()

    # main_file should prefer patients
    assert res.main_file == outdir / "patients.csv"
    # tables metadata present for written tables
    assert set(res.table_files.keys()) == {"patients", "visits"}
    assert all(isinstance(m, TableMeta) for m in res.tables.values())

    patients = pl.read_csv(outdir / "patients.csv")
    assert patients.columns == ["patient_id", "trial_id"]


def test_write_frames_dir_parquet(tmp_path: Path, frames_norm: dict[str, pl.DataFrame]):
    outdir = tmp_path / "dir_parq"
    res = write_frames_dir(frames_norm, outdir, "parquet")
    assert (outdir / "patients.parquet").is_file()
    assert (outdir / "visits.parquet").is_file()
    assert res.main_file == outdir / "patients.parquet"


def test_write_json_serializes_dates(tmp_path: Path):
    p = tmp_path / "obj.json"
    obj = {"d": dt.date(2020, 1, 2), "ts": dt.datetime(2020, 1, 2, 3, 4, 5)}
    res = write_json(obj, p, JsonOptions(indent=2))
    assert p.is_file()
    loaded = json.loads(p.read_text())
    # ISOJSONEncoder should produce ISO strings
    assert isinstance(loaded["d"], str) and loaded["d"].startswith("2020-01-02")
    assert isinstance(loaded["ts"], str) and "2020-01-02" in loaded["ts"]
    assert res.main_file == p


def test_write_manifest_writes_pretty_json(tmp_path: Path):
    p = tmp_path / "manifest.json"
    doc = {"trial": "X", "fmt": "csv"}
    write_manifest(doc, p)
    assert p.is_file()
    assert json.loads(p.read_text()) == doc
