import json
from pathlib import Path
from typing import Dict
import polars as pl
import pytest
import datetime as dt

from omop_etl.harmonization.core.io_export import HarmonizedExporter
from omop_etl.infra.io.types import Layout, TabularFormat, WideFormat
from omop_etl.infra.utils.run_context import RunMetadata


@pytest.fixture
def run_context() -> RunMetadata:
    return RunMetadata(trial="test_trial", run_id="abc123", started_at="20231201_143000")


@pytest.fixture
def exporter(tmp_path: Path) -> HarmonizedExporter:
    # write into tmp_path / runs / ...
    return HarmonizedExporter(base_out=tmp_path, layout=Layout.TRIAL_TIMESTAMP_RUN)


class FakeHarmonizedData:
    def __init__(self) -> None:
        self._wide = pl.DataFrame(
            [
                pl.Series("patient_id", ["P1", "P2"], dtype=pl.Utf8),
                pl.Series("trial_id", ["T", "T"], dtype=pl.Utf8),
                pl.Series("best_overall_response.code", [2, None], dtype=pl.Int64),
                pl.Series(
                    "best_overall_response.date",
                    [dt.date(2024, 1, 1), None],
                    dtype=pl.Date,
                ),
            ]
        )

        self._norm: Dict[str, pl.DataFrame] = {
            "patients": pl.DataFrame(
                [
                    pl.Series("patient_id", ["P1", "P2"], dtype=pl.Utf8),
                    pl.Series("trial_id", ["T", "T"], dtype=pl.Utf8),
                    pl.Series("age", [60, 70], dtype=pl.Int64),
                ]
            ),
            "adverse_events": pl.DataFrame(
                [
                    pl.Series("patient_id", ["P1", "P1"], dtype=pl.Utf8),
                    pl.Series("row_index", [0, 1], dtype=pl.Int64),
                    pl.Series("ae.term", ["Headache", "Nausea"], dtype=pl.Utf8),
                ]
            ),
        }

        # for json output size
        self.patients = [{"id": "P1"}, {"id": "P2"}]

    def to_dataframe_wide(self) -> pl.DataFrame:
        return self._wide

    def to_frames_normalized(self) -> Dict[str, pl.DataFrame]:
        return self._norm

    @staticmethod
    def to_dict() -> dict:
        return {
            "trial_id": "T",
            "patients": [{"patient_id": "P1"}, {"patient_id": "P2"}],
        }


@pytest.fixture
def fake_hd() -> FakeHarmonizedData:
    return FakeHarmonizedData()


def test_export_wide_csv(exporter: HarmonizedExporter, run_context: RunMetadata, fake_hd: FakeHarmonizedData, tmp_path: Path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("dummy\n")

    out = exporter.export_wide(
        hd=fake_hd,  # type: ignore
        meta=run_context,
        input_path=input_path,
        formats=["csv"],
    )

    ctx = out["csv"]
    assert ctx.data_path.suffix == ".csv"
    assert ctx.data_path.exists()

    df = pl.read_csv(ctx.data_path)
    assert "patient_id" in df.columns
    assert "best_overall_response.code" in df.columns

    manifest = json.loads(ctx.manifest_path.read_text())
    assert manifest["trial"] == run_context.trial
    assert manifest["run_id"] == run_context.run_id
    assert manifest["started_at"] == run_context.started_at
    assert manifest["format"] == "csv"
    assert manifest["mode"] == "wide"
    assert Path(manifest["output"]).name == ctx.data_path.name


@pytest.mark.parametrize("fmt", ["parquet", "json"])
def test_export_wide_other_formats(
    fmt: WideFormat, exporter: HarmonizedExporter, run_context: RunMetadata, fake_hd: FakeHarmonizedData, tmp_path: Path
):
    input_path = tmp_path / "input.csv"
    input_path.write_text("dummy\n")

    out = exporter.export_wide(
        hd=fake_hd,  # type: ignore
        meta=run_context,
        input_path=input_path,
        formats=[fmt],
    )
    ctx = out[fmt]

    if fmt == "parquet":
        assert ctx.data_path.suffix == ".parquet"
        df = pl.read_parquet(ctx.data_path)
        assert df.shape[0] == 2
    elif fmt == "json":
        assert ctx.data_path.suffix == ".json"
        data = json.loads(ctx.data_path.read_text())
        assert "patients" in data

    manifest = json.loads(ctx.manifest_path.read_text())
    assert manifest["format"] == fmt
    assert manifest["mode"] == "wide"


@pytest.mark.parametrize("fmt", ["csv", "tsv", "parquet"])
def test_export_normalized(fmt: TabularFormat, exporter: HarmonizedExporter, run_context: RunMetadata, fake_hd: FakeHarmonizedData, tmp_path: Path):
    input_path = tmp_path / "input.csv"
    input_path.write_text("dummy\n")

    out = exporter.export_normalized(
        hd=fake_hd,  # type: ignore
        meta=run_context,
        input_path=input_path,
        formats=[fmt],
    )
    ctx = out[fmt]

    if fmt == "csv":
        patients_path = ctx.data_dir / "patients.csv"
    elif fmt == "tsv":
        patients_path = ctx.data_dir / "patients.tsv"
    else:
        patients_path = ctx.data_dir / "patients.parquet"

    assert patients_path.exists(), "Expected 'patients' table to be written"

    if fmt == "parquet":
        df = pl.read_parquet(patients_path)
    elif fmt == "csv":
        df = pl.read_csv(patients_path)
    else:
        df = pl.read_csv(patients_path, separator="\t")

    assert "patient_id" in df.columns

    manifest = json.loads(ctx.manifest_path.read_text())
    assert manifest["format"] == fmt
    assert manifest["mode"] == "normalized"
    assert Path(manifest["output"]).exists()
