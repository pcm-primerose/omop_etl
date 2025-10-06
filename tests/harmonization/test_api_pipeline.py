from pathlib import Path
from typing import Any, Dict
import json
import polars as pl
import pytest

from omop_etl.harmonization.api import HarmonizationService
from omop_etl.harmonization.core.pipeline import _read_input
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata


class _FakeHD:
    """
    Minimal HarmonizedData double:
    - to_dataframe_wide(): a tiny DF with mixed types
    - to_frames_normalized(): two small tables
    - to_dict(): tiny JSON-able object (for wide/json)
    """

    def __init__(self) -> None:
        self.patients = [{"patient_id": "P1"}, {"patient_id": "P2"}]

    @staticmethod
    def to_dataframe_wide() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "patient_id": ["P1", "P2"],
                "trial_id": ["IMPRESS", "IMPRESS"],
                "best_overall_response.code": [2, None],
                "best_overall_response.date": [pl.date(2020, 1, 1), None],
                "best_overall_response.response": ["PR", None],
            }
        )

    @staticmethod
    def to_frames_normalized() -> Dict[str, pl.DataFrame]:
        return {
            "patients": pl.DataFrame({"patient_id": ["P1", "P2"], "trial_id": ["IMPRESS", "IMPRESS"]}),
            "best_overall_response": pl.DataFrame(
                {
                    "patient_id": ["P1"],
                    "best_overall_response.code": [2],
                    "best_overall_response.response": ["PR"],
                }
            ),
        }

    @staticmethod
    def to_dict() -> Dict[str, Any]:
        return {"trial_id": "IMPRESS", "n": 2}


class _FakeHarmonizer:
    def __init__(self, df: pl.DataFrame, trial_id: str) -> None:
        self.df = df
        self.trial_id = trial_id

    def process(self) -> _FakeHD:
        return _FakeHD()


def test__read_input_routes_by_suffix(tmp_path: Path):
    csv = tmp_path / "x.csv"
    tsv = tmp_path / "x.tsv"
    pq = tmp_path / "x.parquet"

    pl.DataFrame({"a": [1]}).write_csv(csv)
    pl.DataFrame({"a": [2]}).write_csv(tsv, separator="\t")
    pl.DataFrame({"a": [3]}).write_parquet(pq)

    assert _read_input(csv)["a"].to_list() == [1]
    assert _read_input(tsv)["a"].to_list() == [2]
    assert _read_input(pq)["a"].to_list() == [3]

    with pytest.raises(ValueError):
        _read_input(tmp_path / "x.xlsx")


@pytest.fixture
def run_meta() -> RunMetadata:
    return RunMetadata(trial="impress", run_id="abc123", started_at="20240101T000000Z")


def _make_input_csv(tmp_path: Path) -> Path:
    p = tmp_path / "input.csv"
    pl.DataFrame(
        {"SubjectId": ["P1", "P2"], "trial_id": ["IMPRESS", "IMPRESS"], "COH_COHORTNAME": ["A", "B"]},
    ).write_csv(p)
    return p


def _patch_resolver(monkeypatch):
    from omop_etl.harmonization.core import dispatch as dispatch_mod

    monkeypatch.setattr(dispatch_mod, "resolve_harmonizer", lambda trial: _FakeHarmonizer)


def test_service_wide_csv(tmp_path: Path, run_meta: RunMetadata, monkeypatch):
    _patch_resolver(monkeypatch)
    svc = HarmonizationService(outdir=tmp_path, layout=Layout.TRIAL_RUN)
    inp = _make_input_csv(tmp_path)

    hd = svc.run(
        trial="IMPRESS",
        write_wide=True,
        write_normalized=False,
        input_path=inp,
        meta=run_meta,
        formats=["csv"],
    )
    assert hasattr(hd, "to_dataframe_wide")

    # assert files lands under: runs/ts_run/harmonized/impress/...
    seg = f"{run_meta.started_at}_{run_meta.run_id}"
    base = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_wide" / "csv"
    assert (base / f"impress_{run_meta.run_id}_{run_meta.started_at}_harmonized_wide.csv").is_file()
    manifest_p = base / f"impress_{run_meta.run_id}_{run_meta.started_at}_harmonized_wide_manifest.json"
    assert manifest_p.is_file()
    json.loads(manifest_p.read_text())


@pytest.mark.parametrize("fmt", ["csv", "tsv", "parquet"])
def test_service_normalized_tabular(tmp_path: Path, run_meta: RunMetadata, fmt: str, monkeypatch):
    _patch_resolver(monkeypatch)
    svc = HarmonizationService(outdir=tmp_path, layout=Layout.TRIAL_RUN)
    inp = _make_input_csv(tmp_path)

    svc.run(
        trial="IMPRESS",
        write_wide=False,
        write_normalized=True,
        input_path=inp,
        meta=run_meta,
        formats=[fmt],  # type: ignore
    )

    seg = f"{run_meta.started_at}_{run_meta.run_id}"
    base = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_norm" / fmt
    # patients table should exist
    ext = ".tsv" if fmt == "tsv" else (".csv" if fmt == "csv" else ".parquet")
    patients = base / f"patients{ext}"
    assert patients.is_file()


def test_service_both_modes_multiple_formats(tmp_path: Path, run_meta: RunMetadata, monkeypatch):
    _patch_resolver(monkeypatch)
    svc = HarmonizationService(outdir=tmp_path, layout=Layout.TRIAL_RUN)
    inp = _make_input_csv(tmp_path)

    svc.run(
        trial="IMPRESS",
        write_wide=True,
        write_normalized=True,
        input_path=inp,
        meta=run_meta,
        formats=["csv", "parquet"],
    )

    seg = f"{run_meta.started_at}_{run_meta.run_id}"
    wide_csv = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_wide" / "csv"
    wide_pq = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_wide" / "parquet"
    norm_csv = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_norm" / "csv"
    norm_pq = tmp_path / "runs" / seg / "harmonized" / "impress" / "harmonized_norm" / "parquet"

    assert any(p.suffix == ".csv" for p in wide_csv.glob("*.csv"))
    assert any(p.suffix == ".parquet" for p in wide_pq.glob("*.parquet"))
    assert (norm_csv / "patients.csv").is_file()
    assert (norm_pq / "patients.parquet").is_file()
