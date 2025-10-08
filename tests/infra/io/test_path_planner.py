from pathlib import Path
import pytest

from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.path_planner import (
    run_root,
    plan_single_file,
    plan_table_dir,
)
from omop_etl.infra.io.types import TabularFormat, WideFormat


@pytest.fixture
def meta() -> RunMetadata:
    return RunMetadata(trial="impress", run_id="c593d23d", started_at="20251006T114400Z")


def _seg(meta: RunMetadata) -> str:
    return f"{meta.started_at}_{meta.run_id}"


def test_run_root(tmp_path: Path, meta: RunMetadata):
    rr = run_root(tmp_path, meta)
    assert rr == tmp_path / "runs" / _seg(meta)


@pytest.mark.parametrize("fmt", ["csv", "tsv", "parquet", "json"])
@pytest.mark.parametrize("mode", ["preprocessed", "harmonized_wide"])
def test_plan_single_file_layout_and_names(tmp_path: Path, meta: RunMetadata, fmt: WideFormat, mode: str):
    ctx = plan_single_file(
        base_out=tmp_path,
        meta=meta,
        module="preprocessed" if mode == "preprocessed" else "harmonized",
        trial=meta.trial,
        mode=mode,  # type: ignore
        fmt=fmt,
        filename_base="{trial}_{run_id}_{started_at}_{mode}",
    )

    # dir shape
    expect_dir = tmp_path / "runs" / _seg(meta) / ("preprocessed" if mode == "preprocessed" else "harmonized") / "impress" / mode / fmt
    assert ctx.base_dir == expect_dir
    assert ctx.data_dir == expect_dir

    stem = f"impress_{meta.run_id}_{meta.started_at}_{mode}"
    assert (
        ctx.data_path.name == f"{stem}.{('tsv' if fmt == 'tsv' else 'csv' if fmt == 'csv' else 'parquet' if fmt == 'parquet' else 'json')}"
    )
    assert ctx.manifest_path.name == f"{stem}_manifest.json"
    assert ctx.log_path.name == f"{stem}.log"


@pytest.mark.parametrize("fmt", ["csv", "tsv", "parquet"])
def test_plan_table_dir_layout_and_names(tmp_path: Path, meta: RunMetadata, fmt: TabularFormat):
    mode = "harmonized_norm"
    ctx = plan_table_dir(
        base_out=tmp_path,
        meta=meta,
        module="harmonized",
        trial=meta.trial,
        mode=mode,  # type: ignore
        fmt=fmt,
        filename_base="{trial}_{run_id}_{started_at}_{mode}",
    )

    expect_dir = tmp_path / "runs" / _seg(meta) / "harmonized" / "impress" / mode / fmt
    assert ctx.base_dir == expect_dir
    assert ctx.data_dir == expect_dir

    # stamped names in same dir
    stem = f"impress_{meta.run_id}_{meta.started_at}_{mode}"
    assert ctx.manifest_path.name == f"{stem}_manifest.json"
    assert ctx.log_path.name == f"{stem}.log"

    # data_path is a sentinel, not used by writers in preprocessor, but should be stamped
    assert ctx.data_path.name.startswith(stem)
    assert ctx.data_path.suffix in (".csv", ".tsv", ".parquet")


def test_plan_single_file_missing_template_key_raises(tmp_path: Path, meta: RunMetadata):
    with pytest.raises(ValueError) as ex:
        plan_single_file(
            base_out=tmp_path,
            meta=meta,
            module="preprocessed",
            trial=meta.trial,
            mode="preprocessed",
            fmt="csv",
            filename_base="{trial}_{run_id}_{started_at}_{mode}_{whoops}",  # unknown key
        )
    assert "Missing template key" in str(ex.value)


def test_plan_table_dir_missing_template_key_raises(tmp_path: Path, meta: RunMetadata):
    with pytest.raises(ValueError):
        plan_table_dir(
            base_out=tmp_path,
            meta=meta,
            module="harmonized",
            trial=meta.trial,
            mode="harmonized_norm",
            fmt="csv",
            filename_base="{trial}_{oops}",
        )
