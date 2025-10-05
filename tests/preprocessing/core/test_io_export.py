import json
import os
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from omop_etl.preprocessing.core.io_export import PreprocessExporter
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import Layout, TabularFormat
from omop_etl.infra.io.path_planner import plan_paths, WriterContext
from omop_etl.infra.io.format_utils import expand_formats, normalize_format


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "SubjectId": ["A_001", "A_002", "A_003"],
            "age": [25, 30, 35],
            "sex": ["M", "F", "M"],
        },
    )


@pytest.fixture
def run_context() -> RunMetadata:
    # matches your synthetic IDs used in earlier tests
    return RunMetadata(trial="test_trial", started_at="20231201_143000", run_id="abc123")


@pytest.fixture
def exporter(tmp_path: Path) -> PreprocessExporter:
    return PreprocessExporter(base_out=tmp_path / "outputs", layout=Layout.TRIAL_RUN)


class TestPlanPaths:
    """Plan paths via the new path planner (dir roots, not files)."""

    def test_plan_paths_structured_dir(self, run_context: RunMetadata, tmp_path: Path):
        ctx: WriterContext = plan_paths(
            base_out=tmp_path,
            module="preprocessing",
            trial=run_context.trial,
            run_id=run_context.run_id,
            stem="preprocessed",
            fmt="csv",
            layout=Layout.TRIAL_RUN,
            started_at=run_context.started_at,
            include_stem_dir=True,
            name_template=None,
        )

        # layout: base/module/trial/run-seg/stem/fmt/*
        assert ctx.base_dir == (tmp_path / "preprocessing" / "test_trial" / run_context.run_id / "preprocessed" / "csv")
        assert ctx.data_path.name == "data_preprocessed.csv"
        assert ctx.manifest_path.name == "manifest_preprocessed.json"
        assert ctx.log_path.name == "preprocessed.log"

    def test_plan_paths_templated_name(self, run_context: RunMetadata, tmp_path: Path):
        ctx = plan_paths(
            base_out=tmp_path,
            module="preprocessing",
            trial=run_context.trial,
            run_id=run_context.run_id,
            stem="preprocessed",
            fmt="parquet",
            layout=Layout.TRIAL_RUN,
            started_at=run_context.started_at,
            include_stem_dir=False,
            name_template="{trial}_{run_id}_{started_at}_{mode}",
            template_vars=None,
        )
        # layout: base/module/trial/run-seg/fmt/*
        assert ctx.base_dir == (tmp_path / "preprocessing" / "test_trial" / run_context.run_id / "parquet")
        # templated names
        prefix = f"{run_context.trial}_{run_context.run_id}_{run_context.started_at}_preprocessed"
        assert ctx.data_path.name == f"{prefix}.parquet"
        assert ctx.manifest_path.name == f"{prefix}_manifest.json"
        assert ctx.log_path.name == f"{prefix}.log"


class TestExportWide:
    """End-to-end write via PreprocessExporter.export_wide (public API)."""

    def test_write_csv(self, exporter: PreprocessExporter, run_context: RunMetadata, sample_dataframe: pl.DataFrame, tmp_path: Path):
        input_path = tmp_path / "input.csv"
        input_path.write_text("dummy\n")

        out = exporter.export_wide(
            df=sample_dataframe,
            meta=run_context,
            input_path=input_path,
            formats=["csv"],
        )
        ctx = out["csv"]
        assert ctx.data_path.exists()
        df_read = pl.read_csv(ctx.data_path)
        assert tuple(df_read.columns) == tuple(sample_dataframe.columns)
        assert df_read.shape == sample_dataframe.shape

        manifest = json.loads(ctx.manifest_path.read_text())
        assert manifest["trial"] == run_context.trial
        assert manifest["started_at"] == run_context.started_at
        assert manifest["run_id"] == run_context.run_id
        # the manifest builder uses "fmt"
        assert manifest["fmt"] == "csv"
        assert "statistics" in manifest and "rows" in manifest["statistics"] and "columns" in manifest["statistics"]

    def test_write_parquet(self, exporter: PreprocessExporter, run_context: RunMetadata, sample_dataframe: pl.DataFrame, tmp_path: Path):
        input_path = tmp_path / "input.csv"
        input_path.write_text("dummy\n")

        out = exporter.export_wide(
            df=sample_dataframe,
            meta=run_context,
            input_path=input_path,
            formats=["parquet"],
        )
        ctx = out["parquet"]
        assert ctx.data_path.suffix == ".parquet"
        df_read = pl.read_parquet(ctx.data_path)
        assert df_read.shape == sample_dataframe.shape

    def test_write_tsv(self, exporter: PreprocessExporter, run_context: RunMetadata, sample_dataframe: pl.DataFrame, tmp_path: Path):
        input_path = tmp_path / "input.csv"
        input_path.write_text("dummy\n")

        out = exporter.export_wide(
            df=sample_dataframe,
            meta=run_context,
            input_path=input_path,
            formats=["tsv"],
        )
        ctx = out["tsv"]
        assert ctx.data_path.suffix == ".tsv"
        df_read = pl.read_csv(ctx.data_path, separator="\t")
        assert df_read.shape == sample_dataframe.shape


class TestFormatUtils:
    """Format inference/normalization uses format_utils now (public helpers)."""

    def test_expand_formats(self):
        assert expand_formats("csv") == ["csv"]
        assert set(expand_formats("all")) >= {"csv", "tsv", "parquet"}  # tabular + maybe json depending on allowed
        assert expand_formats(["CSV", "parquet"]) == ["csv", "parquet"]
        # todo: fix: Expected type 'str | Sequence[str] | None', got 'list[list[str]]' instead
        assert expand_formats([["csv"], ["parquet"]]) == ["csv", "parquet"]

    def test_normalize_format(self):
        assert normalize_format("CSV") == "csv"
        assert normalize_format("txt") == "tsv"
        assert normalize_format(None) == "csv"
        with pytest.raises(ValueError):
            normalize_format("xml")


class TestEndToEndDisabledLog:
    """Replicate the old 'disable log file' flow with env var guard."""

    def test_write_with_log_file_disabled(
        self,
        exporter: PreprocessExporter,
        run_context: RunMetadata,
        sample_dataframe: pl.DataFrame,
        tmp_path: Path,
    ):
        input_path = tmp_path / "input.csv"
        input_path.write_text("dummy\n")

        # simulate your old env toggle; exporter respects ctx.log_path regardless,
        # but your global configure/add_file_handler may check this variable.
        with patch.dict(os.environ, {"DISABLE_LOG_FILE": "1"}):
            out = exporter.export_wide(
                df=sample_dataframe,
                meta=run_context,
                input_path=input_path,
                formats=["csv"],
            )
        ctx = out["csv"]
        # manifest should still exist; log might be empty or absent depending on your logger setup.
        assert ctx.manifest_path.exists()
        # don't assert log file content here; logging is configured at process level.


# tests for manifest schema if your manifest_builder includes it
@pytest.mark.parametrize("fmt", ["csv", "tsv", "parquet"])
def test_manifest_schema_columns(
    exporter: PreprocessExporter,
    run_context: RunMetadata,
    sample_dataframe: pl.DataFrame,
    tmp_path: Path,
    fmt: TabularFormat,
):
    input_path = tmp_path / "input.csv"
    input_path.write_text("dummy\n")
    out = exporter.export_wide(df=sample_dataframe, meta=run_context, input_path=input_path, formats=[fmt])
    ctx = out[fmt]
    manifest = json.loads(ctx.manifest_path.read_text())
    schema = manifest.get("schema") or {}
    # If your manifest builder encodes schema mapping
    assert {"SubjectId", "age", "sex"}.issubset(schema.keys())
