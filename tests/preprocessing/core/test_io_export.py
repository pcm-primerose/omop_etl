import json
from pathlib import Path
import polars as pl
import pytest

from omop_etl.preprocessing.core.exporter import PreprocessExporter
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.io.types import Layout
from omop_etl.infra.io.types import (
    TabularFormat,
    WIDE_FORMATS,
    TABULAR_FORMATS,
)
from omop_etl.infra.io.format_utils import (
    expand_formats,
    normalize_format,
)
from omop_etl.infra.io.path_planner import (
    plan_table_dir,
    plan_single_file,
    WriterContext,
)


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
    return RunMetadata(trial="test_trial", started_at="20231201_143000", run_id="abc123")


@pytest.fixture
def exporter(tmp_path: Path) -> PreprocessExporter:
    return PreprocessExporter(base_out=tmp_path / "outputs", layout=Layout.TRIAL_RUN)


class TestPlanPaths:
    def test_plan_table_dir_structured(self, run_context, tmp_path):
        ctx: WriterContext = plan_table_dir(
            base_out=tmp_path, meta=run_context, module="preprocessed", trial=run_context.trial, fmt="csv", mode="preprocessed"
        )

        seg = f"{run_context.started_at}_{run_context.run_id}"
        assert ctx.base_dir == (tmp_path / "runs" / seg / "preprocessed" / "test_trial" / "preprocessed" / "csv")

        prefix = f"{run_context.trial}_{run_context.run_id}_{run_context.started_at}_preprocessed"
        assert ctx.data_path.name == f"{prefix}.csv"
        assert ctx.manifest_path.name == f"{prefix}_manifest.json"
        assert ctx.log_path.name == f"{prefix}.log"

    def test_plan_single_file_templated(self, run_context, tmp_path):
        ctx: WriterContext = plan_single_file(
            base_out=tmp_path, meta=run_context, module="preprocessed", trial=run_context.trial, fmt="parquet", mode="preprocessed"
        )

        seg = f"{run_context.started_at}_{run_context.run_id}"
        assert ctx.base_dir == (tmp_path / "runs" / seg / "preprocessed" / "test_trial" / "preprocessed" / "parquet")
        prefix = f"{run_context.trial}_{run_context.run_id}_{run_context.started_at}_preprocessed"
        assert ctx.data_path.name == f"{prefix}.parquet"
        assert ctx.manifest_path.name == f"{prefix}_manifest.json"
        assert ctx.log_path.name == f"{prefix}.log"


class TestExportWide:
    """End-to-end write via PreprocessExporter.export_wide."""

    def test_write_csv(
        self,
        exporter: PreprocessExporter,
        run_context: RunMetadata,
        sample_dataframe: pl.DataFrame,
        tmp_path: Path,
    ):
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
        assert manifest["format"] == "csv"
        assert "tables" in manifest and "wide" in manifest["tables"]
        wide_meta = manifest["tables"]["wide"]
        assert wide_meta["rows"] == sample_dataframe.height
        assert wide_meta["cols"] == sample_dataframe.width
        assert set(wide_meta["schema"].keys()) == set(sample_dataframe.columns)

    def test_write_parquet(
        self,
        exporter: PreprocessExporter,
        run_context: RunMetadata,
        sample_dataframe: pl.DataFrame,
        tmp_path: Path,
    ):
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

    def test_write_tsv(
        self,
        exporter: PreprocessExporter,
        run_context: RunMetadata,
        sample_dataframe: pl.DataFrame,
        tmp_path: Path,
    ):
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
    def test_expand_formats(self):
        assert expand_formats("csv", allowed=WIDE_FORMATS) == ["csv"]
        assert set(expand_formats("all", allowed=TABULAR_FORMATS)) == set(TABULAR_FORMATS)
        assert expand_formats(["CSV", "parquet"], allowed=WIDE_FORMATS) == ["csv", "parquet"]  # type: ignore
        assert expand_formats([["csv"], ["parquet"]], allowed=TABULAR_FORMATS) == ["csv", "parquet"]  # type: ignore

    def test_normalize_format(self):
        assert normalize_format("CSV", allowed=WIDE_FORMATS) == "csv"  # type: ignore
        assert normalize_format("txt", allowed=WIDE_FORMATS) == "tsv"  # type: ignore
        assert normalize_format("csv", allowed=WIDE_FORMATS) == "csv"
        with pytest.raises(ValueError):
            normalize_format("xml", allowed=WIDE_FORMATS)  # type: ignore


class TestEndToEndDisabledLog:
    def test_write_with_log_file_disabled(
        self,
        exporter: PreprocessExporter,
        run_context: RunMetadata,
        sample_dataframe: pl.DataFrame,
        tmp_path: Path,
    ):
        input_path = tmp_path / "input.csv"
        input_path.write_text("dummy\n")

        out = exporter.export_wide(
            df=sample_dataframe,
            meta=run_context,
            input_path=input_path,
            formats=["csv"],
        )
        ctx = out["csv"]
        assert ctx.manifest_path.exists()


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

    # schema is per-table in manifest
    wide = manifest["tables"]["wide"]
    schema = wide["schema"]
    assert {"SubjectId", "age", "sex"}.issubset(schema.keys())
