import json
import pytest
from pathlib import Path
from typing import List
import polars as pl

from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.semantic_mapping.core.exporter import SemanticExporter
from omop_etl.semantic_mapping.core.models import (
    Query,
    QueryResult,
    BatchQueryResult,
    SemanticRow,
    QueryTarget,
    OmopDomain,
)


@pytest.fixture
def run_meta() -> RunMetadata:
    return RunMetadata(
        trial="TEST",
        run_id="abc123",
        started_at="20260105T120000Z",
    )


@pytest.fixture
def semantic_rows() -> List[SemanticRow]:
    return [
        SemanticRow(
            term_id="t1",
            source_col="col1",
            source_term="aml",
            frequency=10,
            omop_concept_id="123",
            omop_concept_code="C123",
            omop_name="acute myeloid leukemia",
            omop_class="disorder",
            omop_concept="S",
            omop_validity="valid",
            omop_domain="condition",
            omop_vocab="SNOMED",
        ),
    ]


@pytest.fixture
def batch_result(semantic_rows) -> BatchQueryResult:
    query_matched = Query(
        patient_id="P1",
        id="q1",
        query="aml",
        field_path=("diagnoses", "term"),
        raw_value="AML",
        leaf_index=0,
        target=QueryTarget(domains=[OmopDomain.CONDITION]),
    )
    query_missing = Query(
        patient_id="P2",
        id="q2",
        query="unknown",
        field_path=("diagnoses", "term"),
        raw_value="Unknown",
        leaf_index=1,
        target=QueryTarget(domains=[OmopDomain.CONDITION]),
    )

    results = (
        QueryResult(patient_id="P1", query=query_matched, results=semantic_rows),
        QueryResult(patient_id="P2", query=query_missing, results=[]),
    )
    return BatchQueryResult(results=results)


class TestSemanticExporter:
    def test_export_json(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=Path("/input/file.json"),
            formats=["json"],
        )

        assert "json" in contexts
        ctx = contexts["json"]

        # Check files exist
        matches_path = ctx.base_dir / "matches_TEST_abc123_20260105T120000Z.json"
        missing_path = ctx.base_dir / "missing_TEST_abc123_20260105T120000Z.json"

        assert matches_path.exists()
        assert missing_path.exists()
        assert ctx.manifest_path.exists()
        assert ctx.log_path.exists()

    def test_export_json_matches_content(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]
        matches_path = ctx.base_dir / "matches_TEST_abc123_20260105T120000Z.json"

        with open(matches_path) as f:
            matches = json.load(f)

        assert isinstance(matches, list)
        assert len(matches) == 1
        assert matches[0]["patient_id"] == "P1"
        assert matches[0]["leaf_index"] == 0

    def test_export_json_missing_content(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]
        missing_path = ctx.base_dir / "missing_TEST_abc123_20260105T120000Z.json"

        with open(missing_path) as f:
            missing = json.load(f)

        assert isinstance(missing, list)
        assert len(missing) == 1
        assert missing[0]["patient_id"] == "P2"
        assert missing[0]["leaf_index"] == 1

    def test_export_csv(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["csv"],
        )

        assert "csv" in contexts
        ctx = contexts["csv"]

        matches_path = ctx.base_dir / "matches_TEST_abc123_20260105T120000Z.csv"
        missing_path = ctx.base_dir / "missing_TEST_abc123_20260105T120000Z.csv"

        assert matches_path.exists()
        assert missing_path.exists()

        # Verify CSV content
        matches_df = pl.read_csv(matches_path)
        assert matches_df.height == 1
        assert "patient_id" in matches_df.columns

    def test_export_tsv(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["tsv"],
        )

        assert "tsv" in contexts
        ctx = contexts["tsv"]

        matches_path = ctx.base_dir / "matches_TEST_abc123_20260105T120000Z.tsv"
        assert matches_path.exists()

        # Verify TSV content (tab-separated)
        matches_df = pl.read_csv(matches_path, separator="\t")
        assert matches_df.height == 1

    def test_export_parquet(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["parquet"],
        )

        assert "parquet" in contexts
        ctx = contexts["parquet"]

        matches_path = ctx.base_dir / "matches_TEST_abc123_20260105T120000Z.parquet"
        assert matches_path.exists()

        # Verify parquet content
        matches_df = pl.read_parquet(matches_path)
        assert matches_df.height == 1
        assert "patient_id" in matches_df.columns

    def test_export_multiple_formats(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json", "csv", "parquet"],
        )

        assert len(contexts) == 3
        assert "json" in contexts
        assert "csv" in contexts
        assert "parquet" in contexts

    def test_export_unsupported_format_raises(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        with pytest.raises(ValueError, match="Unsupported fmt"):
            exporter.export(
                batch_result=batch_result,
                meta=run_meta,
                input_path=None,
                formats=["xlsx"],  # type: ignore
            )


class TestManifest:
    def test_manifest_structure(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=Path("/input/test.json"),
            formats=["json"],
        )

        ctx = contexts["json"]

        with open(ctx.manifest_path) as f:
            manifest = json.load(f)

        assert "trial" in manifest
        assert manifest["trial"] == "TEST"
        assert "run_id" in manifest
        assert manifest["run_id"] == "abc123"

    def test_manifest_has_no_matches_prefix(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]

        # Manifest should NOT have "matches_" prefix
        assert not ctx.manifest_path.name.startswith("matches_")
        assert ctx.manifest_path.name == "TEST_abc123_20260105T120000Z_manifest.json"

    def test_manifest_options_include_file_info(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]

        with open(ctx.manifest_path) as f:
            manifest = json.load(f)

        options = manifest.get("options", {})
        assert "matches_file" in options
        assert "missing_file" in options
        assert "matches_rows" in options
        assert "missing_rows" in options
        assert "total_queries" in options

        assert options["matches_rows"] == 1
        assert options["missing_rows"] == 1
        assert options["total_queries"] == 2

    def test_manifest_includes_coverage_stats(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]

        with open(ctx.manifest_path) as f:
            manifest = json.load(f)

        options = manifest.get("options", {})
        assert "coverage_by_field" in options

        coverage = options["coverage_by_field"]
        assert "diagnoses.term" in coverage

        field_stats = coverage["diagnoses.term"]
        assert field_stats["matched"] == 1
        assert field_stats["missing"] == 1
        assert field_stats["total"] == 2
        assert field_stats["coverage_fraction"] == 0.50


class TestExportDirectoryStructure:
    def test_output_directory_structure(self, tmp_path, batch_result, run_meta):
        exporter = SemanticExporter(base_out=tmp_path)

        contexts = exporter.export(
            batch_result=batch_result,
            meta=run_meta,
            input_path=None,
            formats=["json"],
        )

        ctx = contexts["json"]

        # Verify directory structure matches expected pattern
        # <base>/runs/<started_at>_<run_id>/semantic_mapped/<trial>/semantic_mapped/json/
        relative_path = ctx.base_dir.relative_to(tmp_path)
        parts = relative_path.parts

        assert "runs" in parts
        assert "20260105T120000Z_abc123" in parts
        assert "semantic_mapped" in parts
        assert "test" in parts  # trial lowercased
        assert "json" in parts
