import json

from omop_etl.concept_mapping.core.exporter import ConceptLookupExporter
from omop_etl.concept_mapping.core.models import (
    LookupResult,
    MappedConcept,
)


class TestConceptLookupExporter:
    def test_export_csv(self, tmp_path, run_meta):
        exporter = ConceptLookupExporter(base_out=tmp_path)
        result = LookupResult()
        result.record_miss("static", "sex", "X")
        result.record_match("static", "sex", "M", MappedConcept("8507", "M", "Male", "Gender", "Gender", "Valid"))

        ctx = exporter.export(result, run_meta, formats=["csv"])

        assert "csv" in ctx
        assert ctx["csv"].base_dir.exists()
        assert ctx["csv"].manifest_path.exists()

    def test_export_json(self, tmp_path, run_meta):
        exporter = ConceptLookupExporter(base_out=tmp_path)
        result = LookupResult()
        result.record_miss("static", "sex", "X")

        ctx = exporter.export(result, run_meta, formats=["json"])

        assert "json" in ctx
        missed_file = list(ctx["json"].base_dir.glob("missed_*.json"))[0]
        data = json.loads(missed_file.read_text())
        assert len(data) == 1
        assert data[0]["value_set"] == "sex"

    def test_manifest_contains_coverage(self, tmp_path, run_meta):
        exporter = ConceptLookupExporter(base_out=tmp_path)
        result = LookupResult()
        result.record_match("static", "sex", "M", MappedConcept("8507", "M", "Male", "Gender", "Gender", "Valid"))
        result.record_miss("static", "sex", "X")

        ctx = exporter.export(result, run_meta, formats=["csv"])

        manifest = json.loads(ctx["csv"].manifest_path.read_text())
        assert "coverage_by_field" in manifest["options"]
        assert "sex" in manifest["options"]["coverage_by_field"]
