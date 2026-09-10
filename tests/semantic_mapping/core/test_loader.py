import pytest
import polars as pl

from omop_etl.semantic_mapping.core.loader import LoadSemantics
from omop_etl.semantic_mapping.core.models import SemanticRow


class TestLoadSemantics:
    def test_as_rows_returns_list_of_semantic_rows(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        rows = loader.as_rows()

        assert isinstance(rows, list)
        assert all(isinstance(row, SemanticRow) for row in rows)

    def test_as_rows_count_matches_csv(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        rows = loader.as_rows()

        # semantic_data fixture has 3 rows
        assert len(rows) == 3

    def test_as_rows_values_lowercased(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        rows = loader.as_rows()

        # AML in source should be lowercased
        aml_row = next(r for r in rows if "aml" in r.source_value.lower())
        assert aml_row.source_value == "aml"

    def test_as_indexed_returns_dict(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        indexed = loader.as_indexed()

        assert isinstance(indexed, dict)

    def test_as_indexed_keyed_by_source_term(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        indexed = loader.as_indexed()

        # source_terms from fixture: "something", "AML", "gsv sleeper service"
        # all lowercased
        assert "aml" in indexed
        assert "something" in indexed
        assert "gsv sleeper service" in indexed

    def test_as_indexed_values_are_lists(self, semantic_file):
        loader = LoadSemantics(semantic_file)
        indexed = loader.as_indexed()

        for key, value in indexed.items():
            assert isinstance(value, list)
            assert all(isinstance(row, SemanticRow) for row in value)

    def test_index_groups_duplicate_terms(self, tmp_path):
        # CSV with duplicate source_terms
        df = pl.DataFrame(
            data={
                "source_value": ["cancer", "cancer", "other"],
                "concept_id": [100, 200, 300],
                "concept_code": ["C100", "C200", "C300"],
                "concept_name": ["cancer type 1", "cancer type 2", "other thing"],
                "concept_class_id": ["cls", "cls", "cls"],
                "standard_concept": ["S", "S", "S"],
                "validity": ["", "", ""],
                "domain_id": ["condition", "condition", "condition"],
                "vocabulary_id": ["SNOMED", "ICD10", "SNOMED"],
            }
        )
        csv_path = tmp_path / "test.csv"
        df.write_csv(csv_path)

        loader = LoadSemantics(csv_path)
        indexed = loader.as_indexed()

        assert len(indexed["cancer"]) == 2
        assert len(indexed["other"]) == 1

    def test_as_lazyframe_not_implemented(self, semantic_file):
        loader = LoadSemantics(semantic_file)

        with pytest.raises(NotImplementedError):
            loader.as_lazyframe()

    def test_index_static_method(self):
        rows = [
            SemanticRow(
                source_value="Term A",
                concept_id="1",
                concept_code="C1",
                concept_name="name1",
                concept_class_id="cls",
                standard_concept="S",
                validity="valid",
                domain_id="condition",
                vocabulary_id="SNOMED",
            ),
            SemanticRow(
                source_value="term a",
                concept_id="2",
                concept_code="C2",
                concept_name="name2",
                concept_class_id="cls",
                standard_concept="S",
                validity="valid",
                domain_id="condition",
                vocabulary_id="ICD10",
            ),
        ]

        indexed = LoadSemantics._index(rows)

        # both should be grouped under lowercased key
        assert "term a" in indexed
        assert len(indexed["term a"]) == 2

    def test_index_deduplicates_same_concept_id(self):
        """Case-normalization can make two source terms collapse (e.g. OxyNorm/Oxynorm).
        If they map to the same concept_id, _index should keep only one."""
        rows = [
            SemanticRow(
                source_value="OxyNorm",
                concept_id="1124957",
                concept_code="7804",
                concept_name="oxycodone",
                concept_class_id="ingredient",
                standard_concept="standard",
                validity="valid",
                domain_id="drug",
                vocabulary_id="rxnorm",
            ),
            SemanticRow(
                source_value="oxynorm",
                concept_id="1124957",
                concept_code="7804",
                concept_name="oxycodone",
                concept_class_id="ingredient",
                standard_concept="standard",
                validity="valid",
                domain_id="drug",
                vocabulary_id="rxnorm",
            ),
        ]

        indexed = LoadSemantics._index(rows)

        assert len(indexed["oxynorm"]) == 1
        assert indexed["oxynorm"][0].concept_id == "1124957"

    def test_index_keeps_different_concept_ids(self):
        """Multi-ingredient drugs (e.g. Calcigran Forte) should keep all concepts."""
        rows = [
            SemanticRow(
                source_value="calcigran forte",
                concept_id="19009405",
                concept_code="11253",
                concept_name="vitamin d",
                concept_class_id="ingredient",
                standard_concept="standard",
                validity="valid",
                domain_id="drug",
                vocabulary_id="rxnorm",
            ),
            SemanticRow(
                source_value="calcigran forte",
                concept_id="19035704",
                concept_code="1897",
                concept_name="calcium carbonate",
                concept_class_id="ingredient",
                standard_concept="standard",
                validity="valid",
                domain_id="drug",
                vocabulary_id="rxnorm",
            ),
        ]

        indexed = LoadSemantics._index(rows)

        assert len(indexed["calcigran forte"]) == 2
        concept_ids = {r.concept_id for r in indexed["calcigran forte"]}
        assert concept_ids == {"19009405", "19035704"}
