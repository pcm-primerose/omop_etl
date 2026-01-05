import pytest

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
        aml_row = next(r for r in rows if "aml" in r.source_term.lower())
        assert aml_row.source_term == "aml"

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
        csv_content = """\
term_id,source_col,source_term,frequency,omop_concept_id,omop_concept_code,omop_name,omop_class,omop_concept,omop_validity,omop_domain,omop_vocab
t1,col,cancer,1,100,C100,cancer type 1,cls,S,Valid,condition,SNOMED
t2,col,cancer,2,200,C200,cancer type 2,cls,S,Valid,condition,ICD10
t3,col,other,1,300,C300,other thing,cls,S,Valid,condition,SNOMED
"""

        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

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
                term_id="t1",
                source_col="c",
                source_term="Term A",
                frequency=1,
                omop_concept_id="1",
                omop_concept_code="C1",
                omop_name="name1",
                omop_class="cls",
                omop_concept="S",
                omop_validity="valid",
                omop_domain="condition",
                omop_vocab="SNOMED",
            ),
            SemanticRow(
                term_id="t2",
                source_col="c",
                source_term="term a",
                frequency=2,
                omop_concept_id="2",
                omop_concept_code="C2",
                omop_name="name2",
                omop_class="cls",
                omop_concept="S",
                omop_validity="valid",
                omop_domain="condition",
                omop_vocab="ICD10",
            ),
        ]

        indexed = LoadSemantics._index(rows)

        # both should be grouped under lowercased key
        assert "term a" in indexed
        assert len(indexed["term a"]) == 2


class TestLoadSemanticsDefaultPath:
    def test_init_without_path_uses_default(self):
        # verifies loader can be initialized without a path
        # should use default resource path
        try:
            loader = LoadSemantics(path=None)
            assert loader.path is not None
        except (FileNotFoundError, ValueError):
            # expected if no default semantic file exists in resources
            pytest.skip("No default semantic mapping file in resources")
