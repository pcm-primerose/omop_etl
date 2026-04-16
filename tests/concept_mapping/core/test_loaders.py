from omop_etl.concept_mapping.core.static_loader import StaticMapLoader
from omop_etl.concept_mapping.core.structural_loader import StructuralMapLoader


class TestStaticMapLoader:
    def test_as_rows_normalizes_case(self, static_csv_file):
        """csv values with uppercase are lowercased and stripped at load time."""
        loader = StaticMapLoader(static_csv_file)

        rows = loader.as_rows()

        assert len(rows) == 2
        assert rows[0].value_set == "sex"
        # csv had "M": loader normalizes to "m"
        assert rows[0].local_value == "m"
        assert rows[0].concept_id == "8507"
        # display fields also normalized
        assert rows[0].concept_name == "male"
        assert rows[0].domain_id == "gender"

    def test_as_index_uses_normalized_keys(self, static_csv_file):
        loader = StaticMapLoader(static_csv_file)

        idx = loader.as_index()

        # csv had ("sex", "M"): indexed under ("sex", "m")
        assert ("sex", "m") in idx
        assert ("sex", "f") in idx
        assert idx[("sex", "m")].concept_name == "male"


class TestStructuralMapLoader:
    def test_as_rows_normalizes_case(self, structural_csv_file):
        loader = StructuralMapLoader(structural_csv_file)

        rows = loader.as_rows()

        assert len(rows) == 1
        assert rows[0].value_set == "ecrf"
        assert rows[0].concept_id == "32817"
        # "Type Concept": "type concept"
        assert rows[0].domain_id == "type concept"

    def test_as_index_uses_normalized_keys(self, structural_csv_file):
        loader = StructuralMapLoader(structural_csv_file)

        idx = loader.as_index()

        assert "ecrf" in idx
        assert idx["ecrf"].concept_name == "ehr encounter record"
