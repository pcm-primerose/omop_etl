from omop_etl.concept_mapping.core.static_loader import StaticMapLoader
from omop_etl.concept_mapping.core.structural_loader import StructuralMapLoader


class TestStaticMapLoader:
    def test_as_rows(self, static_csv_file):
        loader = StaticMapLoader(static_csv_file)

        rows = loader.as_rows()

        assert len(rows) == 2
        assert rows[0].value_set == "sex"
        assert rows[0].local_value == "M"
        assert rows[0].concept_id == "8507"

    def test_as_index(self, static_csv_file):
        loader = StaticMapLoader(static_csv_file)

        idx = loader.as_index()

        assert ("sex", "M") in idx
        assert ("sex", "F") in idx
        assert idx[("sex", "M")].concept_name == "Male"


class TestStructuralMapLoader:
    def test_as_rows(self, structural_csv_file):
        loader = StructuralMapLoader(structural_csv_file)

        rows = loader.as_rows()

        assert len(rows) == 1
        assert rows[0].value_set == "ecrf"
        assert rows[0].concept_id == "32817"

    def test_as_index(self, structural_csv_file):
        loader = StructuralMapLoader(structural_csv_file)

        idx = loader.as_index()

        assert "ecrf" in idx
        assert idx["ecrf"].concept_name == "EHR encounter record"
