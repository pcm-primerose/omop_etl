import logging

import pytest
import polars as pl

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
        assert rows[0].source_value == "m"
        assert rows[0].concept_id == 8507
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
        assert rows[0].concept_id == 32817
        # "Type Concept": "type concept"
        assert rows[0].domain_id == "type concept"

    def test_as_index_uses_normalized_keys(self, structural_csv_file):
        loader = StructuralMapLoader(structural_csv_file)

        idx = loader.as_index()

        assert "ecrf" in idx
        assert idx["ecrf"].concept_name == "ehr encounter record"


class TestMalformedCsvRows:
    def test_static_loader_warns_on_missing_columns(self, tmp_path, caplog):
        """A row with fewer values than columns logs a warning with file + line context."""
        # deliberately malformed (9 values, 10 columns)
        csv = tmp_path / "bad.csv"
        csv.write_text(
            "value_set,source_value,concept_id,concept_code,concept_name,"
            "concept_class_id,standard_concept,validity,domain_id,vocabulary_id\n"
            "sex,m,8507,M,MALE,Gender,Standard,,Gender\n"
        )

        with caplog.at_level(logging.WARNING):
            rows = StaticMapLoader(csv).as_rows()

        assert len(rows) == 1
        assert rows[0].vocabulary_id == ""  # None to empty string via _norm
        assert "Malformed row" in caplog.text
        assert "vocabulary_id" in caplog.text
        assert "line 2" in caplog.text

    def test_static_loader_does_not_crash_on_none_values(self, tmp_path):
        """Malformed rows produce empty-string fields, not crashes."""
        csv = tmp_path / "bad.csv"
        csv.write_text(
            "value_set,source_value,concept_id,concept_code,concept_name,"
            "concept_class_id,standard_concept,validity,domain_id,vocabulary_id\n"
            "sex,m,8507,M,MALE,Gender,Standard,,Gender\n"
        )

        _ = StaticMapLoader(csv).as_rows()
        idx = StaticMapLoader(csv).as_index()

        assert ("sex", "m") in idx
        assert idx[("sex", "m")].vocabulary_id == ""


class TestDuplicateCuratedKeys:
    """
    A curated source value must resolve to one concept, conflicting
    duplicate keys are a curation error caught at load.
    """

    def test_static_conflicting_duplicate_raises(self, tmp_path):
        # same (value_set, source_value): two different concept_ids
        csv_path = tmp_path / "dup.csv"
        pl.DataFrame(
            data={
                "value_set": ["sex", "sex"],
                "source_value": ["m", "m"],
                "concept_id": [8507, 9999],
                "concept_code": ["M", "M"],
                "concept_name": ["Male", "Male"],
                "concept_class_id": ["Gender", "Gender"],
                "standard_concept": ["Standard", "Standard"],
                "validity": ["", ""],
                "domain_id": ["Gender", "Gender"],
                "vocabulary_id": ["Gender", "Gender"],
            }
        ).write_csv(csv_path)

        with pytest.raises(ValueError, match="Duplicate static mapping"):
            StaticMapLoader(csv_path).as_index()

    def test_static_exact_duplicate_allowed(self, tmp_path):
        # exact-duplicate row (same key, same concept_id) is harmless, not an error
        csv_path = tmp_path / "dup.csv"
        pl.DataFrame(
            data={
                "value_set": ["sex", "sex"],
                "source_value": ["m", "m"],
                "concept_id": [8507, 8507],
                "concept_code": ["M", "M"],
                "concept_name": ["Male", "Male"],
                "concept_class_id": ["Gender", "Gender"],
                "standard_concept": ["Standard", "Standard"],
                "validity": ["", ""],
                "domain_id": ["Gender", "Gender"],
                "vocabulary_id": ["Gender", "Gender"],
            }
        ).write_csv(csv_path)

        idx = StaticMapLoader(csv_path).as_index()
        assert idx[("sex", "m")].concept_id == 8507

    def test_structural_conflicting_duplicate_raises(self, tmp_path):
        csv_path = tmp_path / "dup.csv"
        pl.DataFrame(
            data={
                "value_set": ["ecrf", "ecrf"],
                "concept_id": [32817, 99999],
                "concept_code": ["OMOP4822053", "OMOP4822053"],
                "concept_name": ["EHR encounter record", "EHR encounter record"],
                "concept_class_id": ["Obs Type", "Obs Type"],
                "standard_concept": ["Standard", "Standard"],
                "validity": ["", ""],
                "domain_id": ["Type Concept", "Type Concept"],
                "vocabulary_id": ["Type Concept", "Type Concept"],
            }
        ).write_csv(csv_path)

        with pytest.raises(ValueError, match="Duplicate structural mapping"):
            StructuralMapLoader(csv_path).as_index()
