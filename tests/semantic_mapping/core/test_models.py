import pytest
from typing import List

from omop_etl.semantic_mapping.core.models import (
    Query,
    QueryResult,
    BatchQueryResult,
    SemanticRow,
    QueryTarget,
    OmopDomain,
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
        SemanticRow(
            term_id="t2",
            source_col="col1",
            source_term="aml",
            frequency=5,
            omop_concept_id="456",
            omop_concept_code="C456",
            omop_name="aml variant",
            omop_class="disorder",
            omop_concept="S",
            omop_validity="valid",
            omop_domain="condition",
            omop_vocab="ICD10",
        ),
    ]


@pytest.fixture
def sample_queries() -> List[Query]:
    return [
        Query(
            patient_id="P1",
            id="q1",
            query="aml",
            field_path=("diagnoses", "term"),
            raw_value="AML",
            leaf_index=0,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
        Query(
            patient_id="P1",
            id="q2",
            query="diabetes",
            field_path=("diagnoses", "term"),
            raw_value="Diabetes",
            leaf_index=1,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
        Query(
            patient_id="P2",
            id="q3",
            query="aspirin",
            field_path=("medications", "name"),
            raw_value="Aspirin",
            leaf_index=0,
            target=QueryTarget(domains=[OmopDomain.DRUG]),
        ),
        Query(
            patient_id="P2",
            id="q4",
            query="unknown_med",
            field_path=("medications", "name"),
            raw_value="Unknown Med",
            leaf_index=1,
            target=QueryTarget(domains=[OmopDomain.DRUG]),
        ),
    ]


@pytest.fixture
def batch_result(sample_queries, semantic_rows) -> BatchQueryResult:
    # q1 and q3 have matches, q2 and q4 are missing
    results = (
        QueryResult(patient_id="P1", query=sample_queries[0], results=semantic_rows),
        QueryResult(patient_id="P1", query=sample_queries[1], results=[]),  # missing
        QueryResult(patient_id="P2", query=sample_queries[2], results=[semantic_rows[0]]),
        QueryResult(patient_id="P2", query=sample_queries[3], results=[]),  # missing
    )
    return BatchQueryResult(results=results)


class TestBatchQueryResult:
    def test_matches_returns_only_matched_results(self, batch_result):
        matches = batch_result.matches
        assert len(matches) == 2
        assert all(len(m.results) > 0 for m in matches)

    def test_missing_returns_only_unmatched_queries(self, batch_result):
        missing = batch_result.missing
        assert len(missing) == 2
        assert all(isinstance(m, Query) for m in missing)
        missing_ids = {m.id for m in missing}
        assert missing_ids == {"q2", "q4"}

    def test_to_matches_df_structure(self, batch_result):
        df = batch_result.to_matches_df()

        expected_columns = {
            "patient_id",
            "query_id",
            "query",
            "field_path",
            "raw_value",
            "leaf_index",
            "term_id",
            "source_col",
            "source_term",
            "frequency",
            "omop_concept_id",
            "omop_concept_code",
            "omop_name",
            "omop_class",
            "omop_concept",
            "omop_validity",
            "omop_domain",
            "omop_vocab",
        }
        assert set(df.columns) == expected_columns

    def test_to_matches_df_row_count(self, batch_result):
        df = batch_result.to_matches_df()
        # q1 has 2 semantic rows, q3 has 1 semantic row = 3 total rows
        assert df.height == 3

    def test_to_matches_df_field_path_joined(self, batch_result):
        df = batch_result.to_matches_df()
        field_paths = df["field_path"].to_list()
        assert "diagnoses.term" in field_paths
        assert "medications.name" in field_paths

    def test_to_matches_df_preserves_leaf_index(self, batch_result):
        df = batch_result.to_matches_df()
        leaf_indices = df["leaf_index"].to_list()
        assert 0 in leaf_indices
        assert 1 not in leaf_indices  # only q1 (idx=0) and q3 (idx=0) matched

    def test_to_missing_df_structure(self, batch_result):
        df = batch_result.to_missing_df()

        expected_columns = {"patient_id", "query_id", "query", "field_path", "raw_value", "leaf_index"}
        assert set(df.columns) == expected_columns

    def test_to_missing_df_row_count(self, batch_result):
        df = batch_result.to_missing_df()
        assert df.height == 2

    def test_to_missing_df_preserves_leaf_index(self, batch_result):
        df = batch_result.to_missing_df()
        leaf_indices = df["leaf_index"].to_list()
        assert 1 in leaf_indices  # q2 and q4 both have leaf_index=1

    def test_to_matches_dict_returns_list(self, batch_result):
        result = batch_result.to_matches_dict()
        assert isinstance(result, list)
        assert len(result) == 3  # same as df row count

    def test_to_matches_dict_structure(self, batch_result):
        result = batch_result.to_matches_dict()
        first_row = result[0]

        assert "patient_id" in first_row
        assert "query_id" in first_row
        assert "leaf_index" in first_row
        assert "omop_concept_id" in first_row

    def test_to_missing_dict_returns_list(self, batch_result):
        result = batch_result.to_missing_dict()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_to_missing_dict_includes_patient_id(self, batch_result):
        result = batch_result.to_missing_dict()
        assert all("patient_id" in row for row in result)

    def test_to_missing_dict_preserves_all_queries(self, batch_result):
        result = batch_result.to_missing_dict()
        query_ids = {row["query_id"] for row in result}
        assert query_ids == {"q2", "q4"}


class TestCoverageByFieldPath:
    def test_coverage_by_field_path_structure(self, batch_result):
        coverage = batch_result.coverage_by_field_path()

        assert isinstance(coverage, dict)
        assert "diagnoses.term" in coverage
        assert "medications.name" in coverage

    def test_coverage_by_field_path_stats(self, batch_result):
        coverage = batch_result.coverage_by_field_path()

        diagnoses_stats = coverage["diagnoses.term"]
        assert diagnoses_stats["matched"] == 1
        assert diagnoses_stats["missing"] == 1
        assert diagnoses_stats["total"] == 2
        assert diagnoses_stats["coverage_fraction"] == 0.50

    def test_coverage_by_field_path_medications(self, batch_result):
        coverage = batch_result.coverage_by_field_path()

        meds_stats = coverage["medications.name"]
        assert meds_stats["matched"] == 1
        assert meds_stats["missing"] == 1
        assert meds_stats["total"] == 2
        assert meds_stats["coverage_fraction"] == 0.50

    def test_coverage_full_match(self):
        query = Query(
            patient_id="P1",
            id="q1",
            query="test",
            field_path=("field",),
            raw_value="Test",
        )
        semantic_row = SemanticRow(
            term_id="t1",
            source_col="c",
            source_term="test",
            frequency=1,
            omop_concept_id="1",
            omop_concept_code="C1",
            omop_name="test",
            omop_class="cls",
            omop_concept="S",
            omop_validity="valid",
            omop_domain="condition",
            omop_vocab="SNOMED",
        )
        batch = BatchQueryResult(results=(QueryResult(patient_id="P1", query=query, results=[semantic_row]),))

        coverage = batch.coverage_by_field_path()
        assert coverage["field"]["coverage_fraction"] == 1.0

    def test_coverage_no_matches(self):
        query = Query(
            patient_id="P1",
            id="q1",
            query="test",
            field_path=("field",),
            raw_value="Test",
        )
        batch = BatchQueryResult(results=(QueryResult(patient_id="P1", query=query, results=[]),))

        coverage = batch.coverage_by_field_path()
        assert coverage["field"]["coverage_fraction"] == 0.0


class TestSemanticRow:
    def test_from_csv_row(self):
        row = {
            "term_id": "T1",
            "source_col": "COL1",
            "source_term": "AML",
            "frequency": "10",
            "omop_concept_id": "123",
            "omop_concept_code": "C123",
            "omop_name": "Acute Myeloid Leukemia",
            "omop_class": "Disorder",
            "omop_concept": "S",
            "omop_validity": "Valid",
            "omop_domain": "Condition",
            "omop_vocab": "SNOMED",
        }

        semantic_row = SemanticRow.from_csv_row(row)

        # values should be lowercased and stripped
        assert semantic_row.term_id == "t1"
        assert semantic_row.source_term == "aml"
        assert semantic_row.omop_name == "acute myeloid leukemia"
        assert semantic_row.frequency == 10


class TestQueryTarget:
    def test_domains_converted_to_frozenset(self):
        target = QueryTarget(domains=[OmopDomain.CONDITION, OmopDomain.DRUG])
        assert isinstance(target.domains, frozenset)

    def test_none_domains_stays_none(self):
        target = QueryTarget(domains=None)
        assert target.domains is None

    def test_frozenset_unchanged(self):
        fs = frozenset([OmopDomain.CONDITION])
        target = QueryTarget(domains=fs)
        assert target.domains is fs
