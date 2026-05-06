import pytest

from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.semantic_mapping.core.models import (
    Query,
    QueryResult,
    BatchQueryResult,
    SemanticRow,
)


class TestConceptLookupService:
    def test_lookup_static_hit(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M")

        assert result is not None
        assert result.concept_id == 8507
        assert result.concept_name == "Male"

    def test_lookup_static_miss(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "X")

        assert result is None
        assert len(service.result.missed["static"]) == 1

    def test_lookup_structural_hit(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.lookup_structural("ecrf")

        assert result is not None
        assert result.concept_id == 32817

    def test_lookup_structural_miss(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.lookup_structural("unknown")

        assert result is None
        assert len(service.result.missed["structural"]) == 1

    def test_result_accumulates(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        service.lookup_static("sex", "M")
        service.lookup_static("sex", "F")
        service.lookup_static("sex", "X")

        assert len(service.result.matched["static"]) == 2
        assert len(service.result.missed["static"]) == 1

    def test_reset_clears_results(self, static_index):
        service = ConceptLookupService(static_index=static_index)
        service.lookup_static("sex", "M")

        service.reset()

        assert len(service.result.matched["static"]) == 0


class TestCaseInsensitiveLookups:
    """Lookup keys are case-normalized and stripped"""

    def test_lookup_static_matches_uppercase_input(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        # fixture indexes under ("sex", "m"): caller passes "M"
        result = service.lookup_static("sex", "M")

        assert result is not None
        assert result.concept_id == 8507

    def test_lookup_static_matches_titlecase_value_set(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        # value_set case-insensitive too
        result = service.lookup_static("SEX", "m")

        assert result is not None

    def test_lookup_static_strips_whitespace(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("  sex  ", "  M  ")

        assert result is not None

    def test_lookup_structural_matches_uppercase_input(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.lookup_structural("ECRF")

        assert result is not None
        assert result.concept_id == 32817

    def test_domain_filter_is_case_insensitive(self, static_index):
        """Filter accepts mixed-case domain strings and still matches."""
        service = ConceptLookupService(static_index=static_index)

        # fixture domain_id is "Gender": filter passes "gender" / "GENDER" / "Gender"
        for domain_filter in ("gender", "GENDER", "Gender"):
            result = service.lookup_static("sex", "M", domains={domain_filter})
            assert result is not None, f"filter {domain_filter!r} should match"

    def test_domain_filter_miss_is_case_insensitive(self, static_index):
        """Wrong-domain filter misses regardless of case."""
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", domains={"Procedure"})

        assert result is None
        assert len(service.result.missed["static"]) == 1

    def test_vocab_filter_matches(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", vocabs={"Gender"})
        assert result is not None

    def test_vocab_filter_misses(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", vocabs={"SNOMED"})
        assert result is None

    def test_validity_filter_matches(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", validity={"Valid"})
        assert result is not None

    def test_validity_filter_misses(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", validity={"Deleted"})
        assert result is None

    def test_combined_filters(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M", domains={"Gender"}, vocabs={"Gender"}, validity={"Valid"})
        assert result is not None

        result = service.lookup_static("sex", "M", domains={"Gender"}, vocabs={"SNOMED"})
        assert result is None


def _make_semantic_row(concept_id: str, domain: str = "condition", name: str = "test") -> SemanticRow:
    return SemanticRow(
        term_id="t1",
        source_col="col",
        source_term="term",
        frequency=1,
        omop_concept_id=concept_id,
        omop_concept_code="code",
        omop_concept_name=name,
        omop_concept_class="class",
        omop_standard_concept="concept",
        omop_validity="standard",
        omop_domain=domain,
        omop_vocab="snomed",
    )


def _make_query(patient_id: str = "P1", field_path: tuple[str, ...] = ("collection", "field"), leaf_index: int = 0) -> Query:
    return Query(patient_id=patient_id, id="q1", query="test", field_path=field_path, raw_value="test", leaf_index=leaf_index)


def _build_semantic_index(*query_results: QueryResult) -> SemanticResultIndex:
    return SemanticResultIndex.from_batch(BatchQueryResult(results=tuple(query_results)))


class TestSemanticLookup:
    """
    lookup_semantic returns a tuple of concepts (0, 1, or N),
    raises on duplicate concept_ids (mapping file issue).
    """

    def test_single_match_returns_tuple(self, static_index):
        query = _make_query()
        qr = QueryResult(patient_id="P1", query=query, results=[_make_semantic_row("12345")])
        idx = _build_semantic_index(qr)

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.lookup_semantic("P1", ("collection", "field"), 0)

        assert len(result) == 1
        assert result[0].concept_id == 12345

    def test_zero_match_returns_empty_tuple(self, static_index):
        idx = _build_semantic_index()

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.lookup_semantic("P1", ("collection", "field"), 0)

        assert result == ()

    def test_multi_match_returns_all(self, static_index):
        """Legitimate multi-concept (e.g. combination drug) returns all."""
        query = _make_query()
        qr = QueryResult(
            patient_id="P1",
            query=query,
            results=[_make_semantic_row("111"), _make_semantic_row("222")],
        )
        idx = _build_semantic_index(qr)

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.lookup_semantic("P1", ("collection", "field"), 0)

        assert len(result) == 2
        assert {m.concept_id for m in result} == {111, 222}

    def test_duplicate_concept_id_raises(self, static_index):
        """Same concept_id twice = mapping file issue, must be fixed."""
        query = _make_query()
        qr = QueryResult(
            patient_id="P1",
            query=query,
            results=[_make_semantic_row("111"), _make_semantic_row("111")],
        )
        idx = _build_semantic_index(qr)

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        with pytest.raises(RuntimeError, match="Duplicate concept_id"):
            service.lookup_semantic("P1", ("collection", "field"), 0)

    def test_domain_filter_narrows_results(self, static_index):
        query = _make_query()
        qr = QueryResult(
            patient_id="P1",
            query=query,
            results=[
                _make_semantic_row("111", domain="condition"),
                _make_semantic_row("222", domain="procedure"),
            ],
        )
        idx = _build_semantic_index(qr)

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.lookup_semantic("P1", ("collection", "field"), 0, domains={"condition"})

        assert len(result) == 1
        assert result[0].concept_id == 111

    def test_no_semantic_index_returns_empty(self, static_index):
        service = ConceptLookupService(static_index=static_index, semantic_index=None)

        result = service.lookup_semantic("P1", ("collection", "field"), 0)

        assert result == ()
