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
    def test_resolve_static_hit(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M")

        assert len(result) == 1
        assert result[0].concept_id == 8507
        assert result[0].concept_name == "Male"

    def test_resolve_static_miss(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "X")

        assert result == ()
        assert len(service.result.missed["static"]) == 1

    def test_resolve_structural_hit(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.resolve("ecrf")

        assert len(result) == 1
        assert result[0].concept_id == 32817

    def test_resolve_structural_miss(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.resolve("unknown")

        assert result == ()
        assert len(service.result.missed["structural"]) == 1

    def test_result_accumulates(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        service.resolve("sex", "M")
        service.resolve("sex", "F")
        service.resolve("sex", "X")

        assert len(service.result.matched["static"]) == 2
        assert len(service.result.missed["static"]) == 1

    def test_reset_clears_results(self, static_index):
        service = ConceptLookupService(static_index=static_index)
        service.resolve("sex", "M")

        service.reset()

        assert len(service.result.matched["static"]) == 0


class TestCaseInsensitiveLookups:
    """Lookup keys are case-normalized and stripped"""

    def test_resolve_static_matches_uppercase_input(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        # fixture indexes under ("sex", "m"): caller passes "M"
        result = service.resolve("sex", "M")

        assert len(result) == 1
        assert result[0].concept_id == 8507

    def test_resolve_static_matches_titlecase_value_set(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        # value_set case-insensitive too
        result = service.resolve("SEX", "m")

        assert result

    def test_resolve_static_strips_whitespace(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("  sex  ", "  M  ")

        assert result

    def test_resolve_structural_matches_uppercase_input(self, static_index, structural_index):
        service = ConceptLookupService(
            static_index=static_index,
            structural_index=structural_index,
        )

        result = service.resolve("ECRF")

        assert len(result) == 1
        assert result[0].concept_id == 32817

    def test_domain_filter_is_case_insensitive(self, static_index):
        """Filter accepts mixed-case domain strings and still matches."""
        service = ConceptLookupService(static_index=static_index)

        # fixture domain_id is "Gender": filter passes "gender" / "GENDER" / "Gender"
        for domain_filter in ("gender", "GENDER", "Gender"):
            result = service.resolve("sex", "M", domains={domain_filter})
            assert result, f"filter {domain_filter!r} should match"

    def test_domain_filter_rejects_regardless_of_case(self, static_index):
        """Wrong-domain filter returns an empty tuple regardless of case."""
        service = ConceptLookupService(static_index=static_index)

        for domain_filter in ("procedure", "PROCEDURE", "Procedure"):
            result = service.resolve("sex", "M", domains={domain_filter})
            assert result == (), f"filter {domain_filter!r} should reject"

    def test_filter_reject_is_not_recorded_as_miss(self, static_index):
        """
        An entry exists in the index but the requested filter rejects it.
        This is a caller-side flow event, a builder is asking the wrong domain question,
        and not a data-quality gap, so it must not be recorded in the missed-lookup log.
        """
        service = ConceptLookupService(static_index=static_index)

        service.resolve("sex", "M", domains={"Procedure"})

        assert len(service.result.missed["static"]) == 0

    def test_vocab_filter_matches(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M", vocabs={"Gender"})
        assert result

    def test_vocab_filter_misses(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M", vocabs={"SNOMED"})
        assert result == ()

    def test_validity_filter_matches(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M", validity={"Valid"})
        assert result

    def test_validity_filter_misses(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M", validity={"Deleted"})
        assert result == ()

    def test_combined_filters(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.resolve("sex", "M", domains={"Gender"}, vocabs={"Gender"}, validity={"Valid"})
        assert result

        result = service.resolve("sex", "M", domains={"Gender"}, vocabs={"SNOMED"})
        assert result == ()


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


def _make_query(patient_id: str = "P1", field_path: tuple[str, ...] = ("collection", "field"), value: str = "test") -> Query:
    return Query(patient_id=patient_id, id="q1", query=value, field_path=field_path, raw_value=value)


def _build_semantic_index(*query_results: QueryResult) -> SemanticResultIndex:
    return SemanticResultIndex.from_batch(BatchQueryResult(results=tuple(query_results)))


class TestSemanticLookup:
    """
    resolve() on a term-field (field_path) returns a tuple of concepts (0, 1, or N),
    raises on duplicate concept_ids (mapping file issue).
    """

    def test_single_match_returns_tuple(self, static_index):
        query = _make_query()
        qr = QueryResult(patient_id="P1", query=query, results=[_make_semantic_row("12345")])
        idx = _build_semantic_index(qr)

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.resolve(("collection", "field"), "test")

        assert len(result) == 1
        assert result[0].concept_id == 12345

    def test_zero_match_returns_empty_tuple(self, static_index):
        idx = _build_semantic_index()

        service = ConceptLookupService(static_index=static_index, semantic_index=idx)
        result = service.resolve(("collection", "field"), "test")

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
        result = service.resolve(("collection", "field"), "test")

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
            service.resolve(("collection", "field"), "test")

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
        result = service.resolve(("collection", "field"), "test", domains={"condition"})

        assert len(result) == 1
        assert result[0].concept_id == 111

    def test_no_semantic_index_returns_empty(self, static_index):
        service = ConceptLookupService(static_index=static_index, semantic_index=None)

        result = service.resolve(("collection", "field"), "test")

        assert result == ()
