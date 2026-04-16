from omop_etl.concept_mapping.service import ConceptLookupService


class TestConceptLookupService:
    def test_lookup_static_hit(self, static_index):
        service = ConceptLookupService(static_index=static_index)

        result = service.lookup_static("sex", "M")

        assert result is not None
        assert result.concept_id == "8507"
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
        assert result.concept_id == "32817"

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
        assert result.concept_id == "8507"

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
        assert result.concept_id == "32817"

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
