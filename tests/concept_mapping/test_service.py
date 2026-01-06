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
