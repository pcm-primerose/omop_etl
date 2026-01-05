from omop_etl.concept_mapping.core.models import (
    LookupResult,
    MissedLookup,
    MappedConcept,
)


class TestLookupResult:
    def test_record_match(self):
        result = LookupResult()
        concept = MappedConcept(
            concept_id="8507",
            concept_code="M",
            concept_name="Male",
            domain_id="Gender",
            vocabulary_id="Gender",
            standard_flag="Valid",
        )

        result.record_match("static", "sex", "M", concept)

        assert len(result.matched["static"]) == 1
        assert result.matched["static"][0] == ("sex", "M", concept)

    def test_record_miss(self):
        result = LookupResult()

        result.record_miss("static", "sex", "X")

        assert len(result.missed["static"]) == 1
        assert result.missed["static"][0] == MissedLookup("static", "sex", "X")

    def test_coverage_by_field(self):
        result = LookupResult()
        concept = MappedConcept("1", "C", "Name", "Domain", "Vocab", "Valid")

        result.record_match("static", "sex", "M", concept)
        result.record_match("static", "sex", "F", concept)
        result.record_miss("static", "sex", "X")

        coverage = result.coverage_by_field("static")

        assert "sex" in coverage
        assert coverage["sex"].matched == 2
        assert coverage["sex"].missed == 1
        assert coverage["sex"].total == 3
        assert coverage["sex"].coverage_fraction == round(2 / 3, 5)

    def test_missed_list_all(self):
        result = LookupResult()
        result.record_miss("static", "sex", "X")
        result.record_miss("structural", "ecrf", "unknown")

        missed = result.missed_list()

        assert len(missed) == 2

    def test_missed_list_filtered(self):
        result = LookupResult()
        result.record_miss("static", "sex", "X")
        result.record_miss("structural", "ecrf", "unknown")

        missed = result.missed_list("static")

        assert len(missed) == 1
        assert missed[0].lookup_type == "static"
