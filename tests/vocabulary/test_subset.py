import pytest

from omop_etl.vocabulary.core.subset import collect_concept_ids, concept_subset_report, scan_concept_subset
from omop_etl.infra.utils.constants import NO_MATCHING_CONCEPT
from tests.vocabulary.conftest import ConceptRow, write_concept_csv


class TestCollectConceptIds:
    def test_collects_ids_from_all_mapping_files(self, static_mapping_file, structural_mapping_file):
        ids = collect_concept_ids([static_mapping_file, structural_mapping_file])

        assert ids == {8507, 8532, 32817, NO_MATCHING_CONCEPT}

    def test_always_includes_no_matching_concept_sentinel(self, static_mapping_file):
        ids = collect_concept_ids([static_mapping_file])

        assert NO_MATCHING_CONCEPT in ids

    def test_blank_concept_id_raises(self, tmp_path):
        path = tmp_path / "bad_mapping.csv"
        path.write_text("value_set,source_value,concept_id\nsex,M,\n")

        with pytest.raises(ValueError, match="blank `concept_id`"):
            collect_concept_ids([path])


class TestScanConceptSubset:
    def test_returns_only_the_wanted_concepts(self, tmp_path):
        write_concept_csv(
            tmp_path,
            ConceptRow(4112853),
            ConceptRow(8507, concept_name="Male", domain_id="Gender", vocabulary_id="Gender"),
            ConceptRow(999999, concept_name="Not wanted"),  # present in source, not in any mapping
        )

        subset = scan_concept_subset(tmp_path, {4112853, 8507})

        assert set(subset.get_column("concept_id").cast(int).to_list()) == {4112853, 8507}


class TestConceptSubsetReport:
    def test_reports_mapping_ids_missing_from_this_vocab_release(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853))
        subset = scan_concept_subset(tmp_path, {4112853, 55555})

        report = concept_subset_report(subset, {4112853, 55555})

        assert report.missing_concept_ids == frozenset({55555})

    def test_flags_non_standard_and_invalid_concepts(self, tmp_path):
        write_concept_csv(
            tmp_path,
            ConceptRow(1, standard_concept=""),  # non-standard
            ConceptRow(2, invalid_reason="D"),  # invalid (deprecated)
            ConceptRow(3),  # clean: standard + valid, never flagged
        )
        subset = scan_concept_subset(tmp_path, {1, 2, 3})

        report = concept_subset_report(subset, {1, 2, 3})

        flagged = {f.concept_id: f.reason for f in report.flagged_concepts}
        assert flagged == {1: "non-standard (null)", 2: "invalid (D)"}

    def test_no_matching_concept_sentinel_is_never_flagged(self, tmp_path):
        # real Athena shape: "No matching concept" has a blank standard_concept, which
        # would otherwise trip the non-standard flag on every single report
        write_concept_csv(
            tmp_path,
            ConceptRow(
                NO_MATCHING_CONCEPT,
                concept_name="No matching concept",
                domain_id="Metadata",
                vocabulary_id="None",
                concept_class_id="Undefined",
                standard_concept="",
                concept_code="No matching concept",
            ),
        )
        subset = scan_concept_subset(tmp_path, {NO_MATCHING_CONCEPT})

        report = concept_subset_report(subset, {NO_MATCHING_CONCEPT})

        assert report.flagged_concepts == ()
