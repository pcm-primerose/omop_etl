import pytest

from omop_etl.vocabulary.subset import collect_concept_ids, generate_concept_subset
from omop_etl.infra.utils.constants import NO_MATCHING_CONCEPT
from tests.vocabulary.conftest import ConceptRow, write_concept_tsv


class TestCollectConceptIds:
    def test_collects_ids_from_all_mapping_files(self, static_mapping_file, structural_mapping_file):
        ids = collect_concept_ids([static_mapping_file, structural_mapping_file])

        assert ids == {8507, 8532, 32817, NO_MATCHING_CONCEPT}

    def test_always_includes_no_matching_concept_sentinel(self, static_mapping_file):
        ids = collect_concept_ids([static_mapping_file])

        assert NO_MATCHING_CONCEPT in ids

    def test_blank_concept_id_raises(self, tmp_path):
        path = tmp_path / "bad_mapping.csv"
        path.write_text("value_set,local_value,omop_concept_id\nsex,M,\n")

        with pytest.raises(ValueError, match="blank `omop_concept_id`"):
            collect_concept_ids([path])


class TestGenerateConceptSubset:
    def test_writes_only_the_wanted_concepts(self, tmp_path):
        source = write_concept_tsv(
            tmp_path / "CONCEPT.csv",
            ConceptRow(4112853),
            ConceptRow(8507, concept_name="Male", domain_id="Gender", vocabulary_id="Gender"),
            ConceptRow(999999, concept_name="Not wanted"),  # present in source, not in any mapping
        )
        out_path = tmp_path / "subset" / "concept_subset.tsv"

        report = generate_concept_subset(source, {4112853, 8507}, out_path)

        assert report.written_count == 2
        written_ids = {int(line.split("\t")[0]) for line in out_path.read_text().splitlines()[1:]}
        assert written_ids == {4112853, 8507}

    def test_reports_mapping_ids_missing_from_this_vocab_release(self, tmp_path):
        source = write_concept_tsv(tmp_path / "CONCEPT.csv", ConceptRow(4112853))

        report = generate_concept_subset(source, {4112853, 55555}, tmp_path / "out.tsv")

        assert report.missing_concept_ids == frozenset({55555})

    def test_flags_non_standard_and_invalid_concepts(self, tmp_path):
        source = write_concept_tsv(
            tmp_path / "CONCEPT.csv",
            ConceptRow(1, standard_concept=""),  # non-standard
            ConceptRow(2, invalid_reason="D"),  # invalid (deprecated)
            ConceptRow(3),  # clean: standard + valid, never flagged
        )

        report = generate_concept_subset(source, {1, 2, 3}, tmp_path / "out.tsv")

        flagged = {f.concept_id: f.reason for f in report.flagged_concepts}
        assert flagged == {1: "non-standard (null)", 2: "invalid (D)"}

    def test_no_matching_concept_sentinel_is_never_flagged(self, tmp_path):
        # real Athena shape: "No matching concept" has a blank standard_concept, which
        # would otherwise trip the non-standard flag on every single generation run
        source = write_concept_tsv(
            tmp_path / "CONCEPT.csv",
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

        report = generate_concept_subset(source, {NO_MATCHING_CONCEPT}, tmp_path / "out.tsv")

        assert report.flagged_concepts == ()

    def test_creates_missing_parent_directories(self, tmp_path):
        source = write_concept_tsv(tmp_path / "CONCEPT.csv", ConceptRow(4112853))
        out_path = tmp_path / "nested" / "dir" / "concept_subset.tsv"

        generate_concept_subset(source, {4112853}, out_path)

        assert out_path.exists()
