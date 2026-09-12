from omop_etl.vocabulary.core.helpers import REQUIRED_ATHENA_FILES
from omop_etl.vocabulary.core.validate import validate_athena_bundle, validate_mappings
from tests.conftest import (
    MappingRow,
    write_static_mapping_csv,
    write_structural_mapping_csv,
    write_semantic_mapping_csv,
)
from tests.vocabulary.conftest import ConceptRow, write_athena_bundle, write_concept_csv, write_vocabulary_csv


class TestValidateMappings:
    def test_clean_mapping_has_no_errors(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert report.is_clean
        assert report.errors == ()

    def test_missing_concept_id_is_an_error(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(999999, value_set="tumor_type", source_value="melanoma"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "missing"
        assert report.errors[0].concept_id == 999999

    def test_classification_tier_concept_is_not_an_error(self, tmp_path):
        # "C" (Classification) tier is valid
        write_concept_csv(tmp_path, ConceptRow(734318, standard_concept="C"))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(734318, value_set="response_irecist", standard_concept="Classification"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert report.is_clean

    def test_blank_standard_concept_is_still_an_error(self, tmp_path):
        # blank/null is the one value with no vocabulary-sanctioned tier at all
        write_concept_csv(tmp_path, ConceptRow(734318, standard_concept=""))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(734318, value_set="response_irecist", standard_concept="Non-standard"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "non_standard"

    def test_invalid_concept_is_an_error(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853, invalid_reason="D"))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma", validity="Invalid"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "invalid"

    def test_drift_on_concept_name_is_an_error(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853, concept_name="Malignant melanoma"))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma", concept_name="Wrong name"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "drift"
        assert report.errors[0].column == "concept_name"

    def test_drift_on_concept_class_id_is_also_an_error(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853, concept_class_id="Clinical Finding"))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma", concept_class_id="Wrong class"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "drift"
        assert report.errors[0].column == "concept_class_id"

    def test_structural_file_has_no_source_value_column(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(32817))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_structural_mapping_csv(
            tmp_path / "structural_mapping.csv",
            MappingRow(32817, value_set="ecrf"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert report.is_clean

    def test_semantic_file_has_no_value_set_column(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_semantic_mapping_csv(
            tmp_path / "semantic_mapped.csv",
            MappingRow(4112853, source_value="melanoma"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert report.is_clean

    def test_errors_across_multiple_files_are_attributed_correctly(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853), ConceptRow(32817))
        write_vocabulary_csv(tmp_path)
        static_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(999999, value_set="tumor_type", source_value="melanoma"),  # missing
        )
        structural_path = write_structural_mapping_csv(
            tmp_path / "structural_mapping.csv",
            MappingRow(32817, value_set="ecrf"),  # clean
        )

        report = validate_mappings([static_path, structural_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].file == static_path

    def test_malformed_row_is_reported_not_raised(self, tmp_path):
        # a ragged row (containing an extra unquoted field) must not crash the whole
        # validation run, just show up as one issue for this file
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        bad_path = tmp_path / "static_mapping.csv"
        bad_path.write_text(
            "value_set,source_value,concept_id,concept_code,concept_name,concept_class_id,"
            "standard_concept,validity,domain_id,vocabulary_id\n"
            "tumor_type,melanoma,extra,4112853,93655004,Malignant melanoma,Clinical Finding,"
            "Standard,Valid,Condition,SNOMED\n"
        )
        good_path = write_static_mapping_csv(
            tmp_path / "static_mapping_2.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
        )

        report = validate_mappings([bad_path, good_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "malformed"
        assert report.errors[0].file == bad_path

    def test_missing_required_column_is_reported_not_raised(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        bad_path = tmp_path / "static_mapping.csv"
        bad_path.write_text("value_set,source_value,concept_id\ntumor_type,melanoma,4112853\n")

        report = validate_mappings([bad_path], tmp_path)

        assert len(report.errors) == 1
        assert report.errors[0].kind == "malformed"

    def test_duplicate_key_conflicting_concept_ids_is_an_error(self, tmp_path):
        write_concept_csv(tmp_path, ConceptRow(4112853), ConceptRow(9999999))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
            MappingRow(9999999, value_set="tumor_type", source_value="melanoma"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        duplicate_key_errors = [e for e in report.errors if e.kind == "duplicate_key"]
        assert len(duplicate_key_errors) == 1

    def test_duplicate_key_same_concept_id_is_not_an_error(self, tmp_path):
        # duplicate row (same key, same concept_id) is harmless
        write_concept_csv(tmp_path, ConceptRow(4112853))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_static_mapping_csv(
            tmp_path / "static_mapping.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert report.is_clean

    def test_duplicate_key_check_skipped_for_semantic_files(self, tmp_path):
        # same source_value legitimately mapping to N concepts (e.g. combination
        # drugs) is not a duplicate-key violation for semantic files
        write_concept_csv(tmp_path, ConceptRow(4112853), ConceptRow(9999999))
        write_vocabulary_csv(tmp_path)
        mapping_path = write_semantic_mapping_csv(
            tmp_path / "semantic_mapped.csv",
            MappingRow(4112853, source_value="combo drug"),
            MappingRow(9999999, source_value="combo drug"),
        )

        report = validate_mappings([mapping_path], tmp_path)

        assert not any(e.kind == "duplicate_key" for e in report.errors)


class TestValidateAthenaBundle:
    def test_complete_bundle_has_no_problems(self, tmp_path):
        write_athena_bundle(tmp_path)

        assert validate_athena_bundle(tmp_path) == ()

    def test_missing_file_is_reported(self, tmp_path):
        write_athena_bundle(tmp_path, skip=("DRUG_STRENGTH.csv",))

        problems = validate_athena_bundle(tmp_path)

        assert len(problems) == 1
        assert "DRUG_STRENGTH.csv" in problems[0]
        assert "missing" in problems[0]

    def test_multiple_missing_files_are_all_reported(self, tmp_path):
        write_athena_bundle(tmp_path, skip=("DOMAIN.csv", "RELATIONSHIP.csv"))

        problems = validate_athena_bundle(tmp_path)

        assert len(problems) == 2
        assert any("DOMAIN.csv" in p for p in problems)
        assert any("RELATIONSHIP.csv" in p for p in problems)

    def test_checks_every_required_file(self, tmp_path):
        # every file this checks is exactly REQUIRED_ATHENA_FILES, if a bundle is
        # missing all of them, every one of them is reported
        problems = validate_athena_bundle(tmp_path)

        assert len(problems) == len(REQUIRED_ATHENA_FILES)
