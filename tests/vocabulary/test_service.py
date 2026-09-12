from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.vocabulary.service import VocabularyService
from tests.conftest import MappingRow, write_static_mapping_csv
from tests.vocabulary.conftest import (
    ConceptAncestorRow,
    ConceptRow,
    write_athena_bundle,
    write_concept_ancestor_csv,
    write_concept_csv,
)


class TestVocabularyServiceRun:
    def test_clean_run_with_no_drug_concepts_has_empty_concept_ancestor(self, tmp_path):
        athena_dir = tmp_path / "athena"
        athena_dir.mkdir()
        write_athena_bundle(athena_dir)  # default bundle: one Condition-domain concept, 4112853

        mapping_path = write_static_mapping_csv(
            tmp_path / "static.csv",
            MappingRow(4112853, value_set="tumor_type", source_value="melanoma"),
        )

        service = VocabularyService(outdir=tmp_path / "out", athena_dir=athena_dir, mapping_files=[mapping_path])
        result = service.run(RunMetadata.create("TEST"))

        assert result.report.is_clean
        assert result.concept_ancestor.height == 0

    def test_drug_concept_ancestor_gets_scanned_and_folded_into_vocabulary(self, tmp_path):
        athena_dir = tmp_path / "athena"
        athena_dir.mkdir()
        write_athena_bundle(athena_dir)
        write_concept_csv(
            athena_dir,
            ConceptRow(
                100,
                concept_code="308191",
                concept_name="Amoxicillin 250mg tablet",
                domain_id="Drug",
                concept_class_id="Clinical Drug",
                vocabulary_id="RxNorm",
            ),
            # the ingredient ancestor itself is not in any mapping file,
            # only reachable via CONCEPT_ANCESTOR
            ConceptRow(
                200,
                concept_code="723",
                concept_name="Amoxicillin",
                domain_id="Drug",
                concept_class_id="Ingredient",
                vocabulary_id="RxNorm",
            ),
        )
        write_concept_ancestor_csv(athena_dir, ConceptAncestorRow(ancestor_concept_id=200, descendant_concept_id=100))

        mapping_path = write_static_mapping_csv(
            tmp_path / "static.csv",
            MappingRow(
                100,
                value_set="drug",
                source_value="amox",
                concept_code="308191",
                concept_name="Amoxicillin 250mg tablet",
                concept_class_id="Clinical Drug",
                standard_concept="Standard",
                validity="Valid",
                domain_id="Drug",
                vocabulary_id="RxNorm",
            ),
        )

        service = VocabularyService(outdir=tmp_path / "out", athena_dir=athena_dir, mapping_files=[mapping_path])
        result = service.run(RunMetadata.create("TEST"))

        assert result.report.is_clean
        ancestor_rows = result.concept_ancestor.select("ancestor_concept_id", "descendant_concept_id").to_dicts()
        assert ancestor_rows == [{"ancestor_concept_id": "200", "descendant_concept_id": "100"}]

        # 100 was already mapped; 200 (the ingredient ancestor) needed the extra fold-in scan
        mapped = result.vocabulary.hydrate(100)
        assert mapped is not None
        assert mapped.concept_name == "Amoxicillin 250mg tablet"

        ingredient = result.vocabulary.hydrate(200)
        assert ingredient is not None
        assert ingredient.concept_class_id == "Ingredient"
        assert ingredient.concept_name == "Amoxicillin"
