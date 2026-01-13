import pytest
from typing import List

from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.semantic_mapping.core.pipeline import SemanticLookupPipeline
from omop_etl.semantic_mapping.core.models import (
    FieldConfig,
    OmopDomain,
    QueryTarget,
    SemanticMappingResult,
)


@pytest.fixture
def run_meta() -> RunMetadata:
    return RunMetadata(
        trial="TEST",
        run_id="abc123",
        started_at="20260105T120000Z",
    )


@pytest.fixture
def harmonized_data(patients) -> HarmonizedData:
    return HarmonizedData(trial_id="TEST", patients=patients)


@pytest.fixture
def custom_configs() -> List[FieldConfig]:
    return [
        FieldConfig(
            name="custom.field",
            field_path=("tumor_type", "main_tumor_type"),
            target=QueryTarget(domains={OmopDomain.CONDITION}),
            tags={"custom", "test"},
        ),
    ]


class TestSemanticLookupPipeline:
    def test_init_with_semantic_file(self, semantic_file, run_meta):
        pipeline = SemanticLookupPipeline(
            trial="TEST",
            meta=run_meta,
            semantics_path=semantic_file,
        )

        assert pipeline.trial == "TEST"
        assert pipeline.meta == run_meta

    def test_run_returns_semantic_mapping_result(self, semantic_file, run_meta, harmonized_data):
        pipeline = SemanticLookupPipeline(
            trial="TEST",
            meta=run_meta,
            semantics_path=semantic_file,
        )

        result = pipeline.run(
            harmonized_data=harmonized_data,
            write_output=False,
        )

        assert isinstance(result, SemanticMappingResult)
        assert result.meta == run_meta

    def test_run_with_field_configs(self, semantic_file, run_meta, harmonized_data, configs):
        pipeline = SemanticLookupPipeline(
            trial="TEST",
            meta=run_meta,
            semantics_path=semantic_file,
            field_configs=configs,
        )

        result = pipeline.run(
            harmonized_data=harmonized_data,
            write_output=False,
        )

        # Should have queries for the configured fields
        assert result.total_queries > 0

    def test_run_filters_by_enable_names(self, semantic_file, run_meta, harmonized_data, configs):
        pipeline = SemanticLookupPipeline(
            trial="TEST",
            meta=run_meta,
            semantics_path=semantic_file,
            field_configs=configs,
        )

        # Only enable one of the two configs
        result = pipeline.run(
            harmonized_data=harmonized_data,
            enable_names={"tumor type main"},
            write_output=False,
        )

        # Should have fewer queries than if all configs were enabled
        all_result = pipeline.run(
            harmonized_data=harmonized_data,
            write_output=False,
        )

        assert result.total_queries <= all_result.total_queries


class TestBuildConfigs:
    def test_build_configs_returns_all_when_no_filters(self):
        configs = [
            FieldConfig(
                name="config1",
                field_path=("a",),
                target=QueryTarget(domains={OmopDomain.CONDITION}),
                tags={"tag1"},
            ),
            FieldConfig(
                name="config2",
                field_path=("b",),
                target=QueryTarget(domains={OmopDomain.DRUG}),
                tags={"tag2"},
            ),
        ]

        result = SemanticLookupPipeline._build_configs(configs)

        assert len(result) == 2

    def test_build_configs_filter_by_name(self):
        configs = [
            FieldConfig(name="config1", field_path=("a",)),
            FieldConfig(name="config2", field_path=("b",)),
            FieldConfig(name="config3", field_path=("c",)),
        ]

        result = SemanticLookupPipeline._build_configs(
            configs,
            enable_names={"config1", "config3"},
        )

        assert len(result) == 2
        names = {c.name for c in result}
        assert names == {"config1", "config3"}

    def test_build_configs_filter_by_domain(self):
        configs = [
            FieldConfig(
                name="condition_config",
                field_path=("a",),
                target=QueryTarget(domains={OmopDomain.CONDITION}),
            ),
            FieldConfig(
                name="drug_config",
                field_path=("b",),
                target=QueryTarget(domains={OmopDomain.DRUG}),
            ),
            FieldConfig(
                name="no_target",
                field_path=("c",),
            ),
        ]

        result = SemanticLookupPipeline._build_configs(
            configs,
            required_domains={OmopDomain.CONDITION},
        )

        assert len(result) == 1
        assert result[0].name == "condition_config"

    def test_build_configs_filter_by_tags(self):
        configs = [
            FieldConfig(name="config1", field_path=("a",), tags={"important", "medical"}),
            FieldConfig(name="config2", field_path=("b",), tags={"other"}),
            FieldConfig(name="config3", field_path=("c",), tags={"important"}),
        ]

        result = SemanticLookupPipeline._build_configs(
            configs,
            required_tags={"important"},
        )

        assert len(result) == 2
        names = {c.name for c in result}
        assert names == {"config1", "config3"}

    def test_build_configs_combined_filters(self):
        configs = [
            FieldConfig(
                name="match",
                field_path=("a",),
                target=QueryTarget(domains={OmopDomain.CONDITION}),
                tags={"important"},
            ),
            FieldConfig(
                name="wrong_domain",
                field_path=("b",),
                target=QueryTarget(domains={OmopDomain.DRUG}),
                tags={"important"},
            ),
            FieldConfig(
                name="wrong_tag",
                field_path=("c",),
                target=QueryTarget(domains={OmopDomain.CONDITION}),
                tags={"other"},
            ),
        ]

        result = SemanticLookupPipeline._build_configs(
            configs,
            enable_names={"match", "wrong_domain", "wrong_tag"},
            required_domains={OmopDomain.CONDITION},
            required_tags={"important"},
        )

        assert len(result) == 1
        assert result[0].name == "match"


class TestMergeFieldConfigs:
    def test_merge_adds_new_configs(self):
        defaults = [
            FieldConfig(name="default1", field_path=("a",)),
        ]
        overrides = [
            FieldConfig(name="override1", field_path=("b",)),
        ]

        result = SemanticLookupPipeline._merge_field_configs(defaults, overrides)

        assert len(result) == 2
        names = {c.name for c in result}
        assert names == {"default1", "override1"}

    def test_merge_overrides_by_name(self):
        defaults = [
            FieldConfig(name="config1", field_path=("original",)),
        ]
        overrides = [
            FieldConfig(name="config1", field_path=("overridden",)),
        ]

        result = SemanticLookupPipeline._merge_field_configs(defaults, overrides)

        assert len(result) == 1
        assert result[0].field_path == ("overridden",)

    def test_merge_preserves_defaults_not_overridden(self):
        defaults = [
            FieldConfig(name="keep", field_path=("a",)),
            FieldConfig(name="replace", field_path=("b",)),
        ]
        overrides = [
            FieldConfig(name="replace", field_path=("c",)),
            FieldConfig(name="new", field_path=("d",)),
        ]

        result = SemanticLookupPipeline._merge_field_configs(defaults, overrides)

        assert len(result) == 3
        by_name = {c.name: c for c in result}
        assert by_name["keep"].field_path == ("a",)
        assert by_name["replace"].field_path == ("c",)
        assert by_name["new"].field_path == ("d",)


class TestBuildQueries:
    def test_build_queries_from_harmonized_data(self, harmonized_data, configs):
        queries = SemanticLookupPipeline._build_queries(
            harmonized_data=harmonized_data,
            configs=configs,
        )

        # Each patient has tumor_type and medical_histories configured
        # 2 patients * 2 configs = 4 queries
        assert len(queries) == 4

    def test_build_queries_empty_patients(self, configs):
        empty_data = HarmonizedData(trial_id="EMPTY", patients=[])

        queries = SemanticLookupPipeline._build_queries(
            harmonized_data=empty_data,
            configs=configs,
        )

        assert len(queries) == 0

    def test_build_queries_empty_configs(self, harmonized_data):
        queries = SemanticLookupPipeline._build_queries(
            harmonized_data=harmonized_data,
            configs=[],
        )

        assert len(queries) == 0
