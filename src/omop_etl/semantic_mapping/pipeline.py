from pathlib import Path
from typing import (
    Set,
    Sequence,
    Dict,
)

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.semantic_mapping.loader import LoadSemantics
from omop_etl.semantic_mapping.query_extractor import extract_queries
from omop_etl.semantic_mapping.semantic_config import DEFAULT_FIELD_CONFIGS
from omop_etl.semantic_mapping.semantic_index import DictSemanticIndex
from omop_etl.semantic_mapping.models import (
    OmopDomain,
    BatchQueryResult,
    Query,
    FieldConfig,
)


class SemanticLookupPipeline:
    def __init__(
        self,
        semantics_path: Path | None = None,
        field_configs: Sequence[FieldConfig] | None = None,
    ):
        # resolve semantic path (default in resources if not given)
        loader = LoadSemantics(semantics_path)
        self._index = DictSemanticIndex(indexed_corpus=loader.as_indexed())

        # merge defaults and overrides
        base = list(DEFAULT_FIELD_CONFIGS)
        if field_configs is not None:
            base = self._merge_field_configs(base, field_configs)
        self._field_configs: list[FieldConfig] = base

    def run_lookup(
        self,
        harmonized_data: HarmonizedData,
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
    ) -> BatchQueryResult:
        configs = self._build_configs(
            base_configs=self._field_configs,
            enable_names=enable_names,
            required_domains=required_domains,
            required_tags=required_tags,
        )
        all_queries = self._build_queries(harmonized_data=harmonized_data, configs=configs)
        return self._index.lookup_exact(queries=all_queries)

    @staticmethod
    def _build_configs(
        base_configs: Sequence[FieldConfig],
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
    ) -> list[FieldConfig]:
        configs = list(base_configs)

        if enable_names is not None:
            configs = [c for c in configs if c.name in enable_names]

        if required_domains is not None:
            configs = [c for c in configs if c.target and c.target.domains is not None and c.target.domains & required_domains]

        if required_tags is not None:
            configs = [c for c in configs if c.tags & required_tags]

        return configs

    @staticmethod
    def _build_queries(
        harmonized_data: HarmonizedData,
        configs: list[FieldConfig],
    ) -> list[Query]:
        all_queries: list[Query] = []
        for patient in harmonized_data.patients:
            all_queries.extend(extract_queries(patient=patient, configs=configs))
        return all_queries

    @staticmethod
    def _merge_field_configs(
        defaults: Sequence[FieldConfig],
        overrides: Sequence[FieldConfig],
    ) -> list[FieldConfig]:
        """Override default FieldConfig or add by name."""
        by_name: Dict[str, FieldConfig] = {c.name: c for c in defaults}
        for cfg in overrides:
            by_name[cfg.name] = cfg
        return list(by_name.values())
