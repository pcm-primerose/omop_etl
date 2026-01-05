from pathlib import Path
from typing import (
    Set,
    Sequence,
    Dict,
)

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import WideFormat, Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.semantic_mapping.core.io_export import SemanticExporter
from omop_etl.semantic_mapping.core.models import SemanticMappingResult
from omop_etl.semantic_mapping.core.loader import LoadSemantics
from omop_etl.semantic_mapping.core.query_extractor import extract_queries, validate_field_paths
from omop_etl.semantic_mapping.core.semantic_config import DEFAULT_FIELD_CONFIGS
from omop_etl.semantic_mapping.core.semantic_index import SemanticIndex
from omop_etl.semantic_mapping.core.models import (
    OmopDomain,
    Query,
    FieldConfig,
)


class SemanticLookupPipeline:
    def __init__(
        self,
        trial: str,
        meta: RunMetadata,
        semantics_path: Path | None = None,
        layout: Layout = Layout.TRIAL_RUN,
        field_configs: Sequence[FieldConfig] | None = None,
        exporter: SemanticExporter | None = None,
        outdir: Path | None = None,
    ):
        self.meta = meta
        # resolve semantic path (default in resources if not given)
        loader = LoadSemantics(semantics_path)
        self._index = SemanticIndex(indexed_corpus=loader.as_indexed())

        # merge defaults and overrides with provided config by name
        base = list(DEFAULT_FIELD_CONFIGS)
        if field_configs is not None:
            base = self._merge_field_configs(base, field_configs)
        self._field_configs: list[FieldConfig] = base
        self._validated = False
        self.exporter = exporter or SemanticExporter(
            base_out=(outdir if outdir is not None else Path(".data")),
            layout=layout,
        )
        self.trial = trial

    def run(
        self,
        harmonized_data: HarmonizedData,
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
        input_path: Path | None = None,
        formats: Sequence[WideFormat] = "csv",
        write_output: bool | None = None,
    ) -> SemanticMappingResult:
        configs = self._build_configs(
            base_configs=self._field_configs,
            enable_names=enable_names,
            required_domains=required_domains,
            required_tags=required_tags,
        )

        # validate FieldConfig against first 10 Patient instances
        if not self._validated and harmonized_data.patients:
            validate_field_paths(harmonized_data.patients[:10], configs)
            self._validated = True

        # run lookup & collect queries
        all_queries = self._build_queries(harmonized_data=harmonized_data, configs=configs)
        query_results = self._index.lookup_exact(queries=all_queries)

        # write output if enabled
        if write_output is True and self.exporter is not None:
            self.exporter.export(
                batch_result=query_results,
                meta=self.meta,
                input_path=input_path,
                formats=formats,
            )
            if write_output is True and self.exporter is None:
                raise ValueError(f"No exporter initialized in {self.__class__.__name__}")

        return SemanticMappingResult(
            batch_result=query_results,
            meta=self.meta,
        )

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
            configs = [
                c for c in configs if c.target and c.target.domains is not None and any(dom in required_domains for dom in c.target.domains)
            ]

        if required_tags is not None:
            configs = [c for c in configs if any(tag in required_tags for tag in c.tags)]

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
