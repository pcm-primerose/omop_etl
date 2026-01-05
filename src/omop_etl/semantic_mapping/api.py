from pathlib import Path
from typing import Sequence, Set, List

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.format_utils import expand_formats
from omop_etl.infra.io.types import Layout, AnyFormatToken, WideFormat, WIDE_FORMATS
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.semantic_mapping.core.models import SemanticMappingResult
from omop_etl.semantic_mapping.core.models import FieldConfig, OmopDomain
from omop_etl.semantic_mapping.core.pipeline import SemanticLookupPipeline


class SemanticService:
    def __init__(
        self,
        outdir: Path | None = None,
        layout: Layout = Layout.TRIAL_RUN,
    ):
        self.outdir = outdir
        self.layout = layout

    def run(
        self,
        trial: str,
        harmonized_data: HarmonizedData,
        meta: RunMetadata,
        formats: AnyFormatToken = "all",
        input_path: Path | None = None,
        write_output: bool | None = True,
        semantic_path: Path | None = None,
        configs: Sequence[FieldConfig] | None = None,
        enable_names: Set[str] | None = None,
        required_domains: Set[OmopDomain] | None = None,
        required_tags: Set[str] | None = None,
    ) -> SemanticMappingResult:
        supported_fmts: List[WideFormat] = expand_formats(formats, allowed=WIDE_FORMATS)

        pipeline = SemanticLookupPipeline(
            trial=trial,
            meta=meta,
            semantics_path=semantic_path,
            field_configs=configs,
            layout=self.layout,
        )

        return pipeline.run(
            harmonized_data=harmonized_data,
            enable_names=enable_names,
            required_domains=required_domains,
            required_tags=required_tags,
            write_output=write_output,
            input_path=input_path,
            formats=supported_fmts,
        )
