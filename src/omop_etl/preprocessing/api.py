from pathlib import Path
from typing import Optional, List, Sequence

from .core.pipeline import PreprocessingPipeline, PreprocessResult
from .core.io_export import PreprocessExporter
from .core.config_loader import load_ecrf_config
from .core.models import EcrfConfig, PreprocessingRunOptions
from .core.dispatch import list_trials as _list_trials, resolve_processor
from omop_etl.infra.io.types import Layout, TabularFormat, AnyFormatToken, TABULAR_FORMATS
from omop_etl.infra.io.format_utils import expand_formats
from ..infra.utils.run_context import RunMetadata

__all__ = [
    "PreprocessingPipeline",
    "make_ecrf_config",
    "list_trials",
    "PreprocessResult",
    "PreprocessingRunOptions",
    "PreprocessService",
]


def list_trials() -> List[str]:
    return _list_trials()


def make_ecrf_config(trial: str, custom_config_path: Optional[Path] = None) -> EcrfConfig:
    cfg_map = load_ecrf_config(trial=trial, custom_config_path=custom_config_path)
    return EcrfConfig.from_mapping(cfg_map)


class PreprocessService:
    def __init__(self, outdir: Path, layout: Layout = Layout.TRIAL_RUN):
        self.outdir = outdir
        self.layout = layout

    def run(
        self,
        trial: str,
        input_path: Path,
        meta: RunMetadata,
        formats: AnyFormatToken | Sequence[AnyFormatToken] = "csv",
        config: Optional[EcrfConfig] = None,
        combine_key: Optional[str] = None,
        filter_valid_cohorts: Optional[bool] = True,
    ) -> PreprocessResult:
        cfg = config or make_ecrf_config(trial)
        key = combine_key or PreprocessingRunOptions.combine_key

        run_options = PreprocessingRunOptions(
            combine_key=key,
            filter_valid_cohort=filter_valid_cohorts,
        )

        # normalize formats
        fmts: list[TabularFormat] = expand_formats(formats, allowed=TABULAR_FORMATS)

        exporter = PreprocessExporter(
            base_out=self.outdir,
            layout=self.layout,
        )

        pipeline = PreprocessingPipeline(
            trial=trial,
            ecrf_config=cfg,
            meta=meta,
            output_manager=exporter,
            processor=resolve_processor(trial),
        )

        return pipeline.run(
            input_path=input_path,
            run_options=run_options,
            formats=fmts,
        )
