from pathlib import Path
from typing import Optional, List, Sequence
import copy

from .core.pipeline import PreprocessingPipeline, PreprocessResult
from .core.io_export import PreprocessExporter
from .core.config_loader import load_ecrf_config
from .core.models import EcrfConfig, PreprocessingRunOptions
from .core.dispatch import resolve_processor as _resolve_processor, list_trials as _list_trials
from omop_etl.infra.io.types import Layout
from omop_etl.infra.io.format_utils import expand_formats, NORMALIZED_FORMATS

__all__ = [
    "preprocess_trial",
    "make_ecrf_config",
    "list_trials",
    "PreprocessResult",
    "PreprocessingRunOptions",
]


def list_trials() -> List[str]:
    return _list_trials()


def make_ecrf_config(trial: str, custom_config_path: Optional[Path] = None) -> EcrfConfig:
    cfg_map = load_ecrf_config(trial=trial, custom_config_path=custom_config_path)
    return EcrfConfig.from_mapping(cfg_map)


def preprocess_trial(
    trial: str,
    input_path: Path,
    config: Optional[EcrfConfig] = None,
    run_options: Optional[PreprocessingRunOptions] = None,
    fmt: str | Sequence[str] | None = None,
    base_output_dir: Optional[Path] = None,
    layout: Layout = Layout.TRIAL_RUN,
) -> PreprocessResult:
    """High-level API for preprocessing trial data (wide file & manifest)."""
    processor = _resolve_processor(trial)
    cfg = make_ecrf_config(trial) if config is None else copy.deepcopy(config)

    # expand fmt list, defaults to all
    fmts = expand_formats(fmt, allowed=NORMALIZED_FORMATS, default="all")

    base_root = base_output_dir or Path(".data/preprocessing")
    exporter = PreprocessExporter(base_out=base_root, layout=layout)

    pipeline = PreprocessingPipeline(
        trial=trial,
        ecrf_config=cfg,
        output_manager=exporter,
        processor=processor,
    )

    return pipeline.run(
        input_path=input_path,
        options=run_options,
        formats=fmts,
        combine_key=PreprocessingRunOptions.combine_key,
    )
