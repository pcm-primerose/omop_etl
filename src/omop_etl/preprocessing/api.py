from pathlib import Path
from typing import Optional, List
import copy

from .core.pipeline import PreprocessingPipeline, PreprocessResult
from .core.io_export import OutputManager
from .core.config_loader import load_ecrf_config
from .core.models import EcrfConfig, PreprocessingRunOptions
from .core.dispatch import resolve_processor as _resolve_processor, list_trials as _list_trials

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
    output: Optional[Path] = None,
    fmt: Optional[str] = None,
    combine_key: str = "SubjectId",
    base_output_dir: Optional[Path] = None,
) -> PreprocessResult:
    """High-level API for preprocessing trial data."""
    # resolve processor explicitly
    processor = _resolve_processor(trial)

    if config is None:
        config = make_ecrf_config(trial)
    else:
        config = copy.deepcopy(config)

    output_manager = OutputManager(base_dir=base_output_dir)
    pipeline = PreprocessingPipeline(
        trial=trial,
        ecrf_config=config,
        output_manager=output_manager,
        processor=processor,
    )

    return pipeline.run(
        input_path=input_path,
        options=run_options,
        output=output,
        format=fmt,
        combine_key=combine_key,
    )
