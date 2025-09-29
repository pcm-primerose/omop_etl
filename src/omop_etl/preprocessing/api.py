from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import copy

from .core.pipeline import PreprocessingPipeline, PreprocessResult
from .core.io_export import OutputManager
from .core.config_loader import load_ecrf_config
from .core.registry import TRIAL_PROCESSORS
from .core.models import EcrfConfig, RunOptions

__all__ = [
    "preprocess_trial",
    "make_ecrf_config",
    "list_trials",
    "PreprocessResult",
    "RunOptions",
]


def list_trials() -> List[str]:
    """List available trial processors."""
    return sorted(TRIAL_PROCESSORS.keys())


def make_ecrf_config(trial: str, custom_config_path: Optional[Path] = None) -> EcrfConfig:
    cfg_map = load_ecrf_config(trial=trial, custom_config_path=custom_config_path)
    return EcrfConfig.from_mapping(cfg_map)


def preprocess_trial(
    trial: str,
    input_path: Path,
    config: Optional[EcrfConfig] = None,
    run_options: Optional[RunOptions] = None,
    output: Optional[Path] = None,
    fmt: Optional[str] = None,
    combine_key: str = "SubjectId",
    base_output_dir: Optional[Path] = None,
) -> PreprocessResult:
    """
    High-level API for preprocessing trial data.

    Args:
        trial: Trial name
        input_path: Input Excel or CSV directory
        config: Optional eCRF config (creates default if None)
        run_options: Optional processing options
        output: Optional output path
        fmt: Optional output format
        combine_key: Key for joining sheets
        base_output_dir: Optional base directory for outputs

    Returns:
        PreprocessResult with output details
    """
    # create config if not provided
    if config is None:
        config = make_ecrf_config(trial)
    else:
        config = copy.deepcopy(config)

    # create pipeline with output manager
    output_manager = OutputManager(base_dir=base_output_dir)
    pipeline = PreprocessingPipeline(trial=trial, ecrf_config=config, output_manager=output_manager)

    # run pipeline
    return pipeline.run(
        input_path=input_path,
        options=run_options,
        output=output,
        format=fmt,
        combine_key=combine_key,
    )
