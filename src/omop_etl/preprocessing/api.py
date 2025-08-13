from __future__ import annotations
from pathlib import Path
from typing import Optional, List, cast
import copy
import polars as pl

from .core.pipeline import run_pipeline
from .core.io import write_output, write_manifest
from .core.registry import TRIAL_PROCESSORS
from .core.config_loader import load_ecrf_config

# re-export types
from .core.models import (
    EcrfConfig,
    RunOptions,
    OutputSpec,
    PreprocessResult,
    OutputFormat,
)

__all__ = [
    "preprocess_trial",
    "make_ecrf_config",
    "list_trials",
    "EcrfConfig",
    "RunOptions",
    "OutputSpec",
    "PreprocessResult",
    "OutputFormat",
]

_ALLOWED_OUT_FMT: set[OutputFormat] = {"csv", "tsv", "parquet"}


def list_trials() -> List[str]:
    return sorted(TRIAL_PROCESSORS.keys())


def make_ecrf_config(
    trial: str, custom_config_path: Optional[Path] = None
) -> EcrfConfig:
    cfg_map = load_ecrf_config(custom_config_path=custom_config_path, trial=trial)
    return EcrfConfig.from_mapping(cfg_map)


def _infer_fmt_from_output(path: Path) -> Optional[OutputFormat]:
    suf = path.suffix.lower().lstrip(".")
    if suf == "txt":
        suf = "tsv"
    return cast(Optional[OutputFormat], suf if suf in _ALLOWED_OUT_FMT else None)


def _normalize_fmt(fmt: Optional[str]) -> OutputFormat:
    f = (fmt or "csv").strip().lower()
    if f == "txt":
        f = "tsv"
    if f not in _ALLOWED_OUT_FMT:
        raise ValueError(
            f"Unsupported format '{fmt}'. Supported: {', '.join(sorted(_ALLOWED_OUT_FMT))}."
        )
    return cast(OutputFormat, f)


def _build_output_spec(
    trial: str,
    output: Optional[Path],
    fmt: Optional[str],
    default_dir: Path,
) -> OutputSpec:
    if output:
        inferred = _infer_fmt_from_output(output)
        if inferred:
            # explicit path
            return OutputSpec(
                base_dir=output.parent,
                fmt=inferred,
                subdir="",
                filename=output.stem,
            )
        # dir: timestamped and run id
        return OutputSpec(
            base_dir=output,
            fmt=_normalize_fmt(fmt),
            subdir="{trial}/{ts}_{run_id}",
            filename="preprocessed",
        )
    # no output: default dir + timestamped subfolders
    return OutputSpec(
        base_dir=default_dir,
        fmt=_normalize_fmt(fmt),
        subdir="{trial}/",
        filename="{ts}_{run_id}_preprocessed",
    )


def preprocess_trial(
    trial: str,
    input_path: Path,
    cfg: EcrfConfig,
    run_opts: Optional[RunOptions] = None,
    out: Optional[OutputSpec] = None,
    output: Optional[Path] = None,
    fmt: Optional[str] = None,
    combine_on: str = "SubjectId",
    default_dir: Path = Path(".data/preprocessing"),
) -> PreprocessResult:
    ecfg = copy.deepcopy(cfg)
    df: pl.DataFrame = run_pipeline(
        trial=trial,
        input_path=input_path,
        ecfg=ecfg,
        combine_on=combine_on,
        run_opts=run_opts or RunOptions(),
    )
    spec = out or _build_output_spec(trial, output, fmt, default_dir)
    path = write_output(df, trial, spec)
    write_manifest(path, trial, input_path, df, run_opts)
    return PreprocessResult(path=path, rows=df.height, cols=df.width)
