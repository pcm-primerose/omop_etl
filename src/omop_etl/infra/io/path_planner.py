from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Literal

from omop_etl.infra.io.format_utils import ext
from omop_etl.infra.io.types import TabularFormat, WideFormat
from omop_etl.infra.utils.run_context import RunMetadata

Mode = Literal["preprocessed", "wide", "normalized", "harmonized_norm", "harmonized_wide", "semantic_mapped"]


@dataclass(frozen=True)
class WriterContext:
    base_dir: Path
    data_path: Path
    data_dir: Path
    manifest_path: Path
    log_path: Path


def run_root(base_out: Path, meta: RunMetadata) -> Path:
    """Canonical run root: <base>/runs/<started_at>_<run_id>"""
    return base_out / "runs" / f"{meta.started_at}_{meta.run_id}"


def _fmt_dir(base_out: Path, meta: RunMetadata, module: str, trial: str, mode: Mode, fmt: str) -> Path:
    return run_root(base_out, meta) / module / trial.lower() / mode / fmt


def _fill_stem(
    trial: str,
    meta: RunMetadata,
    mode: Mode,
    filename_base: str,
    extra_vars: Optional[Dict[str, str]] = None,
) -> str:
    vars_ = {
        "trial": trial.lower(),
        "run_id": meta.run_id,
        "started_at": meta.started_at,
        "mode": mode,
    }
    if extra_vars:
        vars_.update(extra_vars)
    try:
        return filename_base.format(**vars_)
    except KeyError as e:
        raise ValueError(f"Missing template key '{e.args[0]}' in filename_base: {filename_base!r}") from e


def plan_single_file(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
    mode: Mode,
    fmt: WideFormat,
    filename_base: str = "{trial}_{run_id}_{started_at}_{mode}",
    extra_vars: Optional[Dict[str, str]] = None,
) -> WriterContext:
    base_dir = _fmt_dir(base_out, meta, module, trial, mode, fmt)
    base_dir.mkdir(parents=True, exist_ok=True)

    stem = _fill_stem(trial=trial, meta=meta, mode=mode, filename_base=filename_base, extra_vars=extra_vars)

    data_path = base_dir / f"{stem}{ext(fmt)}"
    manifest_path = base_dir / f"{stem}_manifest.json"
    log_path = base_dir / f"{stem}.log"

    return WriterContext(
        base_dir=base_dir,
        data_path=data_path,
        data_dir=base_dir,
        manifest_path=manifest_path,
        log_path=log_path,
    )


def plan_table_dir(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
    mode: Mode,
    fmt: TabularFormat,
    filename_base: str = "{trial}_{run_id}_{started_at}_{mode}",
    extra_vars: Optional[Dict[str, str]] = None,
) -> WriterContext:
    base_dir = _fmt_dir(base_out, meta, module, trial, mode, fmt)
    base_dir.mkdir(parents=True, exist_ok=True)

    stem = _fill_stem(trial=trial, meta=meta, mode=mode, filename_base=filename_base, extra_vars=extra_vars)

    # data_path is a sentinel, not used by writers
    data_path = base_dir / f"{stem}{ext(fmt)}"
    manifest_path = base_dir / f"{stem}_manifest.json"
    log_path = base_dir / f"{stem}.log"

    return WriterContext(
        base_dir=base_dir,
        data_path=data_path,
        data_dir=base_dir,
        manifest_path=manifest_path,
        log_path=log_path,
    )
