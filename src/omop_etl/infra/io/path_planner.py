from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Literal

from omop_etl.infra.io.format_utils import ext
from omop_etl.infra.io.types import TabularFormat, WideFormat
from omop_etl.infra.utils.run_context import RunMetadata

Mode = Literal["preprocessed", "wide", "normalized", "harmonized_norm", "harmonized_wide"]


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


def _module_dir(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
) -> Path:
    return run_root(base_out, meta) / module / trial.lower()


def _mode_dir(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
    mode: Mode,
) -> Path:
    return _module_dir(base_out, meta, module, trial) / mode


def _fmt_dir(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
    mode: Mode,
    fmt: str,
) -> Path:
    return _mode_dir(base_out, meta, module, trial, mode) / fmt


def _stamped_base(
    trial: str,
    meta: RunMetadata,
    mode: Mode,
) -> str:
    # e.g. impress_c593d23d_20251006T114400Z_harmonized_wide
    return f"{trial.lower()}_{meta.run_id}_{meta.started_at}_{('preprocessed' if mode == 'preprocessed' else ('harmonized_' + mode))}"


def plan_single_file(
    base_out: Path,
    meta: RunMetadata,
    module: str,
    trial: str,
    mode: str,
    fmt: WideFormat,
    filename_base: str = "{trial}_{run_id}_{started_at}_{mode}",
    extra_vars: Optional[Dict[str, str]] = None,
) -> WriterContext:
    _run_root = base_out / "runs" / f"{meta.started_at}_{meta.run_id}"

    # normalize & fill template
    vars_ = {
        "trial": trial.lower(),
        "run_id": meta.run_id,
        "started_at": meta.started_at,
        "mode": mode,
    }
    if extra_vars:
        vars_.update(extra_vars)
    try:
        stem = filename_base.format(**vars_)
    except KeyError as e:
        raise ValueError(f"Missing template key '{e.args[0]}' in filename_base: {filename_base!r}") from e

    # leaf dir: module/trial/mode/fmt/
    base_dir = _run_root / module / trial.lower() / mode / fmt
    base_dir.mkdir(parents=True, exist_ok=True)

    suffix = ext(fmt)
    data_path = base_dir / f"{stem}{suffix}"
    manifest_path = base_dir / f"{stem}_manifest.json"
    log_path = base_dir / f"{stem}.log"

    return WriterContext(
        base_dir=base_dir,
        data_path=data_path,
        data_dir=base_dir,
        manifest_path=manifest_path,
        log_path=log_path,
    )


def plan_table_dir(base_out: Path, meta: RunMetadata, module: str, trial: str, mode: Mode, fmt: TabularFormat) -> WriterContext:
    """
    Produce a context for directory of tables (harmonized normalized).
    Directory: <base>/runs/<ts_id>/<module>/<trial>/normalized/<fmt>/
      - per-table files live in data_dir (short names, e.g. patients.csv)
      - manifest_harmonized_norm.json
      - harmonized_norm.log
    """
    base_dir = _fmt_dir(base_out, meta, module, trial, mode, fmt)
    manifest = base_dir / "manifest_harmonized_norm.json"
    log = base_dir / "harmonized_norm.log"
    # data_path is not used by callers for multi-table outputs, set to data_dir / sentinel
    data_path = base_dir / f"{trial.lower()}_{meta.run_id}_{meta.started_at}_{mode}{ext(fmt)}"
    return WriterContext(
        base_dir=base_dir,
        data_path=data_path,
        data_dir=base_dir,
        manifest_path=manifest,
        log_path=log,
    )
