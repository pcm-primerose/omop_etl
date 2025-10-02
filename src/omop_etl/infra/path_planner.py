# infra/paths.py
from dataclasses import dataclass
from pathlib import Path

from .types import Layout


@dataclass(frozen=True)
class WriterContext:
    base_dir: Path
    data_path: Path
    data_dir: Path
    manifest_path: Path
    log_path: Path


def _ext(fmt: str) -> str:
    return {"csv": ".csv", "tsv": ".tsv", "parquet": ".parquet", "json": ".json", "ndjson": ".ndjson"}[fmt]


def plan_paths(*, base_out: Path, module: str, trial: str, run_id: str, stem: str, fmt: str, layout: Layout, started_at: str) -> WriterContext:
    trial = trial.lower()
    if layout == Layout.TRIAL_ONLY:
        root = base_out / module / trial
    elif layout == Layout.TRIAL_RUN:
        root = base_out / module / trial / run_id
    else:
        root = base_out / module / trial / f"{run_id}_{started_at}"
    fmt_root = root / stem / fmt
    return WriterContext(
        base_dir=fmt_root,
        data_path=fmt_root / f"data_{stem}{_ext(fmt)}",
        data_dir=fmt_root,
        manifest_path=fmt_root / f"manifest_{stem}.json",
        log_path=fmt_root / f"{stem}.log",
    )
