from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

from omop_etl.infra.io.format_utils import ext
from omop_etl.infra.io.path_utils import run_segment
from omop_etl.infra.io.types import Layout, Format


@dataclass(frozen=True)
class WriterContext:
    base_dir: Path
    data_path: Path
    data_dir: Path
    manifest_path: Path
    log_path: Path


def plan_paths(
    base_out: Path,
    module: Optional[str],
    trial: str,
    run_id: str,
    stem: str,
    fmt: Format,
    layout: Layout,
    started_at: str,
    include_stem_dir: bool = True,
    name_template: Optional[str] = None,
    template_vars: Optional[Dict[str, str]] = None,
) -> WriterContext:
    """
    Default layout:
      base/module/trial/run-seg/stem/fmt/
        data_{stem}.{ext}
        manifest_{stem}.json
        {stem}.log

    With name_template set (e.g. {trial}_{run_id}_{started_at}_{mode}):
      base/module/trial/run-seg/fmt/        (if include_stem_dir=False)
        templ.{ext}
        templ_manifest.json
        templ.log
    """
    trial = trial.lower()

    root = base_out
    if module:
        root = root / module
    root = root / trial

    seg = run_segment(layout, run_id, started_at)
    if seg:
        root = root / seg

    # optionally include stem layer
    fmt_root = root / fmt if not include_stem_dir else root / stem / fmt

    # filename base (shared by data/log/manifest)
    if name_template:
        # stem available as mode by default
        vars_ = {
            "trial": trial,
            "run_id": run_id,
            "started_at": started_at,
            "mode": stem,
        }
        if template_vars:
            vars_.update(template_vars)
        base_name = name_template.format(**vars_)
        data_base = base_name
        manifest_base = f"{base_name}_manifest"
        log_base = base_name
    else:
        data_base = f"data_{stem}" if stem else "data"
        manifest_base = f"manifest_{stem}" if stem else "manifest"
        log_base = stem or "run"

    return WriterContext(
        base_dir=fmt_root,
        data_path=fmt_root / f"{data_base}{ext(fmt)}",
        data_dir=fmt_root,
        manifest_path=fmt_root / f"{manifest_base}.json",
        log_path=fmt_root / f"{log_base}.log",
    )
