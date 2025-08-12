from pathlib import Path
import polars as pl
from .types import EcrfConfig
from .io import resolve_input
from .combine import combine
from .registry import SOURCES


def run_pipeline(
    source: str,
    input_path: Path,
    cfg: EcrfConfig,
    combine_on: str = "SubjectId",
    pre=None,
    post=None,
) -> pl.DataFrame:
    if source not in SOURCES:
        raise ValueError(f"Unknown source '{source}'. Available: {', '.join(SOURCES)}")

    cfg.trial = source
    cfg = resolve_input(input_path, cfg)
    df = combine(cfg, on=combine_on)
    if pre:
        df = pre(df, cfg)

    # call source-specific function
    df = SOURCES[source](df, cfg)
    if post:
        df = post(df, cfg)
    return df
