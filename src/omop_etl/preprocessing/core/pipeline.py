from pathlib import Path
from typing import Optional
import polars as pl

from .models import EcrfConfig, RunOptions
from .io import resolve_input
from .combine import combine
from .registry import TRIAL_PROCESSORS


def run_pipeline(
    trial: str,
    input_path: Path,
    ecfg: EcrfConfig,
    run_opts: Optional[RunOptions] = None,
    combine_on: str = "SubjectId",
) -> pl.DataFrame:
    if trial not in TRIAL_PROCESSORS:
        raise ValueError(
            f"Unknown source '{trial}'. Available: {', '.join(TRIAL_PROCESSORS)}"
        )

    ecfg.trial = trial
    ecfg = resolve_input(input_path, ecfg)
    df = combine(ecfg, on=combine_on)

    # source-specific processing
    df = TRIAL_PROCESSORS[trial](df, ecfg, run_opts)

    return df
