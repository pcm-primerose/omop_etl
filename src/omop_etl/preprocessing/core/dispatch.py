from typing import Callable, Dict
import polars as pl

from omop_etl.preprocessing.sources.impress import preprocess_impress
from omop_etl.preprocessing.core.models import (
    EcrfConfig,
    PreprocessingRunOptions,
)

Processor = Callable[[pl.DataFrame, EcrfConfig, PreprocessingRunOptions], pl.DataFrame]

_TRIALS: Dict[str, Processor] = {
    "impress": preprocess_impress,
    # "other": other_preprocess_func,
}


def resolve_preprocessor(trial: str) -> Processor:
    key = trial.lower()
    try:
        return _TRIALS[key]
    except KeyError:
        raise KeyError(f"No processor for trial '{trial}'. Known: {', '.join(sorted(_TRIALS)) or '(none)'}")


def list_trials() -> list[str]:
    return sorted(_TRIALS.keys())
