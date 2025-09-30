from typing import Callable, Dict, List
import polars as pl

from .models import EcrfConfig, PreprocessingRunOptions

Processor = Callable[[pl.DataFrame, EcrfConfig, PreprocessingRunOptions], pl.DataFrame]
TRIAL_PROCESSORS: Dict[str, Processor] = {}


def register_trial(name: str):
    def deco(fn: Processor) -> Processor:
        # normalize to lowercase
        normalized_name = name.lower()
        if normalized_name in TRIAL_PROCESSORS:
            raise KeyError(f"Processor '{name}' already registered")
        TRIAL_PROCESSORS[normalized_name] = fn
        return fn

    return deco


def get_registered_trials() -> List[str]:
    """Get list of registered trial processors"""
    return sorted(TRIAL_PROCESSORS.keys())


def get_processor(trial: str) -> Processor:
    """Get processor for a trial (case-insensitive)"""
    normalized_trial = trial.lower()
    if normalized_trial not in TRIAL_PROCESSORS:
        available = ", ".join(get_registered_trials())
        raise KeyError(f"No processor for trial '{trial}'. Available: {available}")

    return TRIAL_PROCESSORS[normalized_trial]


def clear_registry():
    """Clear registry"""
    TRIAL_PROCESSORS.clear()
