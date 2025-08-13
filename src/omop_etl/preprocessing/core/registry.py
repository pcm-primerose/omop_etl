from typing import Callable, Dict
import polars as pl

from .models import EcrfConfig, RunOptions

Processor = Callable[[pl.DataFrame, EcrfConfig, RunOptions], pl.DataFrame]
TRIAL_PROCESSORS: Dict[str, Processor] = {}


def register_trial(name: str):
    def deco(fn: Processor) -> Processor:
        if name in TRIAL_PROCESSORS:
            raise KeyError(f"Processor '{name}' already registered")
        TRIAL_PROCESSORS[name] = fn
        return fn

    return deco
