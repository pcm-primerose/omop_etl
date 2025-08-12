from typing import Callable, Dict
import polars as pl
from .types import EcrfConfig

Processor = Callable[[pl.DataFrame, EcrfConfig], pl.DataFrame]
SOURCES: Dict[str, Processor] = {}


def register(name: str):
    def deco(fn: Processor) -> Processor:
        SOURCES[name] = fn
        return fn

    return deco
