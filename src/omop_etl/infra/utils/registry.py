# from importlib import import_module
# from typing import Callable, Dict, TYPE_CHECKING
# import polars as pl
#
# if TYPE_CHECKING:
#     from omop_etl.preprocessing.core.models import EcrfConfig, PreprocessingRunOptions
#     Processor = Callable[[pl.DataFrame, EcrfConfig, PreprocessingRunOptions], pl.DataFrame]
# else:
#     Processor = Callable[..., pl.DataFrame]
#
# TRIAL_PROCESSORS: Dict[str, Processor] = {}
# TRIAL_LOADERS: Dict[str, str] = {} # name = "pkg.mod:func"
#
# def list_trials() -> list[str]:
#     # public API used by preprocessing.api
#     return sorted(set(TRIAL_LOADERS.keys()) | set(TRIAL_PROCESSORS.keys()))
#
# def register_trial_loader(name: str, dotted: str) -> None:
#     """Register a lazy loader for a trial without importing the module"""
#     TRIAL_LOADERS[name.lower()] = dotted
#
# def _load(dotted: str) -> Processor:
#     mod, func = dotted.split(":")
#     return getattr(import_module(mod), func)
#
# def get_trial_processor(name: str) -> Processor:
#     key = name.lower()
#     if key in TRIAL_PROCESSORS:
#         return TRIAL_PROCESSORS[key]
#     if key in TRIAL_LOADERS:
#         fn = _load(TRIAL_LOADERS[key])
#         TRIAL_PROCESSORS[key] = fn
#         return fn
#     raise KeyError(f"No processor for trial '{name}'. Registered: "
#                    f"{', '.join(sorted(set(TRIAL_PROCESSORS)|set(TRIAL_LOADERS))) or '(none)'}")
#
# def clear_registry() -> None:
#     TRIAL_PROCESSORS.clear()
