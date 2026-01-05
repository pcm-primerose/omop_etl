from typing import Dict
from ..harmonizers.base import BaseHarmonizer
from ..harmonizers.impress import ImpressHarmonizer

_TRIALS: Dict[str, type[BaseHarmonizer]] = {
    "impress": ImpressHarmonizer,
    # "other": OtherHarmonizer,
}


def resolve_harmonizer(trial: str) -> type[BaseHarmonizer]:
    key = trial.lower()
    try:
        return _TRIALS[key]
    except KeyError:
        raise KeyError(f"No harmonizer for trial: {trial}. Supported trials: {', '.join(sorted(_TRIALS)) or '(none)'}")


def list_trials() -> list[str]:
    return sorted(_TRIALS.keys())
