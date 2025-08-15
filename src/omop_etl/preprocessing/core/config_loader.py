# core/config_loader.py
from __future__ import annotations
from importlib.resources import files
from pathlib import Path
from typing import Optional
import json5 as json

_BASE = files("omop_etl.resources") / "ecrf_variables"


def load_ecrf_config(trial: str, custom_config_path: Optional[Path] = None) -> dict:
    if custom_config_path:
        config_path = Path(custom_config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Custom config file not found: {config_path}")
        return _validate(json.loads(config_path.read_text()))

    resources_config = _BASE / f"{trial.lower()}.json5"
    if not resources_config.exists():  # type: ignore
        raise FileNotFoundError(
            f"No packaged config for trial '{trial}'. "
            f"Expected: omop_etl/resources/ecrf_variables/{trial.lower()}.json5. "
            f"Available: {', '.join(available_trials()) or 'none'}"
        )
    with resources_config.open("r") as f:
        return _validate(json.load(f))


def available_trials() -> list[str]:
    return sorted(
        p.name.strip(".json5") for p in _BASE.iterdir() if p.name.endswith(".json5")
    )


def _validate(config: dict) -> dict:
    if not isinstance(config, dict) or not all(
        isinstance(v, list) for v in config.values()
    ):
        raise ValueError("Config must be a mapping[str, list[str]].")
    return config
