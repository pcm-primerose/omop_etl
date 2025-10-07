from importlib.resources import files as pkg_files
from pathlib import Path
from typing import Optional
import json5 as json
from logging import getLogger

log = getLogger(__name__)

_BASE = pkg_files("omop_etl.resources") / "ecrf_variables"


def load_ecrf_config(trial: str, custom_config_path: Optional[Path] = None) -> dict:
    if custom_config_path:
        config_path = Path(custom_config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Custom config file not found: {config_path}")
        return _validate(json.loads(config_path.read_text(encoding="utf-8")))

    path = _find_config_path(trial, _BASE)
    if not path:
        avail = ", ".join(available_trials()) or "none"
        raise FileNotFoundError(f"No packaged config for trial '{trial}'. Expected one of: {avail}")

    with path.open("r", encoding="utf-8") as f:  # type: ignore
        return _validate(json.load(f))


def _find_config_path(trial: str, base) -> Optional[object]:
    """Return a Traversable/Path for the matching JSON5, case-insensitive."""
    target_name = f"{trial.casefold()}.json5"

    cand = base / target_name
    try:
        if cand.is_file():
            return cand
    except AttributeError:
        pass

    for p in base.iterdir():
        name = getattr(p, "name", None)
        if not name or not name.endswith(".json5"):
            continue
        if name.casefold() == target_name:
            return p
    return None


def available_trials() -> list[str]:
    out = sorted(p.name.removesuffix(".json5") for p in _BASE.iterdir() if p.name.endswith(".json5"))
    log.info("Available trials: {out}")
    return out


def _validate(config: dict) -> dict:
    if not isinstance(config, dict) or not all(isinstance(v, list) for v in config.values()):
        raise ValueError("Config must be a mapping[str, list[str]].")
    return config
