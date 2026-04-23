import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def _find_project_root() -> Path:
    """Walk up from this file until pyproject.toml is found."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError(f"Could not find pyproject.toml walking up from {Path(__file__).resolve()}")


PROJECT_ROOT = _find_project_root()


def _resolve_data_root() -> Path:
    """DATA_ROOT from env, resolved relative to PROJECT_ROOT if the env value is a
    relative path. Absolute paths are used as-is. Falls back to PROJECT_ROOT / .data."""
    env_val = os.getenv("DATA_ROOT")
    if env_val is None:
        return PROJECT_ROOT / ".data"
    p = Path(env_val)
    return p if p.is_absolute() else PROJECT_ROOT / p


DATA_ROOT = _resolve_data_root()
SYNTHETIC_DATA = DATA_ROOT / "synthetic"

# debug for dev, info for prod
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# synthetic dataset registry: short name -> path
SYNTHETIC_DATASETS: dict[str, Path] = {
    "impress_150": SYNTHETIC_DATA / "impress_150",
    "impress_1k": SYNTHETIC_DATA / "impress_1k",
    "impress_nonv600": SYNTHETIC_DATA / "impress_nonv600",
}


def resolve_dataset(name_or_path: str) -> Path:
    """Look up registry name, fallback treats value as path"""
    if name_or_path in SYNTHETIC_DATASETS:
        return SYNTHETIC_DATASETS[name_or_path]
    return Path(name_or_path)


# used when no --dataset arg is passed on the command line
DEFAULT_DATASET = SYNTHETIC_DATASETS["impress_150"]
