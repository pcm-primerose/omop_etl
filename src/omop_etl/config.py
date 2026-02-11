import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parents[4]
DATA_ROOT = Path(os.getenv("DATA_ROOT", PROJECT_ROOT / ".data"))
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


ACTIVE_DATASET = resolve_dataset(os.getenv("SYNTHETIC_DATASET", "impress_150"))
