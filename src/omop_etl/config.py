import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parents[4]
DATA_ROOT = Path(os.getenv("DATA_ROOT", PROJECT_ROOT / ".data"))
SYNTHETIC_DATA = DATA_ROOT / "synthetic"

# debug for dev, info for prod
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# base synthetic input
IMPRESS_150 = SYNTHETIC_DATA / "impress_150"
IMPRESS_NON_V600 = SYNTHETIC_DATA / "nonv600_cohorts"
IMPRESS_1K = SYNTHETIC_DATA / "impress_1k"
