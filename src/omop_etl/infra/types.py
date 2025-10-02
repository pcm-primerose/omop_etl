from enum import Enum
from typing import Literal


class OutputMode(str, Enum):
    WIDE = "wide"
    NORMALIZED = "normalized"


class Layout(str, Enum):
    TRIAL_ONLY = "trial_only"
    TRIAL_RUN = "trial_run"
    TRIAL_TIMESTAMP_RUN = "trial_timestamp_run"


Format = Literal["csv", "tsv", "parquet", "json", "ndjson"]
TABULAR_FORMATS: set[str] = {"csv", "tsv", "parquet"}
WIDE_FORMATS: set[str] = {"csv", "tsv", "parquet", "json", "ndjson"}
NORMALIZED_FORMATS: set[str] = {"csv", "tsv", "parquet"}
