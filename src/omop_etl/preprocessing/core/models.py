# core/models.py
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Mapping, Sequence, Literal
import polars as pl


@dataclass(frozen=True)
class SheetConfig:
    key: str
    usecols: Sequence[str]


@dataclass
class SheetData:
    key: str
    data: pl.DataFrame
    input_path: Optional[Path] = None


@dataclass
class EcrfConfig:
    configs: list[SheetConfig]
    data: list[SheetData] | None = None
    trial: str | None = None
    source_type: str | None = None

    @classmethod
    def from_mapping(cls, m: Mapping[str, list[str]]) -> "EcrfConfig":
        return cls([SheetConfig(key=k.upper(), usecols=v) for k, v in m.items()])


@dataclass(frozen=True)
class RunOptions:
    filter_valid_cohort: bool = False


OutputFormat = Literal["csv", "tsv", "parquet"]


@dataclass(frozen=True)
class OutputPath:
    """Resolved output paths for data and manifest."""

    data_file: Path
    manifest_file: Path
    log_file: Path
    directory: Path
    format: OutputFormat


@dataclass(frozen=True)
class RunContext:
    """Immutable context for a preprocessing run."""

    trial: str
    timestamp: str
    run_id: str

    @classmethod
    def create(cls, trial: str) -> "RunContext":
        """Create a new run context with generated timestamp and ID."""
        return cls(
            trial=trial,
            timestamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            run_id=uuid.uuid4().hex[:8],
        )

    def as_dict(self) -> dict[str, str]:
        """Convert dict for logging/serialization."""
        return {"trial": self.trial, "timestamp": self.timestamp, "run_id": self.run_id}


@dataclass(frozen=True)
class PreprocessResult:
    """Result of preprocessing operation."""

    output_path: OutputPath
    rows: int
    columns: int
    context: RunContext
