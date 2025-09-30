from dataclasses import dataclass
from datetime import datetime, timezone
import uuid


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

    @property
    def artifact_dir(self) -> str:
        return f"{self.trial.lower()}/{self.timestamp}_{self.run_id}"
