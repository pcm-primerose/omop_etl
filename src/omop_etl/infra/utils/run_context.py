from dataclasses import dataclass
import uuid
from datetime import (
    datetime,
    timezone,
)

from omop_etl.infra.io.types import RunSource


@dataclass(frozen=True)
class RunMetadata:
    trial: str
    run_id: str
    started_at: str
    source: RunSource | None = None
    user: str | None = None

    @classmethod
    def create(cls, trial: str, run_id: str | None = None) -> RunMetadata:
        return cls(
            trial=trial,
            run_id=run_id or uuid.uuid4().hex[:8],
            started_at=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        )

    def as_dict(self) -> dict:
        return {
            "trial": self.trial,
            "run_id": self.run_id,
            "started_at": self.started_at,
            **({"user": self.user} if self.user else {}),
            **({"source": self.source} if self.source else {}),
        }
