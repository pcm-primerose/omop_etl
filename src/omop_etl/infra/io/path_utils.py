from typing import Optional

from omop_etl.infra.io.types import Layout


def run_segment(layout: Layout, run_id: str, started_at: str) -> Optional[str]:
    if layout is Layout.TRIAL_ONLY:
        return None
    if layout is Layout.TRIAL_RUN:
        return run_id
    if layout is Layout.TRIAL_TIMESTAMP_RUN:
        return f"{started_at}_{run_id}"
    return run_id
