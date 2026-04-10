from dataclasses import dataclass
import datetime as dt

from src.omop_etl.harmonization.models.patient import Patient


@dataclass(slots=True, frozen=True)
class VisitEvent:
    date: dt.date
    key_parts: str | None
    source_value: str | None


def iter_visit_event(patient: Patient) -> VisitEvent:
    pass
