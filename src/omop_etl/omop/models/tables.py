from dataclasses import dataclass, field
from typing import Any, ClassVar

from omop_etl.omop.models.rows import (
    PersonRow,
    ObservationPeriodRow,
    ObservationRow,
    CdmSourceRow,
    VisitOccurrenceRow,
    DrugExposureRow,
    ConditionOccurrenceRow,
    ProcedureOccurrenceRow,
    MeasurementRow,
    DeathRow,
    EpisodeRow,
    EpisodeEventRow,
)


@dataclass
class OmopTables:
    """
    Container for built OMOP tables.

    Supports dynamic table building via extend/add methods,
    with typed property accessors for known tables.

    Uniqueness is not enforced here. Each builder guarantees its own row-id
    uniqueness (deterministic content hashes from generate_row_id), the
    harmonizer dedups source records per patient, and the DB primary key is the
    final authority. This container just collects rows.

    A field of a given row used to build tables is considered required if not optional.

    Class-level constants (CDM_SOURCE, PERSON, ...) are the canonical OMOP
    table names used by builders' `table_name` class var, by `OmopRowRef.table`,
    and by `BuildContext.publish_rows / resolve_rows` calls.
    """

    CDM_SOURCE: ClassVar[str] = "cdm_source"
    CONDITION_OCCURRENCE: ClassVar[str] = "condition_occurrence"
    DRUG_EXPOSURE: ClassVar[str] = "drug_exposure"
    MEASUREMENT: ClassVar[str] = "measurement"
    OBSERVATION: ClassVar[str] = "observation"
    OBSERVATION_PERIOD: ClassVar[str] = "observation_period"
    PERSON: ClassVar[str] = "person"
    PROCEDURE_OCCURRENCE: ClassVar[str] = "procedure_occurrence"
    VISIT_OCCURRENCE: ClassVar[str] = "visit_occurrence"
    DEATH: ClassVar[str] = "death"
    EPISODE: ClassVar[str] = "episode"
    EPISODE_EVENT: ClassVar[str] = "episode_event"

    _tables: dict[str, list[Any]] = field(default_factory=dict)

    @classmethod
    def values(cls) -> set[str]:
        """Set of all known OMOP table-name constants."""
        return {v for k, v in vars(cls).items() if not k.startswith("_") and isinstance(v, str)}

    def extend(self, table_name: str, rows: list[Any]) -> None:
        """Append multiple rows to a table."""
        self._tables.setdefault(table_name, []).extend(rows)

    def add(self, table_name: str, row: Any) -> None:
        """Append a single row to a table."""
        self._tables.setdefault(table_name, []).append(row)

    def __getitem__(self, table_name: str) -> list[Any]:
        """Get rows by table name."""
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' not found")
        return self._tables[table_name]

    def get(self, table_name: str, default: list[Any] | None = None) -> list[Any]:
        """Get rows by table name with default."""
        return self._tables.get(table_name, default if default is not None else [])

    @property
    def person(self) -> list[PersonRow]:
        return self._tables.get(self.PERSON, [])

    @property
    def observation_period(self) -> list[ObservationPeriodRow]:
        return self._tables.get(self.OBSERVATION_PERIOD, [])

    @property
    def cdm_source(self) -> CdmSourceRow | None:
        rows = self._tables.get(self.CDM_SOURCE, [])
        return rows[0] if rows else None

    @property
    def visit_occurrence(self) -> list[VisitOccurrenceRow]:
        return self._tables.get(self.VISIT_OCCURRENCE, [])

    @property
    def drug_exposure(self) -> list[DrugExposureRow]:
        return self._tables.get(self.DRUG_EXPOSURE, [])

    @property
    def condition_occurrence(self) -> list[ConditionOccurrenceRow]:
        return self._tables.get(self.CONDITION_OCCURRENCE, [])

    @property
    def procedure_occurrence(self) -> list[ProcedureOccurrenceRow]:
        return self._tables.get(self.PROCEDURE_OCCURRENCE, [])

    @property
    def measurement(self) -> list[MeasurementRow]:
        return self._tables.get(self.MEASUREMENT, [])

    @property
    def observation(self) -> list[ObservationRow]:
        return self._tables.get(self.OBSERVATION, [])

    @property
    def death(self) -> list[DeathRow]:
        return self._tables.get(self.DEATH, [])

    @property
    def episode(self) -> list[EpisodeRow]:
        return self._tables.get(self.EPISODE, [])

    @property
    def episode_event(self) -> list[EpisodeEventRow]:
        return self._tables.get(self.EPISODE_EVENT, [])
