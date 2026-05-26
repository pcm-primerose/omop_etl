import logging
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
)

logger = logging.getLogger(__name__)


@dataclass
class OmopTables:
    """
    Container for built OMOP tables.

    Supports dynamic table building via extend/add methods,
    with typed property accessors for known tables.

    Deduplication is automatic for rows that define a natural_key() method.
    Rows with duplicate natural keys are logged and skipped.

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

    _tables: dict[str, list[Any]] = field(default_factory=dict)
    _seen_keys: dict[str, set[tuple]] = field(default_factory=dict, repr=False)

    @classmethod
    def values(cls) -> set[str]:
        """Set of all known OMOP table-name constants."""
        return {v for k, v in vars(cls).items() if not k.startswith("_") and isinstance(v, str)}

    def extend(self, table_name: str, rows: list[Any]) -> None:
        """Extend a table with multiple rows, deduplicating by natural key."""
        table = self._tables.setdefault(table_name, [])
        seen = self._seen_keys.setdefault(table_name, set())

        for row in rows:
            if hasattr(row, "natural_key"):
                key = row.natural_key()
                if key in seen:
                    self._log_duplicate(table_name, row, key)
                    continue
                seen.add(key)
            table.append(row)

    def add(self, table_name: str, row: Any) -> None:
        """Add a single row to a table, deduplicating by natural key."""
        table = self._tables.setdefault(table_name, [])
        seen = self._seen_keys.setdefault(table_name, set())

        if hasattr(row, "natural_key"):
            key = row.natural_key()
            if key in seen:
                self._log_duplicate(table_name, row, key)
                return
            seen.add(key)
        table.append(row)

    @staticmethod
    def _log_duplicate(table_name: str, row: Any, key: tuple) -> None:
        """Log a duplicate row with context for debugging."""
        key_fields = getattr(row, "natural_key_fields", ())
        key_dict = dict(zip(key_fields, key))

        # include source field if available for provenance
        source_field = getattr(row, "_source_field", None)
        if source_field:
            logger.warning(
                "Duplicate row skipped in %s: %s (from %s)",
                table_name,
                key_dict,
                source_field,
            )
        else:
            logger.warning("Duplicate row skipped in %s: %s", table_name, key_dict)

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
