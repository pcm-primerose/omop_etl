import logging
from dataclasses import dataclass, field
from typing import Any

from omop_etl.omop.models.rows import (
    PersonRow,
    ObservationPeriodRow,
    CdmSourceRow,
    VisitOccurrenceRow,
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
    This does not reflect the OMOP CDM spec but
    """

    _tables: dict[str, list[Any]] = field(default_factory=dict)
    _seen_keys: dict[str, set[tuple]] = field(default_factory=dict, repr=False)

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
        return self._tables.get("person", [])

    @property
    def observation_period(self) -> list[ObservationPeriodRow]:
        return self._tables.get("observation_period", [])

    @property
    def cdm_source(self) -> CdmSourceRow | None:
        rows = self._tables.get("cdm_source", [])
        return rows[0] if rows else None

    @property
    def visit_occurrence(self) -> list[VisitOccurrenceRow] | None:
        return self._tables.get("visit_occurrence", [])
