from dataclasses import dataclass
from typing import TypeVar, Hashable, Generic
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.tables import OmopTables

T = TypeVar("T")


def _all_str_values(ns) -> set[str]:
    return {v for k, v in vars(ns).items() if not k.startswith("_") and isinstance(v, str)}


@dataclass(frozen=True, slots=True)
class SourceReference:
    """
    A unique source-domain instance: (patient, source kind,
    natural key). Used as the anchor for cross-builder FK linkage.

    `source_kind` must be a `Patient.Collections.*` / `Patient.Singletons.*`
    constant.
    """

    patient_id: str
    source_kind: str
    natural_key: tuple[Hashable, ...]

    def __post_init__(self):
        valid = _all_str_values(Patient.Collections) | _all_str_values(Patient.Singletons)
        if self.source_kind not in valid:
            raise ValueError(f"Unknown source_kind {self.source_kind!r}: must be one of Patient.Collections.* or Patient.Singletons.* constants")


@dataclass(frozen=True, slots=True)
class OmopRowReference:
    """
    Handle to an OMOP row emitted by a builder, used by
    downstream builders to construct FK linkage without seeing the full row.

    `primary_concept_id` is the row's primary topic concept (e.g.
    `condition_concept_id`), used publisher-side for deterministic
    sort ordering. `0` is valid (CDM convention: unmapped primary).
    `table` must be a `OmopTables.*` constant.

    `event_date` optionally carries the published row's clinical date so
    date-driven consumers can read this instead of recomputing.
    """

    table: str
    row_id: int
    primary_concept_id: int
    event_date: dt.date | None = None

    def __post_init__(self):
        if self.table not in OmopTables.values():
            raise ValueError(f"Unknown OMOP table {self.table!r}, must be one of OmopTables.* constants")


@dataclass(frozen=True, slots=True)
class LinkTarget:
    """
    Resolved target for OMOP event-link columns.

    Represents a real published OMOP row that can be referenced by a consumer
    row's event-link fields.
    """

    event_id: int
    field_concept_id: int


@dataclass(frozen=True, slots=True)
class RowPublication:
    """
    A builder's intent to publish OmopRowReferences from a SourceReference
    into `target_table`. Carried in `BuildResult.publications`. Applied to
    BuildContext by `build_and_populate` / the service orchestrator, not
    by the builder itself.
    """

    target_table: str
    source_ref: SourceReference
    rows: tuple[OmopRowReference, ...]

    def __post_init__(self) -> None:
        if self.target_table not in OmopTables.values():
            raise ValueError(f"Unknown OMOP target_table {self.target_table!r}, must be one of OmopTables.* constants")
        if not self.rows:
            raise ValueError(f"Cannot publish empty row set for {self.target_table} from {self.source_ref}")
        for row in self.rows:
            if row.table != self.target_table:
                raise ValueError(f"Published row table mismatch: target_table={self.target_table}, row.table={row.table}")


@dataclass(frozen=True, slots=True)
class BuildResult(Generic[T]):
    """
    A builder's full output: produced rows plus any RowPublication intents.
    Pure value, builders return it instead of mutating BuildContext. The
    orchestrator applies publications via `ctx.publish_rows(...)`.
    """

    rows: tuple[T, ...]
    publications: tuple[RowPublication, ...] = ()
