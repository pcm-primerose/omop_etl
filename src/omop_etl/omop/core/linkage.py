from dataclasses import dataclass
from typing import Final, TypeVar, Hashable, Generic
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.tables import OmopTables

T = TypeVar("T")


def _all_str_values(ns) -> set[str]:
    return {v for k, v in vars(ns).items() if not k.startswith("_") and isinstance(v, str)}


class SourceAnchors:
    """
    Synthetic anchors for SourceReference when the source isn't a Patient
    attribute. Today only `VISIT_DATE`, used to anchor visit_occurrence rows
    by start date so downstream builders can resolve by date.
    """

    # TODO: remove VISIT_DATE once visits are modeled as a Patient attribute.
    VISIT_DATE = "visit_date"

    @classmethod
    def values(cls) -> set[str]:
        return _all_str_values(cls)


@dataclass(frozen=True, slots=True)
class SourceReference:
    """
    A unique source-domain instance: (patient, source kind,
    natural key). Used as the anchor for cross-builder FK linkage.

    `source_kind` must be a `Patient.Collections.*` / `Patient.Singletons.*`
    constant, or a `SourceAnchors.*` constant.
    """

    patient_id: str
    source_kind: str
    natural_key: tuple[Hashable, ...]

    def __post_init__(self):
        valid = _all_str_values(Patient.Collections) | _all_str_values(Patient.Singletons) | SourceAnchors.values()
        if self.source_kind not in valid:
            raise ValueError(
                f"Unknown source_kind {self.source_kind!r}; must be one of Patient.Collections.*, Patient.Singletons.*, or SourceAnchors.* constants"
            )


@dataclass(frozen=True, slots=True)
class OmopRowReference:
    """
    Handle to an OMOP row emitted by a builder, used by
    downstream builders to construct FK linkage without seeing the full row.

    `primary_concept_id` is the row's primary topic concept (e.g.
    `condition_concept_id`), used publisher-side for deterministic
    sort ordering. `0` is valid (CDM convention: unmapped primary).
    `table` must be a `OmopTables.*` constant.
    """

    table: str
    row_id: int
    primary_concept_id: int

    def __post_init__(self):
        if self.table not in OmopTables.values():
            raise ValueError(f"Unknown OMOP table {self.table!r}, must be one of OmopTables.* constants")


@dataclass(frozen=True, slots=True)
class LinkTarget:
    """
    Resolved FK target for OMOP event-link columns.

    `event_id` is the target row's primary key, and `field_concept_id`
    identifies which CDM field it points to, e.g.
    condition_occurrence.condition_occurrence_id.
    Both are `None` for an intentionally unlinked row.
    """

    event_id: int | None
    field_concept_id: int | None

    def __post_init__(self) -> None:
        if (self.event_id is None) != (self.field_concept_id is None):
            raise ValueError("LinkTarget must be either fully linked (event_id and field_concept_id set) or fully unlinked (both None)")


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


def visit_source_ref(patient_id: str, date: dt.date) -> SourceReference:
    """SourceReference for the visit-occurrence-by-date anchor."""
    # TODO: remove once visits are modeled as a Patient attribute.
    return SourceReference(patient_id, SourceAnchors.VISIT_DATE, (date,))


# builders resolve:
# targets = self._resolve_link_targets(...) or UNLINKED_TARGETS
type LinkTargets = tuple[LinkTarget, ...]

UNLINKED_TARGETS: Final[LinkTargets] = (LinkTarget(event_id=None, field_concept_id=None),)
