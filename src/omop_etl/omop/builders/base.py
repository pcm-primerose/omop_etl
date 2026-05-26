import datetime as dt
import json
from abc import ABC, abstractmethod
from collections.abc import Hashable, Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Generic, TypeVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.models.tables import OmopTables

T = TypeVar("T")


def _all_str_values(ns) -> set[str]:
    return {v for k, v in vars(ns).items() if not k.startswith("_") and isinstance(v, str)}


@dataclass(frozen=True, slots=True)
class SourceRef:
    """
    Identifies a source-domain instance: per-patient, per source attribute
    on Patient, per-natural-key. Used as the anchor for cross-builder PK
    lookups. patient_id is part of identity, not an external coordinate, the
    dict key alone is sufficient and immune to cross-patient bugs.

    `source_attr` must be constant on Patient.
    """

    patient_id: str
    source_attr: str
    natural_key: tuple[Hashable, ...]

    def __post_init__(self):
        valid = _all_str_values(Patient.Collections) | _all_str_values(Patient.Singletons)
        if self.source_attr not in valid:
            raise ValueError(f"Unknown source_attr {self.source_attr!r}; must be one of Patient.Collections.* or Patient.Singletons.* constants")


@dataclass(frozen=True, slots=True)
class OmopRowRef:
    """
    Describes an OMOP row emitted from a source instance.

    `primary_concept_id`: the row's *primary* topic concept, e.g.
    `condition_concept_id` or `drug_concept_id`.
    Value-side concepts (e.g. `value_as_concept_id`) are not stored here.

    primary_concept_id=0 is valid, the row was emitted with an unmapped
    primary concept (per CDM convention), not skipped.

    `table` must be one of OmopTables.* constants.
    """

    table: str
    row_id: int
    primary_concept_id: int

    def __post_init__(self):
        if self.table not in OmopTables.values():
            raise ValueError(f"Unknown OMOP table {self.table!r}, must be one of OmopTables.* constants")


@dataclass
class BuildContext:
    """Per-patient runtime state passed to builders.

    Contains only per-patient data and cross-builder state (e.g. visit
    occurrence FKs, the `emitted_rows` cross-builder PK map).
    """

    patient: Patient
    person_id: int
    visit_id_by_date: dict[dt.date, int] = field(default_factory=dict)
    emitted_rows: dict[tuple[str, SourceRef], tuple[OmopRowRef, ...]] = field(default_factory=dict)

    def publish_rows(
        self,
        target_table: str,
        source_ref: SourceRef,
        rows: Sequence[OmopRowRef],
    ) -> None:
        """
        Publish OMOP rows produced from `source_ref` into `target_table`.
        Each (target_table, source_ref) pair has exactly one publishing
        builder, re-publishing raises. Empty row sets raise (no legitimate use
        case). Rows are stored sorted by (primary_concept_id, row_id) so
        modifier expansion is deterministic by construction.
        """
        if target_table not in OmopTables.values():
            raise ValueError(f"Unknown OMOP target_table {target_table!r}; must be one of OmopTables.* constants")
        key = (target_table, source_ref)
        if key in self.emitted_rows:
            raise RuntimeError(f"Duplicate publish for {target_table} from {source_ref}")
        row_tuple = tuple(rows)
        if not row_tuple:
            raise ValueError(f"Cannot publish empty row set for {target_table} from {source_ref}")
        for row in row_tuple:
            if row.table != target_table:
                raise ValueError(f"Published row table mismatch: key={target_table}, row={row.table}")
        self.emitted_rows[key] = tuple(sorted(row_tuple, key=lambda r: (r.primary_concept_id, r.row_id)))

    def resolve_rows(
        self,
        target_table: str,
        source_ref: SourceRef,
    ) -> tuple[OmopRowRef, ...]:
        """
        Return all OMOP rows previously published for `source_ref` in
        `target_table`, or () if none.
        """
        return self.emitted_rows.get((target_table, source_ref), ())

    def cross_product_refs(
        self,
        left_table: str,
        left_ref: SourceRef,
        right_table: str,
        right_ref: SourceRef,
    ) -> tuple[tuple[OmopRowRef, OmopRowRef], ...]:
        """
        N*M expansion for cross-product linkage.
        """
        return tuple((left, right) for left in self.resolve_rows(left_table, left_ref) for right in self.resolve_rows(right_table, right_ref))


class OmopBuilder(ABC, Generic[T]):
    """
    Abstract base class for OMOP table builders.

    Class vars:
        table_name: The OMOP table name (one of OmopTables.* constants)
        id_namespace: Namespace for ID generation
    """

    table_name: ClassVar[str]
    id_namespace: ClassVar[str | None] = None

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    @abstractmethod
    def build(self, ctx: BuildContext) -> list[T]:
        """
        Build rows from a patient.

        Args:
            ctx: Build context containing patient data, person_id, concept service,
                 and cross-builder state (e.g. visit_id_by_date).

        Returns:
            A list of rows (may be empty if patient data is insufficient).
        """
        ...

    def populate_context(self, rows: list[T], ctx: BuildContext) -> None:
        """
        Publish context state derived from this builder's rows for downstream builders.
        Default no-op, override in builders that produce shared identifiers other
        builders consume (e.g. VisitOccurrenceBuilder writes `ctx.visit_id_by_date`).
        """
        return

    def build_and_populate(self, ctx: BuildContext) -> list[T]:
        """
        Convenience method to build and populate context in one go e.g. for tests.
        """
        rows = self.build(ctx)
        self.populate_context(rows, ctx)
        return rows

    def generate_row_id(self, patient_id: str, *key_parts) -> int:
        """
        Deterministic row ID from patient_id + arbitrary key parts, using
        SHA1 hashing with builder's namespace to create a reproducible 63-bit
        integer ID. Namespace defaults to the table_name.

        patient_id is required positional (prevents cross-patient PK collisions
        if a caller forgets to lead with patient_id).

        Key parts are JSON-serialized to avoid two collision modes:
          (a) delimiter collision: ("a:b", "c") vs ("a", "b:c") both join to "a:b:c".
          (b) None-drop collision: ("a", None, "b") vs ("a", "b") both join to "a:b".
        json.dumps preserves structure (escapes delimiters, keeps Nones).
        """
        namespace = self.id_namespace or self.table_name
        payload = json.dumps(
            [patient_id, *key_parts],
            default=str,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return sha1_bigint(namespace, payload)

    def _resolve_event_refs(
        self,
        ctx: BuildContext,
        *,
        target_table: str,
        source_ref: SourceRef,
        cdm_field_local: str,
    ) -> tuple[tuple[OmopRowRef, int], ...]:
        """
        Strict resolver. Returns only real published link targets, empty
        tuple if nothing was published.

        Use when "no FK target" should mean "emit nothing" (typical for
        relationship rows like fact_relationship, a fact_relationship row
        exists only to express linkage).

        Raises RuntimeError if cdm_field static is missing while rows ARE
        published, required infrastructure.
        """
        rows = ctx.resolve_rows(target_table, source_ref)
        if not rows:
            return ()
        field_concept = self.concepts.lookup_static("cdm_field", cdm_field_local, domains={"Metadata"})
        if field_concept is None:
            raise RuntimeError(f"Missing cdm_field mapping for {cdm_field_local}")
        return tuple((row, field_concept.concept_id) for row in rows)

    def _event_refs_or_unlinked(
        self,
        ctx: BuildContext,
        *,
        target_table: str,
        source_ref: SourceRef,
        cdm_field_local: str,
    ) -> tuple[tuple[OmopRowRef | None, int | None], ...]:
        """
        Resolve-or-unlinked. Same as `_resolve_event_refs`, but when no
        upstream row was published returns `((None, None),)` so the caller
        still emits one unlinked row.

        Use when the source fact / modifier observation should always be
        emitted even without upstream linkage (e.g. for AE modifier
        observations, AE severity assessment exists regardless of whether
        the AE produced a condition_occurrence row).
        """
        refs = self._resolve_event_refs(
            ctx,
            target_table=target_table,
            source_ref=source_ref,
            cdm_field_local=cdm_field_local,
        )
        return refs if refs else ((None, None),)
