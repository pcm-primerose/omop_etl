from dataclasses import field, dataclass
from typing import Sequence
import datetime as dt

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.core.linkage import (
    OmopRowReference,
    SourceReference,
    visit_source_ref,
)


@dataclass
class BuildContext:
    """
    Per-patient cross-builder state. Holds the patient_id and person_id inputs,
    and an `published_rows` map indexed by `(target_table, source_ref)` for
    consumer FK resolution. Builders read from `published_rows` using
    `resolve_rows` or `_resolve_link_targets`, only the orchestrator
    writes to it using `publish_rows`.
    """

    patient: Patient
    person_id: int
    published_rows: dict[tuple[str, SourceReference], tuple[OmopRowReference, ...]] = field(default_factory=dict)

    def publish_rows(
        self,
        target_table: str,
        source_ref: SourceReference,
        rows: Sequence[OmopRowReference],
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
        if key in self.published_rows:
            raise RuntimeError(f"Duplicate publish for {target_table} from {source_ref}")
        row_tuple = tuple(rows)
        if not row_tuple:
            raise ValueError(f"Cannot publish empty row set for {target_table} from {source_ref}")
        for row in row_tuple:
            if row.table != target_table:
                raise ValueError(f"Published row table mismatch: key={target_table}, row={row.table}")
        self.published_rows[key] = tuple(sorted(row_tuple, key=lambda r: (r.primary_concept_id, r.row_id)))

    def resolve_rows(
        self,
        target_table: str,
        source_ref: SourceReference,
    ) -> tuple[OmopRowReference, ...]:
        """
        Return all OMOP rows previously published for `source_ref` in
        `target_table`, or () if none.
        """
        return self.published_rows.get((target_table, source_ref), ())

    def cross_product_refs(
        self,
        left_table: str,
        left_ref: SourceReference,
        right_table: str,
        right_ref: SourceReference,
    ) -> tuple[tuple[OmopRowReference, OmopRowReference], ...]:
        """N*M expansion for cross-product linkage."""
        return tuple((left, right) for left in self.resolve_rows(left_table, left_ref) for right in self.resolve_rows(right_table, right_ref))

    def resolve_visit_id(self, date: dt.date) -> int | None:
        """
        Resolve a visit_occurrence_id by date. Visits are 1:1 by date
        by construction (VisitOccurrenceBuilder skips duplicates).
        """
        # TODO: remove once visits are modeled as a Patient attribute.
        refs = self.resolve_rows(
            OmopTables.VISIT_OCCURRENCE,
            visit_source_ref(self.patient.patient_id, date),
        )
        return refs[0].row_id if refs else None
