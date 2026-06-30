from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.core.linkage import (
    BuildResult,
    SourceReference,
    LinkTarget,
)

T = TypeVar("T")


class OmopBuilder(ABC, Generic[T]):
    """
    Abstract base class for OMOP table builders. Builders are pure with
    respect to BuildContext: `build(ctx)` reads from `ctx` and returns a
    BuildResult, only `build_and_populate` (or the service orchestrator)
    applies the result's publications to `ctx`.

    Class vars:
        table_name: The OMOP table name (one of OmopTables.* constants).
        id_namespace: Namespace for PK hashing (defaults to table_name).
    """

    table_name: ClassVar[str]
    id_namespace: ClassVar[str | None] = None

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    @abstractmethod
    def build(self, ctx: BuildContext) -> BuildResult[T]:
        """Compute rows and publications from `ctx`, must not mutate `ctx`."""
        ...

    def build_and_populate(self, ctx: BuildContext) -> tuple[T, ...]:
        """
        Run `build(ctx)`, apply its publications to `ctx`, return rows.
        The single place a builder's output mutates `ctx`.
        """
        result = self.build(ctx)
        for pub in result.publications:
            ctx.publish_rows(pub.target_table, pub.source_ref, pub.rows)

        return result.rows

    def generate_row_id(self, patient_id: str, *key_parts) -> int:
        """
        Deterministic 63-bit row ID from `(namespace, patient_id, *key_parts)`.
        Namespace defaults to `table_name`. `patient_id` is required positional
        to prevent cross-patient PK collisions, passed as the leading id part.
        """
        namespace = self.id_namespace or self.table_name
        return row_id(namespace, patient_id, *key_parts)

    def _resolve_link_targets(
        self,
        ctx: BuildContext,
        *,
        target_table: str,
        source_ref: SourceReference,
        cdm_field_local: str | None = None,
    ) -> tuple[LinkTarget, ...]:
        """
        Resolve `(target_row_id, cdm_field_concept_id)` pairs for FK linkage
        from rows previously published under `source_ref` in `target_table`.
        Returns `()` if nothing was published.

        `cdm_field_local` defaults to the OMOP-convention PK column,
        `f"{target_table}.{target_table}_id"`.

        Policy for "no targets" is handled at callsite (builders).
        Raises RuntimeError if the cdm_field static is missing while rows
        are published (required mapping for the FK shape).
        """
        rows = ctx.resolve_rows(target_table, source_ref)
        if not rows:
            return ()

        if cdm_field_local is None:
            cdm_field_local = f"{target_table}.{target_table}_id"

        field_concepts = self.concepts.resolve(
            "cdm_field",
            cdm_field_local,
            domains={"Metadata"},
        )
        if not field_concepts:
            raise RuntimeError(f"Missing cdm_field mapping for {cdm_field_local}")
        field_concept = field_concepts[0]

        return tuple(
            LinkTarget(
                event_id=row.row_id,
                field_concept_id=field_concept.concept_id,
            )
            for row in rows
        )
