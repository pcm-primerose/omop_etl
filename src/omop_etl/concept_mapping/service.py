from pathlib import Path
from typing import Mapping, List, Collection
from logging import getLogger

from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.concept_mapping.core.exporter import ConceptLookupExporter
from omop_etl.concept_mapping.core.static_loader import StaticMapLoader
from omop_etl.concept_mapping.core.structural_loader import StructuralMapLoader
from omop_etl.infra.io.format_utils import expand_formats
from omop_etl.infra.io.types import (
    Layout,
    AnyFormatToken,
    WideFormat,
    WIDE_FORMATS,
)
from omop_etl.concept_mapping.core.models import (
    MappedConcept,
    LookupResult,
    LookupType,
)
from omop_etl.semantic_mapping.core.models import (
    BatchQueryResult,
    OmopDomain,
)

log = getLogger(__name__)


def _concept_matches_filter(
    concept: MappedConcept,
    domains: Collection[OmopDomain | str] | None,
    vocabs: Collection[str] | None,
    validities: Collection[str] | None,
) -> bool:
    """
    Case-insensitive filter check against a MappedConcept's attributes.

    `domains` accepts both OmopDomain enum members (clinical domains used by the
    semantic-mapping configs) and raw strings (for non-clinical/admin domains
    like "Gender", "Visit", "Route", "Type Concept", "Metadata").
    """
    if domains is not None:
        wanted = {d.lower() for d in domains}
        if (concept.domain_id or "").lower() not in wanted:
            return False
    if vocabs is not None:
        wanted_v = {v.lower() for v in vocabs}
        if (concept.vocabulary_id or "").lower() not in wanted_v:
            return False
    if validities is not None:
        wanted_s = {s.lower() for s in validities}
        if (concept.validity or "").lower() not in wanted_s:
            return False
    return True


class ConceptLookupService:
    """
    Run-aware service for concept lookups behind a single `resolve()` entry point
    (curated value-set/structural lookups by `str` namespace, term-field/semantic
    lookups by `field_path` tuple). Adds:
    - Automatic tracking of hits and misses
    - Coverage statistics per value_set
    - Export of missed lookups to file

    `_semantic_field_paths` is the allowed list of field_paths the semantic pipeline
    extracts. A `resolve()` for a field_path not declared there raises (a wiring bug).
    """

    def __init__(
        self,
        static_index: Mapping[tuple[str, str], MappedConcept],
        structural_index: Mapping[str, MappedConcept] | None = None,
        semantic_index: SemanticResultIndex | None = None,
        semantic_field_paths: Collection[tuple[str, ...]] | None = None,
        meta: RunMetadata | None = None,
        outdir: Path | None = None,
        layout: Layout = Layout.TRIAL_RUN,
    ):
        self._static: Mapping[tuple[str, str], MappedConcept] = static_index
        self._structural: Mapping[str, MappedConcept] = structural_index or {}
        self._semantic_index = semantic_index
        self._semantic_field_paths = frozenset(semantic_field_paths) if semantic_field_paths is not None else None
        self._meta = meta
        self._result = LookupResult()
        self._exporter = ConceptLookupExporter(
            base_out=(outdir if outdir is not None else Path(".data")),
            layout=layout,
        )

    @classmethod
    def from_paths(
        cls,
        static_path: Path,
        structural_path: Path | None = None,
        semantic_batch: BatchQueryResult | None = None,
        meta: RunMetadata | None = None,
        outdir: Path | None = None,
        layout: Layout = Layout.TRIAL_RUN,
    ) -> "ConceptLookupService":
        """
        Factory method to create a service from file paths.

        Args:
            static_path: Path to static mapping CSV
            structural_path: Path to structural mapping CSV
            semantic_batch: Batch query result from semantic mapping
            meta: Run metadata for export
            outdir: Output directory for exports
            layout: Output layout
        """
        static_index = StaticMapLoader(static_path).as_index()
        structural_index = StructuralMapLoader(structural_path).as_index() if structural_path is not None else None
        semantic_index = SemanticResultIndex.from_batch(semantic_batch) if semantic_batch is not None else None

        # the configured field allow-list drives the lookup-time wiring check
        from omop_etl.semantic_mapping.core.semantic_config import DEFAULT_FIELD_CONFIGS

        semantic_field_paths = {cfg.field_path for cfg in DEFAULT_FIELD_CONFIGS} if semantic_batch is not None else None

        return cls(
            static_index=static_index,
            structural_index=structural_index,
            semantic_index=semantic_index,
            semantic_field_paths=semantic_field_paths,
            meta=meta,
            outdir=outdir,
            layout=layout,
        )

    @property
    def result(self) -> LookupResult:
        """Access the accumulated lookup results."""
        return self._result

    def resolve(
        self,
        namespace: str | tuple[str, ...],
        value: str | None = None,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> tuple[MappedConcept, ...]:
        """
        Resolve a source value to OMOP concept(s), the single lookup entry point.

        `namespace` is a value-set name (`str`) for curated mappings, or a Patient
        `field_path` (`tuple`) for term-field (semantic) mappings.
        `value` is the source value to map, pass `None` for value-less constant mappings.
        Returns all matching concepts after filtering, as a tuple (may be empty).

        A tuple namespace is validated against the configured term fields and raises
        on an unconfigured field_path (a wiring bug), a `str` value-set is open (a
        miss is just a data gap). The namespace *type* is the only discriminator.
        """
        if isinstance(namespace, tuple):
            return self._resolve_term(namespace, value, domains=domains, vocabs=vocabs, validity=validity)
        return self._resolve_curated(namespace, value, domains=domains, vocabs=vocabs, validity=validity)

    def _resolve_curated(
        self,
        value_set: str,
        value: str | None,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> tuple[MappedConcept, ...]:
        """
        Resolve a curated value-set lookup against the unified index: a value-keyed
        `source_value` mapping, or a value-less constant (structural) mapping when
        `value is None`. Inputs are lowercased and stripped before index access,
        records a hit/miss for coverage under the derived kind.
        """
        kind: LookupType
        record_value: str
        if value is None:
            kind = "structural"
            record_value = ""
            concept = self._structural.get(value_set.lower().strip())
        else:
            kind = "static"
            record_value = value
            concept = self._static.get((value_set.lower().strip(), str(value).lower().strip()))
        if concept is None:
            self._result.record_miss(kind, value_set, record_value)
            return ()
        if not _concept_matches_filter(concept, domains, vocabs, validity):
            # concept mapped but rejected by filter
            return ()
        self._result.record_match(kind, value_set, record_value, concept)
        return (concept,)

    def _resolve_term(
        self,
        field_path: tuple[str, ...],
        value: str | None,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> tuple[MappedConcept, ...]:
        """
        Resolve a term-field (semantic) lookup for a source `value` at a Patient
        field_path. Raises ValueError if the field_path is not configured (a wiring
        bug), RuntimeError on a duplicate concept_id (a mapping-file issue).
        Legitimate multi-concept mappings (e.g. combination drugs) return multiple
        unique concepts, builders iterate and emit one row per concept.
        """
        if self._semantic_field_paths is not None and tuple(field_path) not in self._semantic_field_paths:
            raise ValueError(f"Semantic lookup for unconfigured field_path {field_path!r}; declare it in the semantic field config so it gets extracted.")

        if self._semantic_index is None:
            return ()

        qr = self._semantic_index.lookup(
            field_path=field_path,
            value=value,
        )
        if qr is None or not qr.results:
            return ()

        mapped: list[MappedConcept] = []
        for row in qr.results:
            concept = MappedConcept(
                concept_id=int(row.omop_concept_id),
                concept_code=row.omop_concept_code,
                concept_name=row.omop_concept_name,
                domain_id=row.omop_domain,
                vocabulary_id=row.omop_vocab,
                validity=row.omop_validity,
            )
            if _concept_matches_filter(concept, domains, vocabs, validity):
                mapped.append(concept)

        # detect duplicate concept_ids (mapping file issue, not legitimate multi-concept)
        seen_ids: set[int | str] = set()
        for m in mapped:
            if m.concept_id in seen_ids:
                path_str = ".".join(field_path)
                details = [(c.concept_id, c.concept_name, c.domain_id) for c in mapped]
                log.error(
                    "Duplicate concept_id %s in semantic mapping: field_path=%s, value=%s, all matches=%s",
                    m.concept_id,
                    path_str,
                    value,
                    details,
                )
                raise RuntimeError(f"Duplicate concept_id {m.concept_id} in semantic mapping: field_path={path_str}, value={value}, all matches={details}")
            seen_ids.add(m.concept_id)

        return tuple(mapped)

    def export(
        self,
        meta: RunMetadata | None = None,
        formats: AnyFormatToken = "csv",
        write_output: bool = True,
    ) -> LookupResult:
        """
        Export missed lookups and coverage stats.

        Args:
            meta: Run metadata (uses instance meta if not provided)
            formats: Output formats (csv, json, parquet, tsv, or "all")
            write_output: Whether to write output files

        Returns:
            The accumulated LookupResult
        """
        run_meta = meta or self._meta
        if write_output and run_meta is None:
            raise ValueError("RunMetadata required for export. Provide via __init__ or export()")

        if write_output and run_meta is not None:
            supported_fmts: List[WideFormat] = expand_formats(formats, allowed=WIDE_FORMATS)
            self._exporter.export(
                lookup_result=self._result,
                meta=run_meta,
                formats=supported_fmts,
            )

        return self._result

    def reset(self) -> None:
        """Reset accumulated results for a new run."""
        self._result = LookupResult()
