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
    StaticConcept,
    StructuralConcept,
    LookupResult,
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
    validity: Collection[str] | None,
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
    if validity is not None:
        wanted_s = {s.lower() for s in validity}
        if (concept.validity or "").lower() not in wanted_s:
            return False
    return True


class ConceptLookupService:
    """
    Run-aware service for concept lookups.

    Wraps static, structural, and semantic lookups with:
    - Automatic tracking of hits and misses
    - Coverage statistics per value_set
    - Export of missed lookups to file
    """

    def __init__(
        self,
        static_index: Mapping[tuple[str, str], StaticConcept],
        structural_index: Mapping[str, StructuralConcept] | None = None,
        semantic_index: SemanticResultIndex | None = None,
        meta: RunMetadata | None = None,
        outdir: Path | None = None,
        layout: Layout = Layout.TRIAL_RUN,
    ):
        self._static_index = static_index
        self._structural_index = structural_index
        self._semantic_index = semantic_index
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
        structural_index = StructuralMapLoader(structural_path).as_index()
        semantic_index = SemanticResultIndex.from_batch(semantic_batch)

        return cls(
            static_index=static_index,
            structural_index=structural_index,
            semantic_index=semantic_index,
            meta=meta,
            outdir=outdir,
            layout=layout,
        )

    @property
    def result(self) -> LookupResult:
        """Access the accumulated lookup results."""
        return self._result

    def lookup_static(
        self,
        value_set: str,
        local_value: str,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> MappedConcept | None:
        """
        Lookup a static mapping and track the result.

        Inputs are lowercased and stripped before index access.

        Optional filters narrow the returned concept by OMOP domain, vocabulary,
        or standard flag. If the mapped concept does not match the filters,
        returns None and records a miss.
        """
        key = (value_set.lower().strip(), str(local_value).lower().strip())
        c = self._static_index.get(key)
        if c is None:
            self._result.record_miss("static", value_set, local_value)
            return None

        concept = MappedConcept(
            concept_id=c.concept_id,
            concept_code=c.concept_code,
            concept_name=c.concept_name,
            domain_id=c.domain_id,
            vocabulary_id=c.vocabulary_id,
            validity=c.validity,
        )
        if not _concept_matches_filter(concept, domains, vocabs, validity):
            self._result.record_miss("static", value_set, local_value)
            return None

        self._result.record_match("static", value_set, local_value, concept)
        return concept

    def lookup_structural(
        self,
        value_set: str,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> MappedConcept | None:
        """
        Lookup a structural mapping and track the result.

        Optional filters validate that the mapped concept belongs to the expected
        OMOP domain, vocabulary, or standard flag. If the concept fails the
        filter, returns None and records a miss.

        `value_set` is lowercased and stripped before index access for case-insensitive lookups.
        """
        key = value_set.lower().strip()
        c = self._structural_index.get(key)
        if c is None:
            self._result.record_miss("structural", value_set, "")
            return None

        concept = MappedConcept(
            concept_id=c.concept_id,
            concept_code=c.concept_code,
            concept_name=c.concept_name,
            domain_id=c.domain_id,
            vocabulary_id=c.vocabulary_id,
            validity=c.validity,
        )
        if not _concept_matches_filter(concept, domains, vocabs, validity):
            self._result.record_miss("structural", value_set, "")
            return None

        self._result.record_match("structural", value_set, "", concept)
        return concept

    def lookup_semantic(
        self,
        patient_id: str,
        field_path: tuple[str, ...],
        leaf_index: int | None,
        *,
        domains: Collection[OmopDomain | str] | None = None,
        vocabs: Collection[str] | None = None,
        validity: Collection[str] | None = None,
    ) -> tuple[MappedConcept, ...]:
        """
        Lookup semantic mappings for a patient field location.

        Returns all matched concepts after filtering, as a tuple (may be empty).
        Raises RuntimeError if duplicate concept_ids are found (same concept
        mapped twice = mapping file issue that must be resolved).
        Legitimate multi-concept mappings (e.g. combination drugs with multiple
        ingredients) return multiple unique concepts, builders iterate and
        emit one row per concept.
        """
        if self._semantic_index is None:
            return ()

        qr = self._semantic_index.lookup(
            patient_id=patient_id,
            field_path=field_path,
            leaf_index=leaf_index,
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
                    "Duplicate concept_id %s in semantic mapping: patient_id=%s, field_path=%s, leaf_index=%s, all matches=%s",
                    m.concept_id,
                    patient_id,
                    path_str,
                    leaf_index,
                    details,
                )
                raise RuntimeError(
                    f"Duplicate concept_id {m.concept_id} in semantic mapping: "
                    f"patient_id={patient_id}, field_path={path_str}, "
                    f"leaf_index={leaf_index}, all matches={details}"
                )
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
