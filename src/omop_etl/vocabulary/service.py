from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.vocabulary.core.io import VocabularyExporter
from omop_etl.vocabulary.core.models import ValidationReport
from omop_etl.vocabulary.core.subset import (
    collect_concept_ids,
    concept_subset_report,
    scan_concept_ancestor_subset,
    scan_concept_subset,
)
from omop_etl.vocabulary.core.validate import AthenaBundleError, MappingValidationError, validate_athena_bundle, validate_mappings
from omop_etl.vocabulary.core.vocabulary import Vocabulary


@dataclass(frozen=True, slots=True)
class VocabularyResult:
    """
    Everything downstream builders need from Athena for this run: the concept_id to
    attributes hydration lookup, the Athena version, the valiation report, and the
    concept_ancestor rows for the mapped Drug-domain concepts (drug_era's
    ingredient rollup; empty if there are none).
    """

    vocabulary: Vocabulary
    athena_version: str
    report: ValidationReport
    concept_ancestor: pl.DataFrame


class VocabularyService:
    """
    Single entry point for everything Athena-related: validates mapping
    files against Athena, hydrates the concept_id to attributes lookup builders need,
    and surfaces the Athena release version used.

    `run()` writes an error report under `outdir` and raises `MappingValidationError`
    on any validation error, the ETL must never proceed on invalid mappings.
    """

    def __init__(self, outdir: Path, athena_dir: Path, mapping_files: Sequence[Path]):
        self.outdir = outdir
        self.athena_dir = athena_dir
        self.mapping_files = mapping_files
        self._exporter = VocabularyExporter(base_out=outdir)

    def run(self, meta: RunMetadata) -> VocabularyResult:
        bundle_problems = validate_athena_bundle(self.athena_dir)
        if bundle_problems:
            raise AthenaBundleError(f"{len(bundle_problems)} problem(s) with the Athena bundle at {self.athena_dir}:\n" + "\n".join(bundle_problems))

        report = validate_mappings(self.mapping_files, self.athena_dir)
        if report.errors:
            report_path = self._exporter.write_validation_report(report, meta)
            raise MappingValidationError(f"{len(report.errors)} mapping validation error(s), see: {report_path}")

        concept_ids_from_mapping_files = collect_concept_ids(self.mapping_files)
        subset = scan_concept_subset(self.athena_dir, concept_ids_from_mapping_files)
        concept_subset_report(subset, concept_ids_from_mapping_files)
        self._exporter.write_concept_subset(subset, meta)

        concept_ancestor = self._build_concept_ancestor_subset(subset, meta)
        vocabulary_rows = self._vocabulary_rows(subset, concept_ancestor, concept_ids_from_mapping_files)
        vocabulary = Vocabulary.from_concept_rows(vocabulary_rows.iter_rows(named=True))

        return VocabularyResult(
            vocabulary=vocabulary,
            athena_version=report.athena_version,
            report=report,
            concept_ancestor=concept_ancestor,
        )

    def _build_concept_ancestor_subset(self, subset: pl.DataFrame, meta: RunMetadata) -> pl.DataFrame:
        """drug_era's ingredient rollup: concept_ancestor rows for the mapped Drug-domain concepts."""
        drug_concept_ids = {int(cid) for cid in subset.filter(pl.col("domain_id") == "Drug").get_column("concept_id")}
        concept_ancestor = scan_concept_ancestor_subset(self.athena_dir, drug_concept_ids)
        self._exporter.write_concept_ancestor_subset(concept_ancestor, meta)
        return concept_ancestor

    def _vocabulary_rows(
        self,
        subset: pl.DataFrame,
        concept_ancestor: pl.DataFrame,
        concept_ids_from_mapping_files: set[int],
    ) -> pl.DataFrame:
        """
        `subset`, and attributes for any ancestor concept_id not already in it.
        CONCEPT_ANCESTOR has no concept attributes, so ancestor ids
        (usually just the Ingredient-level ones) need an extra CONCEPT.csv scan,
        folded into the same Vocabulary as the mapped concepts.
        """
        ancestor_ids = {int(cid) for cid in concept_ancestor.get_column("ancestor_concept_id")}
        new_ancestor_ids = ancestor_ids - concept_ids_from_mapping_files
        if not new_ancestor_ids:
            return subset
        return pl.concat([subset, scan_concept_subset(self.athena_dir, new_ancestor_ids)])
