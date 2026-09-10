from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.vocabulary.core.io import VocabularyExporter
from omop_etl.vocabulary.core.models import ValidationReport
from omop_etl.vocabulary.core.subset import collect_concept_ids, concept_subset_report, scan_concept_subset
from omop_etl.vocabulary.core.validate import AthenaBundleError, MappingValidationError, validate_athena_bundle, validate_mappings
from omop_etl.vocabulary.core.vocabulary import Vocabulary


@dataclass(frozen=True, slots=True)
class VocabularyResult:
    """
    Everything downstream builders need from Athena for this run: the concept_id ->
    attributes hydration lookup, the release version to log/stamp for
    reproducibility, and the validation report that confirmed it's all clean.
    """

    vocabulary: Vocabulary
    athena_version: str
    report: ValidationReport


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

        # one scan of CONCEPT.csv (validate_mappings already did its own), reused for
        # the written concept subset file, the missing/flagged check, and building
        # this run's Vocabulary -- no re-reading anything back off disk
        concept_ids = collect_concept_ids(self.mapping_files)
        subset = scan_concept_subset(self.athena_dir, concept_ids)
        concept_subset_report(subset, concept_ids)
        self._exporter.write_concept_subset(subset, meta)

        vocabulary = Vocabulary.from_concept_rows(subset.iter_rows(named=True))

        return VocabularyResult(vocabulary=vocabulary, athena_version=report.athena_version, report=report)
