from pathlib import Path
import polars as pl

from omop_etl.infra.io.path_planner import run_root
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.vocabulary.core.models import ValidationReport


class VocabularyExporter:
    """
    Run-centric writer for vocabulary outputs: the mapping-validation error report,
    and the concept subset file (`concept_id` from CONCEPT filtered to what the
    mapping files actually reference, used by this run's `Vocabulary` and by
    downstream tools).

    Directory shape: <base>/runs/<started_at>_<run_id>/vocabulary/
    """

    def __init__(self, base_out: Path):
        self.base_out = base_out

    def vocabulary_output_dir(self, meta: RunMetadata) -> Path:
        """Run-scoped dir for this run's vocabulary outputs, created if needed."""
        out_dir = run_root(self.base_out, meta) / "vocabulary"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def write_concept_subset(self, subset: pl.DataFrame, meta: RunMetadata) -> Path:
        """Write a concept subset DataFrame to a TSV, same column layout as Athena's own CONCEPT file."""
        out_path = self.vocabulary_output_dir(meta) / "concept_subset.tsv"
        subset.write_csv(out_path, separator="\t")
        return out_path

    def write_validation_report(self, report: ValidationReport, meta: RunMetadata) -> Path:
        """Write a validation report's errors to a CSV, one row per issue."""
        out_path = self.vocabulary_output_dir(meta) / "mapping_validation_errors.csv"
        pl.DataFrame(
            {
                "file": [str(issue.file) for issue in report.errors],
                "concept_id": [issue.concept_id for issue in report.errors],
                "kind": [issue.kind for issue in report.errors],
                "column": [issue.column for issue in report.errors],
                "detail": [issue.detail for issue in report.errors],
            },
            schema={
                "file": pl.Utf8,
                "concept_id": pl.Int64,
                "kind": pl.Utf8,
                "column": pl.Utf8,
                "detail": pl.Utf8,
            },
        ).write_csv(out_path)

        return out_path
