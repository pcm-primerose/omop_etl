from collections.abc import Sequence
from logging import getLogger
from pathlib import Path
import polars as pl

from omop_etl.infra.utils.mapping_io import MAPPING_CONCEPT_COLUMNS, read_mapping_csv
from omop_etl.vocabulary.core.helpers import ATHENA_CONCEPT_COLUMNS, REQUIRED_ATHENA_FILES, validity_from_invalid_reason
from omop_etl.vocabulary.core.models import ValidationReport, ValidationIssue

log = getLogger(__name__)

_STANDARD_CONCEPT_LABELS = {"S": "Standard", "C": "Classification"}
_UNKNOWN_ATHENA_VERSION = "unknown"


class MappingValidationError(RuntimeError):
    """
    Raised when mapping files fail validation against Athena. The ETL must never
    proceed on invalid mappings, see `VocabularyService.validate`.
    """


class AthenaBundleError(RuntimeError):
    """
    Raised when the Athena release directory is missing a required file, or a
    required file isn't even readable as tab-separated data. We load these files
    wholesale into the CDM database's vocab schema, so a bad bundle must never
    reach that load step.
    """


def validate_athena_bundle(athena_dir: Path) -> tuple[str, ...]:
    """
    Check that every file in `REQUIRED_ATHENA_FILES` exists in `athena_dir` and every
    row in it actually parses as well-formed tab-separated data (no ragged rows).

    `collect_schema()` only reads the header and would miss this, so each file is
    fully parsed (and immediately discarded) to force every row through the reader.
    """
    problems: list[str] = []
    for filename in REQUIRED_ATHENA_FILES:
        path = athena_dir / filename
        if not path.exists():
            problems.append(f"{filename}: missing from {athena_dir}")
            continue
        try:
            # quote_char=None: Athena's export is plain tab-separated, not RFC-quoted CSV
            pl.scan_csv(path, separator="\t", infer_schema_length=0, quote_char=None).collect()
        except pl.exceptions.ComputeError as e:
            problems.append(f"{filename}: could not parse as tab-separated data: {e}")
    return tuple(problems)


def validate_mappings(mapping_files: Sequence[Path], athena_dir: Path) -> ValidationReport:
    """
    Check every mapping file's concept_id against the real Athena vocab in `athena_dir`.
    - a file that can't even be parsed (malformed rows, omissing required columns)
    - a duplicate key (value_set, source_value) mapping to conflicting concept_ids within one file (the same
    - a concept_id missing from this vocab release
    - a concept that's non-standard or invalid per Athena's current data (we never want to map to such concepts)
    - any recorded column (`MAPPING_CONCEPT_COLUMNS`, other than the `concept_id` join key itself)
      disagreeing with what Athena currently states.
    """
    athena_concept_source = athena_dir / "CONCEPT.csv"
    errors: list[ValidationIssue] = []
    file_rows: dict[Path, list[dict[str, str]]] = {}
    concept_ids: set[int] = set()

    for path in mapping_files:
        try:
            df = read_mapping_csv(path)
        except pl.exceptions.ComputeError as e:
            errors.append(ValidationIssue(path, None, "malformed", None, f"could not parse this file: {e}"))
            continue

        missing_columns = [c for c in MAPPING_CONCEPT_COLUMNS if c not in df.columns]
        if missing_columns:
            errors.append(ValidationIssue(path, None, "malformed", None, f"missing required column(s): {missing_columns}"))
            continue
        if "value_set" not in df.columns and "source_value" not in df.columns:
            errors.append(ValidationIssue(path, None, "malformed", None, "missing identity column: needs `value_set` and/or `source_value`"))
            continue

        rows = df.to_dicts()
        file_rows[path] = rows
        for row in rows:
            raw = (row.get("concept_id") or "").strip()
            if not raw:
                errors.append(ValidationIssue(path, None, "malformed", "concept_id", f"blank concept_id: {row}"))
                continue
            concept_ids.add(int(raw))

    errors.extend(_duplicate_key_issues(file_rows))

    athena_by_id = _hydrate_from_athena(athena_concept_source, concept_ids)

    for path, rows in file_rows.items():
        for row in rows:
            raw = (row.get("concept_id") or "").strip()
            if not raw:
                continue  # already reported as malformed

            concept_id = int(raw)
            athena_row = athena_by_id.get(concept_id)
            if athena_row is None:
                errors.append(ValidationIssue(path, concept_id, "missing", None, "not found in this Athena release"))
                continue

            if athena_row["standard_concept"] != "S":
                label = athena_row["standard_concept"] or "null"
                errors.append(ValidationIssue(path, concept_id, "non_standard", None, f"Athena marks this concept non-standard ({label})"))

            if (athena_row["invalid_reason"] or "") != "":
                errors.append(ValidationIssue(path, concept_id, "invalid", None, f"Athena marks this concept invalid ({athena_row['invalid_reason']})"))

            expected = {
                "concept_code": athena_row["concept_code"],
                "concept_name": athena_row["concept_name"],
                "domain_id": athena_row["domain_id"],
                "vocabulary_id": athena_row["vocabulary_id"],
                "validity": validity_from_invalid_reason(athena_row["invalid_reason"] or ""),
                "standard_concept": _standard_concept_label(athena_row["standard_concept"]),
                "concept_class_id": athena_row["concept_class_id"],
            }

            for column in MAPPING_CONCEPT_COLUMNS:
                if column == "concept_id":
                    continue  # the join key itself, not a comparable attribute
                recorded, actual = row.get(column), expected[column]
                if not _matches(recorded, actual):
                    errors.append(ValidationIssue(path, concept_id, "drift", column, f"file says {recorded!r}, Athena says {actual!r}"))

    return ValidationReport(errors=tuple(errors), athena_version=_athena_version(athena_concept_source))


def _duplicate_key_issues(file_rows: dict[Path, list[dict[str, str]]]) -> list[ValidationIssue]:
    """
    A curated key (value_set, source_value) must map to exactly one concept_id
    within a file, the same invariant `StaticMapLoader`/`StructuralMapLoader` raises on.
    Skipped for semantic files (no `value_set` column), because semantic mappings can be 1:N.
    """
    issues: list[ValidationIssue] = []
    for path, rows in file_rows.items():
        if not rows or "value_set" not in rows[0]:
            continue

        key_columns = ["value_set", "source_value"] if "source_value" in rows[0] else ["value_set"]
        seen: dict[tuple[str, ...], int] = {}
        for row in rows:
            raw = (row.get("concept_id") or "").strip()
            if not raw:
                continue
            key = tuple((row.get(c) or "").strip() for c in key_columns)
            concept_id = int(raw)
            existing = seen.get(key)
            if existing is not None and existing != concept_id:
                detail = f"{dict(zip(key_columns, key))} already maps to concept_id {existing}, this row maps to {concept_id}"
                issues.append(ValidationIssue(path, concept_id, "duplicate_key", None, detail))
            else:
                seen[key] = concept_id
    return issues


def _hydrate_from_athena(athena_concept_source: Path, concept_ids: set[int]) -> dict[int, dict[str, str | None]]:
    wanted = list(concept_ids)
    athena = (
        # quote_char=None: Athena is tab-separated, not RFC-quoted CSV
        pl.scan_csv(athena_concept_source, separator="\t", infer_schema_length=0, quote_char=None)
        .filter(pl.col("concept_id").cast(pl.Int64).is_in(wanted))
        .select(ATHENA_CONCEPT_COLUMNS)
        .collect()
    )
    return {int(row["concept_id"]): row for row in athena.iter_rows(named=True)}


def _athena_version(athena_concept_source: Path) -> str:
    """
    The Athena release version (with date) from VOCABULARY.csv in the Athena dir.
    The OHDSI convention is to store this version in the row where
    `vocabulary_id=None` row, the column `vocabulary_version` should then contain
    the overall Athena version (in the format: "v5.0 29-AUG-26").
    """
    vocabulary_path = athena_concept_source.parent / "VOCABULARY.csv"

    versions = (
        pl.read_csv(
            vocabulary_path,
            separator="\t",
            columns=["vocabulary_id", "vocabulary_version"],
            infer_schema_length=0,
        )
        .filter(pl.col("vocabulary_id") == "None")
        .get_column("vocabulary_version")
        .drop_nulls()
    )

    if versions.is_empty():
        log.warning("No Athena version found in %s, setting version to 'unknown'", vocabulary_path)
        return _UNKNOWN_ATHENA_VERSION

    unique = versions.unique()

    if versions.len() > 1:
        log.warning(
            "Found %d Athena version rows in %s",
            versions.len(),
            vocabulary_path,
        )

    if unique.len() > 1:
        log.warning(
            "Conflicting Athena versions in %s: %s",
            vocabulary_path,
            unique.to_list(),
        )
        return _UNKNOWN_ATHENA_VERSION

    return unique[0]


def _standard_concept_label(standard_concept: str | None) -> str:
    return _STANDARD_CONCEPT_LABELS.get((standard_concept or "").strip(), "Non-standard")


def _matches(recorded: str | None, actual: str | None) -> bool:
    return (recorded or "").casefold().strip() == (actual or "").casefold().strip()
