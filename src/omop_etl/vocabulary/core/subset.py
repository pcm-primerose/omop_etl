from collections.abc import Iterable
from logging import getLogger
from pathlib import Path
import polars as pl

from omop_etl.vocabulary.core.helpers import ACCEPTABLE_STANDARD_CONCEPT_VALUES, ATHENA_CONCEPT_COLUMNS, CONCEPT_ANCESTOR_COLUMNS, scan_athena_table
from omop_etl.infra.utils.constants import NO_MATCHING_CONCEPT
from omop_etl.infra.utils.mapping_io import read_mapping_csv
from omop_etl.vocabulary.core.models import ConceptSubsetReport, FlaggedConcept

log = getLogger(__name__)


def collect_concept_ids(mapping_files: Iterable[Path]) -> set[int]:
    """
    The omop concept ids from the column `concept_id` referenced across the mapping files (static, structural, semantic),
    plus `NO_MATCHING_CONCEPT`.
    """
    ids: set[int] = {NO_MATCHING_CONCEPT}
    for path in mapping_files:
        for row in read_mapping_csv(path).iter_rows(named=True):
            raw = (row.get("concept_id") or "").strip()
            if not raw:
                raise ValueError(f"Row in {path} has a blank `concept_id`: {row}")
            else:
                ids.add(int(raw))
    return ids


def scan_concept_subset(athena_dir: Path, concept_ids: Iterable[int]) -> pl.DataFrame:
    """
    Filter Athena's `CONCEPT.csv` in `athena_dir` to `concept_ids` from actual
    mappings (plus `NO_MATCHING_CONCEPT`), lazily so the full Athena dataset doesn't
    load into memory. Same columns as `Vocabulary` hydrates from and the concept
    subset file written by `VocabularyExporter.write_concept_subset`.
    """
    wanted = list(concept_ids)
    return scan_athena_table(athena_dir, "CONCEPT").filter(pl.col("concept_id").cast(pl.Int64).is_in(wanted)).select(ATHENA_CONCEPT_COLUMNS).collect()


def scan_concept_ancestor_subset(athena_dir: Path, descendant_concept_ids: Iterable[int]) -> pl.DataFrame:
    """
    Filter Athena's `CONCEPT_ANCESTOR.csv` in `athena_dir` to rows whose
    `descendant_concept_id` is one of `descendant_concept_ids`, the drug to ingredient
    rollup `drug_era` needs. All 4 of the file's columns are wanted, no `.select()`
    needed.

    Doesn't filter to Ingredient-class ancestors, that's OMOP decisions for the
    era builder to make (via `Vocabulary.hydrate(ancestor_concept_id)`), not this
    generic Athena-facing layer.

    If `descendant_concept_ids` is empty (meaning no mapped Drug-domain concepts),
    the file is never scanned since there's nothing to look up.
    """
    wanted = list(descendant_concept_ids)
    if not wanted:
        return pl.DataFrame(schema={col: pl.Utf8 for col in CONCEPT_ANCESTOR_COLUMNS})

    return scan_athena_table(athena_dir, "CONCEPT_ANCESTOR").filter(pl.col("descendant_concept_id").cast(pl.Int64).is_in(wanted)).collect()


def concept_subset_report(subset: pl.DataFrame, concept_ids: set[int]) -> ConceptSubsetReport:
    """
    Reports: `missing_concept_ids` which are the mapping ids not present in this vocab release,
    and `flagged_concepts` are the mapped concepts that are non-standard or invalid.
    `NO_MATCHING_CONCEPT` is not flagged, it's a sentinel.

    Logs a warning if either is non-empty, by the time this is called, mapping
    validation should have already guaranteed every mapped id is present/standard/
    valid, so a hit here means those two checks have drifted apart from each other.
    """
    found = {int(cid) for cid in subset.get_column("concept_id")}
    missing_concept_ids = frozenset(concept_ids - found)

    flagged_concepts = tuple(
        FlaggedConcept(int(row["concept_id"]), _flag_reason(row["standard_concept"], row["invalid_reason"]))
        for row in subset.iter_rows(named=True)
        if int(row["concept_id"]) != NO_MATCHING_CONCEPT
        and (row["standard_concept"] not in ACCEPTABLE_STANDARD_CONCEPT_VALUES or (row["invalid_reason"] or "") != "")
    )

    if missing_concept_ids or flagged_concepts:
        log.warning(
            "Concept subset has concepts mapping validation should have already caught: missing=%s flagged=%s",
            missing_concept_ids,
            flagged_concepts,
        )

    return ConceptSubsetReport(written_count=subset.height, missing_concept_ids=missing_concept_ids, flagged_concepts=flagged_concepts)


def _flag_reason(standard_concept: str | None, invalid_reason: str | None) -> str:
    reasons: list[str] = []
    if standard_concept not in ACCEPTABLE_STANDARD_CONCEPT_VALUES:
        reasons.append(f"non-standard ({standard_concept or 'null'})")
    if (invalid_reason or "") != "":
        reasons.append(f"invalid ({invalid_reason})")
    return ", ".join(reasons)
