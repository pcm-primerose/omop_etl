from collections.abc import Iterable
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
import polars as pl

from omop_etl.vocabulary.vocabulary import CONCEPT_COLUMNS
from omop_etl.infra.utils.constants import NO_MATCHING_CONCEPT

log = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FlaggedConcept:
    concept_id: int
    reason: str  # non-standard or invalid


@dataclass(frozen=True, slots=True)
class ConceptSubsetReport:
    written_count: int
    missing_concept_ids: frozenset[int]
    flagged_concepts: tuple[FlaggedConcept, ...]


def collect_concept_ids(mapping_files: Iterable[Path]) -> set[int]:
    """
    The omop concept ids from the column `concept_id` referenced across the mapping files (static, structural, semantic),
    plus `NO_MATCHING_CONCEPT`.
    """
    ids: set[int] = {NO_MATCHING_CONCEPT}
    for path in mapping_files:
        df = pl.read_csv(path, comment_prefix="#", infer_schema_length=0)
        for row in df.iter_rows(named=True):
            raw = (row.get("concept_id") or "").strip()
            if not raw:
                raise ValueError(f"Row in {path} has a blank `concept_id`: {row}")
            else:
                ids.add(int(raw))
    return ids


def generate_concept_subset(concept_source: Path, concept_ids: set[int], out_path: Path) -> ConceptSubsetReport:
    """
    Filter the full OMOP `CONCEPT` file to `concept_ids` from actual mappings, write
    the subset (the `Vocabulary` source, same columns). The CONCEPT file is scanned
    lazily so the Athena dataset doesn't fully load into memory.

    Reports: `missing_concept_ids` which are the mapping ids not present in this vocab release,
    and `flagged_concepts` are the mapped concepts that are non-standard or invalid.
    `NO_MATCHING_CONCEPT` is not flagged, it's a sentinel.
    """
    wanted = list(concept_ids)
    subset = (
        # quote_char=None: Athena can have `"` in fields
        pl.scan_csv(concept_source, separator="\t", infer_schema_length=0, quote_char=None)
        .filter(pl.col("concept_id").cast(pl.Int64).is_in(wanted))
        .select(CONCEPT_COLUMNS)
        .collect()
    )

    found = {int(cid) for cid in subset.get_column("concept_id")}
    missing_concept_ids = frozenset(concept_ids - found)

    flagged_concepts = tuple(
        FlaggedConcept(int(row["concept_id"]), _flag_reason(row["standard_concept"], row["invalid_reason"]))
        for row in subset.iter_rows(named=True)
        if int(row["concept_id"]) != NO_MATCHING_CONCEPT and (row["standard_concept"] != "S" or (row["invalid_reason"] or "") != "")
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    subset.write_csv(out_path, separator="\t")
    return ConceptSubsetReport(written_count=subset.height, missing_concept_ids=missing_concept_ids, flagged_concepts=flagged_concepts)


def _flag_reason(standard_concept: str | None, invalid_reason: str | None) -> str:
    reasons: list[str] = []
    if standard_concept != "S":
        reasons.append(f"non-standard ({standard_concept or 'null'})")
    if (invalid_reason or "") != "":
        reasons.append(f"invalid ({invalid_reason})")
    return ", ".join(reasons)
