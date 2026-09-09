from collections import defaultdict
from importlib.resources.abc import Traversable
from pathlib import Path
from logging import getLogger
from typing import List
from importlib.resources import files as pkg_files
import polars as pl

from omop_etl.semantic_mapping.core.models import SemanticRow

_BASE_SEMANTIC_MAPPED = pkg_files("omop_etl.resources.semantic_mapped")
log = getLogger(__name__)


class LoadSemantics:
    def __init__(self, path: Path | None = None):
        self.path = path if path else _resolve_base(_BASE_SEMANTIC_MAPPED)

    def as_rows(self) -> list[SemanticRow]:
        rows: list[SemanticRow] = []
        # Path.open accepts newline="", but Traversable.open does not
        f = self.path.open("r", newline="") if isinstance(self.path, Path) else self.path.open("r")
        df = pl.read_csv(f, comment_prefix="#", infer_schema_length=0)
        for line_no, row in enumerate(df.iter_rows(named=True), start=2):
            none_cols = [k for k, v in row.items() if v is None]
            if none_cols:
                log.warning(
                    "Malformed row in %s line %d: missing columns %s (row: %s)",
                    self.path,
                    line_no,
                    none_cols,
                    dict(row),
                )
            rows.append(SemanticRow.from_csv_row(row))

        return rows

    def as_indexed(self) -> dict[str, list[SemanticRow]]:
        return self._index(self.as_rows())

    def as_lazyframe(self):
        raise NotImplementedError

    @staticmethod
    def _index(rows: List[SemanticRow]) -> dict[str, List[SemanticRow]]:
        # group by normalized source_term, then dedup by concept_id within each group
        # e.g. collapse "OxyNorm" and "Oxynorm" both mapping to oxycodone
        raw: dict[str, list[SemanticRow]] = defaultdict(list)
        for row in rows:
            key = row.source_value.casefold().strip()
            raw[key].append(row)

        idx: dict[str, list[SemanticRow]] = {}
        for key, candidates in raw.items():
            seen: dict[str, SemanticRow] = {}
            for row in candidates:
                if row.concept_id in seen:
                    log.warning(
                        "Collapsing duplicate concept_id=%s for source_term='%s', consider updating mapping file.",
                        row.concept_id,
                        key,
                    )
                else:
                    seen[row.concept_id] = row
            idx[key] = list(seen.values())

        return idx


def _resolve_base(base: Traversable) -> Traversable:
    candidates = [entry for entry in base.iterdir() if entry.is_file()]
    if not candidates:
        raise ValueError(f"No semantic mapping files found in {base.name}")
    if len(candidates) > 1:
        log.warning(
            "Base semantic dir contains several semantic files, current impl picks the first: %s",
            [c.name for c in candidates],
        )
    return candidates[0]
