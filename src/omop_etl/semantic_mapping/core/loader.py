import csv
from collections import defaultdict
from importlib.resources.abc import Traversable
from pathlib import Path
from logging import getLogger
from typing import List
from importlib.resources import files as pkg_files

from omop_etl.semantic_mapping.core.models import SemanticRow

_BASE_SEMANTIC_MAPPED = pkg_files("omop_etl.resources.semantic_mapped")
log = getLogger(__name__)


class LoadSemantics:
    def __init__(self, path: Path | None = None):
        self.path = path if path else _resolve_base(_BASE_SEMANTIC_MAPPED)

    def as_rows(self) -> list[SemanticRow]:
        rows: list[SemanticRow] = []
        with open(self.path, "r", newline="") as f:
            for line_no, row in enumerate(csv.DictReader(f), start=2):
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
        idx: dict[str, list[SemanticRow]] = defaultdict(list)
        for row in rows:
            key = row.source_term.lower().strip()
            idx[key].append(row)
        return dict(idx)


def _resolve_base(base: Traversable) -> Traversable:
    candidates = [entry for entry in base.iterdir() if entry.is_file()]
    if not candidates:
        raise ValueError(f"No semantic mapping files found in {base!r}")
    if len(candidates) > 1:
        log.warning(
            "Base semantic dir contains several semantic files, current impl picks the first: %s",
            [c.name for c in candidates],
        )
    return candidates[0]
