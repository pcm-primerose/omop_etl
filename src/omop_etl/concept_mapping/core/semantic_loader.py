from dataclasses import dataclass
from typing import Tuple, Dict

from omop_etl.semantic_mapping.core.models import (
    QueryResult,
    BatchQueryResult,
)


def _normalize(value: str) -> str:
    return value.casefold().strip()


@dataclass(frozen=True, slots=True)
class SemanticResultIndex:
    """
    Indexes semantic-mapping query results by the normalized source value, so
    builders resolve concepts by value.
    """

    # internal: (field_path, normalized_value) -> QueryResult,
    # the mapping is global and content-keyed
    _by_location: Dict[Tuple[Tuple[str, ...], str], QueryResult]

    @classmethod
    def from_batch(cls, batch: BatchQueryResult) -> SemanticResultIndex:
        by_loc: Dict[Tuple[Tuple[str, ...], str], QueryResult] = {}
        for query_result in batch.results:
            key = (query_result.query.field_path, _normalize(query_result.query.raw_value))
            by_loc[key] = query_result
        return cls(_by_location=by_loc)

    def lookup(
        self,
        field_path: Tuple[str, ...],
        value: str | None,
    ) -> QueryResult | None:
        if value is None:
            return None
        return self._by_location.get((field_path, _normalize(value)))
