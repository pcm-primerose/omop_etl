from dataclasses import dataclass
from typing import Tuple, Dict

from omop_etl.semantic_mapping.core.models import (
    QueryResult,
    BatchQueryResult,
)


@dataclass(frozen=True, slots=True)
class SemanticResultIndex:
    """
    Loads and indexes query results from semantic mapping
    to enable mapping between Patient instances and QueryResult matches.
    """

    # internal: (patient_id, field_path, leaf_index) -> QueryResult
    _by_location: Dict[Tuple[str, Tuple[str, ...], int | None], "QueryResult"]

    @classmethod
    def from_batch(cls, batch: "BatchQueryResult") -> "SemanticResultIndex":
        by_loc: Dict[Tuple[str, Tuple[str, ...], int | None], QueryResult] = {}
        for query_result in batch.results:
            key = (query_result.patient_id, query_result.query.field_path, query_result.query.leaf_index)
            by_loc[key] = query_result
        return cls(_by_location=by_loc)

    def lookup(
        self,
        patient_id: str,
        field_path: Tuple[str, ...],
        leaf_index: int | None,
    ) -> QueryResult | None:
        return self._by_location.get((patient_id, field_path, leaf_index))
