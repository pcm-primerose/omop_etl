from typing import List

from omop_etl.semantic_mapping.core.models import (
    SemanticRow,
    QueryResult,
    Query,
    BatchQueryResult,
)


# todo: when expanding to athena, make multi-index
#   and construct with this before running query, for now doesn't matter
class SemanticIndex:
    def __init__(self, indexed_corpus: dict[str, List[SemanticRow]]):
        self.indexed_corpus = indexed_corpus

    def lookup_exact(self, queries: list[Query]) -> BatchQueryResult:
        results: list[QueryResult] = []

        for q in queries:
            query = q.query.casefold().strip()
            candidates = self.indexed_corpus.get(query, [])

            results.append(
                QueryResult(
                    patient_id=q.patient_id,
                    query=q,
                    results=list(candidates),
                )
            )

        return BatchQueryResult(results=tuple(results))
