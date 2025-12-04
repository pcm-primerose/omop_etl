from typing import List

from omop_etl.semantic_mapping.models import (
    SemanticRow,
    QueryResult,
    Query,
    BatchQueryResult,
    QueryTarget,
    OmopDomain,
)


# todo: when expanding to athena, make multi-index
#   and construct with this before running query, for now doesn't matter
class DictSemanticIndex:
    def __init__(self, indexed_corpus: dict[str, List[SemanticRow]]):
        self.indexed_corpus = indexed_corpus

    def lookup_exact(self, queries: list[Query]) -> BatchQueryResult:
        results: list[QueryResult] = []

        for q in queries:
            candidates = self.indexed_corpus.get(q.query, [])
            candidates = self._filter_by_target(candidates, q.target)

            results.append(
                QueryResult(
                    patient_id=q.patient_id,
                    query=q,
                    results=list(candidates),
                )
            )

        return BatchQueryResult(results=tuple(results))

    @staticmethod
    def _filter_by_target(candidates: list[SemanticRow], target: QueryTarget | None) -> list[SemanticRow]:
        if target is None:
            return candidates

        doms = target.domains
        vocabs = target.vocabs
        classes = target.concept_classes
        validity = target.validity
        standard = target.standard_flags

        filtered: list[SemanticRow] = []
        for row in candidates:
            if doms is not None and OmopDomain(row.omop_domain) not in doms:
                continue
            if vocabs is not None and row.omop_vocab not in vocabs:
                continue
            if classes is not None and row.omop_class not in classes:
                continue
            if standard is not None and row.omop_concept not in standard:
                continue
            if validity is not None and row.omop_validity not in validity:
                continue
            filtered.append(row)

        return filtered
