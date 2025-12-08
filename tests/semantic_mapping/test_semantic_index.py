from src.omop_etl.semantic_mapping.semantic_index import DictSemanticIndex
from src.omop_etl.semantic_mapping.loader import LoadSemantics


def test_semantic_index(semantic_file, queries):
    loader = LoadSemantics(semantic_file)
    index = DictSemanticIndex(indexed_corpus=loader.as_indexed())

    # assert that first query has mapped concept for AML
    results = index.lookup_exact(queries=queries)
    assert len(results.matches) == 1
    assert len(results.missing) == 1
    missing = [m for m in results.missing][0]
    match = [m for m in results.matches][0]

    assert match.patient_id == "A"
    for match in results.matches:
        for results in match.results:
            assert results.omop_name == "acute myeloid leukemia"
        assert match.query.query == "aml"

    assert missing.patient_id == "B"
    assert missing.query == "missing in semantic data"
