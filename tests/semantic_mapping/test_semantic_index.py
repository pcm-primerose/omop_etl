from typing import List

import pytest
from pathlib import Path
import polars as pl

from omop_etl.semantic_mapping.models import Query, OmopDomain, QueryTarget
from src.omop_etl.semantic_mapping.semantic_index import DictSemanticIndex
from src.omop_etl.semantic_mapping.loader import LoadSemantics


@pytest.fixture
def semantic_data() -> pl.DataFrame:
    df = pl.DataFrame(
        data={
            "term_id": ["a", "b", "c"],
            "source_col": ["x", "y", "z"],
            "source_term": ["something", "AML", "gsv sleeper service"],
            "frequency": [1, 2, 3],
            "omop_concept_id": [10, 20, 30],
            "omop_concept_code": [100, 200, 300],
            "omop_name": ["something else", "Acute Myeloid Leukemia", "military aircraft"],
            "omop_class": ["abc", "cde", "efg"],
            "omop_concept": ["S", "S", "S"],
            "omop_validity": ["Valid", "Valid", "Valid"],
            "omop_domain": ["condition", "condition", "condition"],
            "omop_vocab": ["a", "b", "c"],
        }
    )
    return df


@pytest.fixture
def semantic_file(tmp_path, semantic_data) -> Path:
    path = Path(tmp_path / "semantic_test.csv").resolve()
    semantic_data.write_csv(path)
    return path


@pytest.fixture
def queries() -> List[Query]:
    queries: List[Query] = [
        Query(
            patient_id="A",
            id="abc",
            query="AML",
            field_path=("tumor_type", "main_tumor_type"),
            raw_value="AML",
            leaf_index=None,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
        Query(
            patient_id="B",
            id="def",
            query="missing in semantic data",
            field_path=("tumor_type", "main_tumor_type"),
            raw_value="missing in semantic data",
            leaf_index=None,
            target=QueryTarget(domains=[OmopDomain.CONDITION]),
        ),
    ]
    return queries


def test_semantic_index(semantic_file, queries):
    loader = LoadSemantics(semantic_file)
    rows = loader.as_rows()
    index = DictSemanticIndex(indexed_corpus=loader.as_indexed())

    print(f"loader in test: {loader, rows, index}")

    # assert that first query has mapped concept for AML
    results = index.lookup_exact(queries=queries)
    print(f"results in test: {results}")
    assert len(results.matches) == 1
    assert len(results.missing) == 1

    match = [m for m in results.matches][0]
    assert match.patient_id == "A"
    for match in results.matches:
        for results in match.results:
            assert results.omop_name == "Acute Myeloid Leukemia"
        assert match.query.query == "AML"

    missing = [m for m in results.missing][0]
    assert missing.patient_id == "B"
    assert missing.query == "missing in semantic data"
