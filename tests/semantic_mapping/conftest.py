from typing import List
import pytest
from pathlib import Path
import polars as pl

from omop_etl.semantic_mapping.models import (
    Query,
    OmopDomain,
    QueryTarget,
)


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
            "omop_name": ["something else", "Acute Myeloid Leukemia", "systems vehicle"],
            "omop_class": ["abc", "cde", "efg"],
            "omop_concept": ["S", "S", "S"],
            "omop_validity": ["Valid", "Valid", "Valid"],
            "omop_domain": ["condition", "CONDITION", "Condition"],
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
            query="aml",
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
