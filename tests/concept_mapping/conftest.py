import pytest
from pathlib import Path
import polars as pl

from omop_etl.concept_mapping.core.models import (
    StaticConcept,
    StructuralConcept,
    MappedConcept,
)
from omop_etl.infra.utils.run_context import RunMetadata


@pytest.fixture
def run_meta() -> RunMetadata:
    return RunMetadata(
        trial="TEST",
        run_id="abc123",
        started_at="20260105T120000Z",
    )


@pytest.fixture
def static_concepts() -> list[StaticConcept]:
    return [
        StaticConcept(
            value_set="sex",
            source_value="M",
            concept_id=8507,
            concept_code="M",
            concept_name="Male",
            concept_class_id="Gender",
            standard_concept="Standard",
            validity="Valid",
            domain_id="Gender",
            vocabulary_id="Gender",
        ),
        StaticConcept(
            value_set="sex",
            source_value="F",
            concept_id=8532,
            concept_code="F",
            concept_name="Female",
            concept_class_id="Gender",
            standard_concept="Standard",
            validity="Valid",
            domain_id="Gender",
            vocabulary_id="Gender",
        ),
    ]


@pytest.fixture
def static_index(static_concepts) -> dict[tuple[str, str], MappedConcept]:
    # mirror StaticMapLoader.as_index(): value-keyed, projected to MappedConcept
    return {(c.value_set.casefold().strip(), c.source_value.casefold().strip()): c.to_mapped() for c in static_concepts}


@pytest.fixture
def structural_concepts() -> list[StructuralConcept]:
    return [
        StructuralConcept(
            value_set="ecrf",
            concept_id=32817,
            concept_code="OMOP4822053",
            concept_name="EHR encounter record",
            domain_id="Type Concept",
            vocabulary_id="Type Concept",
            validity="Valid",
            concept_class_id="Obs Type",
            standard_concept="Standard",
        ),
    ]


@pytest.fixture
def structural_index(structural_concepts) -> dict[str, MappedConcept]:
    # mirror StructuralMapLoader.as_index(): value-less, keyed by value_set, projected to MappedConcept
    return {c.value_set.casefold().strip(): c.to_mapped() for c in structural_concepts}


@pytest.fixture
def static_csv_content() -> pl.DataFrame:
    return pl.DataFrame(
        data={
            "value_set": ["sex", "sex"],
            "source_value": ["M", "F"],
            "concept_id": [8507, 8532],
            "concept_code": ["M", "F"],
            "concept_name": ["Male", "Female"],
            "concept_class_id": ["Gender", "Gender"],
            "standard_concept": ["Standard", "Standard"],
            "validity": ["", ""],
            "domain_id": ["Gender", "Gender"],
            "vocabulary_id": ["Gender", "Gender"],
        }
    )


@pytest.fixture
def static_csv_file(tmp_path, static_csv_content) -> Path:
    path = tmp_path / "static_mapping.csv"
    static_csv_content.write_csv(path)
    return path


@pytest.fixture
def structural_csv_content() -> pl.DataFrame:
    return pl.DataFrame(
        data={
            "value_set": ["ecrf"],
            "concept_id": [32817],
            "concept_code": ["OMOP4822053"],
            "concept_name": ["EHR encounter record"],
            "concept_class_id": ["Obs Type"],
            "standard_concept": ["Standard"],
            "validity": [""],
            "domain_id": ["Type Concept"],
            "vocabulary_id": ["Type Concept"],
        }
    )


@pytest.fixture
def structural_csv_file(tmp_path, structural_csv_content) -> Path:
    path = tmp_path / "structural_mapping.csv"
    structural_csv_content.write_csv(path)
    return path
