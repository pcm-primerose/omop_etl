import pytest
from pathlib import Path

from omop_etl.concept_mapping.core.models import (
    StaticConcept,
    StructuralConcept,
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
            local_value="M",
            concept_id="8507",
            concept_code="M",
            concept_name="Male",
            concept_class="Gender",
            concept_category="Standard",
            valid_flag="Valid",
            domain_id="Gender",
            vocabulary_id="Gender",
        ),
        StaticConcept(
            value_set="sex",
            local_value="F",
            concept_id="8532",
            concept_code="F",
            concept_name="Female",
            concept_class="Gender",
            concept_category="Standard",
            valid_flag="Valid",
            domain_id="Gender",
            vocabulary_id="Gender",
        ),
    ]


@pytest.fixture
def static_index(static_concepts) -> dict[tuple[str, str], StaticConcept]:
    return {(c.value_set, c.local_value): c for c in static_concepts}


@pytest.fixture
def structural_concepts() -> list[StructuralConcept]:
    return [
        StructuralConcept(
            value_set="ecrf",
            concept_id="32817",
            concept_code="OMOP4822053",
            concept_name="EHR encounter record",
            domain_id="Type Concept",
            vocabulary_id="Type Concept",
            valid_flag="Valid",
            concept_class="Obs Type",
            concept_category="Standard",
        ),
    ]


@pytest.fixture
def structural_index(structural_concepts) -> dict[str, StructuralConcept]:
    return {c.value_set: c for c in structural_concepts}


@pytest.fixture
def static_csv_content() -> str:
    return """\
value_set,local_value,omop_concept_id,omop_concept_code,omop_concept_name,omop_class,omop_concept_category,omop_valid_flag,omop_domain,omop_vocab
sex,M,8507,M,Male,Gender,Standard,Valid,Gender,Gender
sex,F,8532,F,Female,Gender,Standard,Valid,Gender,Gender
"""


@pytest.fixture
def static_csv_file(tmp_path, static_csv_content) -> Path:
    path = tmp_path / "static_mapping.csv"
    path.write_text(static_csv_content)
    return path


@pytest.fixture
def structural_csv_content() -> str:
    return """\
value_set,omop_concept_id,omop_concept_code,omop_concept_name,omop_class,omop_concept_category,omop_valid_flag,omop_domain,omop_vocab
ecrf,32817,OMOP4822053,EHR encounter record,Obs Type,Standard,Valid,Type Concept,Type Concept
"""


@pytest.fixture
def structural_csv_file(tmp_path, structural_csv_content) -> Path:
    path = tmp_path / "structural_mapping.csv"
    path.write_text(structural_csv_content)
    return path
