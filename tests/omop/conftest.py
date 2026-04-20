import datetime as dt
from collections import defaultdict
from unittest.mock import Mock

import pytest

from omop_etl.concept_mapping.core.models import MappedConcept, StructuralConcept, StaticConcept
from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.patient import Patient
from omop_etl.semantic_mapping.core.models import SemanticRow, QueryResult, Query, BatchQueryResult


def create_patient(patient_id: str, trial: str, **scalars: str | dt.date | None | bool | float | int) -> Patient:
    patient = Patient(patient_id=patient_id, trial_id=trial)
    for attr, value in scalars.items():
        setattr(patient, attr, value)

    return patient


def create_semantic_index(
    patient_id: str,
    field_path: tuple[str, str],
    leaf_index: int | None,
    concept_id,
    omop_name,
    domain,
    query_text: str | None = "test",
    source_term: str | None = "test",
    source_col: str | None = "test",
    term_id: str | None = "test",
    validity: str | None = "valid",
    frequency: int | None = 1,
    omop_standard_concept: str | None = "standard",
    concept_code: str | None = None,
    vocab: str | None = None,
    omop_concept_class: str | None = None,
) -> SemanticResultIndex:
    grouped: dict[tuple, list[SemanticRow]] = defaultdict(list)

    grouped[(patient_id, field_path, leaf_index)].append(
        SemanticRow(
            term_id=term_id,
            source_col=source_col,
            source_term=source_term,
            frequency=frequency,
            omop_concept_id=concept_id,
            omop_domain=domain,
            omop_concept_name=omop_name,
            omop_concept_code=concept_code,
            omop_vocab=vocab,
            omop_standard_concept=omop_standard_concept,
            omop_validity=validity,
            omop_concept_class=omop_concept_class,
        )
    )

    results: list[QueryResult] = []
    for (patient_id, field_path, leaf_index), rows in grouped.items():
        query_text = Query(patient_id=patient_id, id="test", query=query_text, field_path=field_path, raw_value="test", leaf_index=leaf_index)
        results.append(QueryResult(patient_id=patient_id, query=query_text, results=rows))

    return SemanticResultIndex.from_batch(BatchQueryResult(results=tuple(results)))


def _structural(value_set: str, concept_id: str, domain_id: str) -> StructuralConcept:
    return StructuralConcept(
        value_set=value_set,
        concept_id=concept_id,
        concept_code="",
        concept_name="",
        domain_id=domain_id,
        vocabulary_id="",
        validity="valid",
        concept_class="",
        standard_concept="standard",
    )


def _static(value_set: str, local_value: str, concept_id: str, domain_id: str) -> StaticConcept:
    return StaticConcept(
        value_set=value_set,
        local_value=local_value,
        concept_id=concept_id,
        concept_code="",
        concept_name="",
        concept_class="",
        standard_concept="standard",
        validity="valid",
        domain_id=domain_id,
        vocabulary_id="",
    )


@pytest.fixture
def structural_index() -> dict[str, StructuralConcept]:
    return {
        "ecrf": _structural("ecrf", "32817", "type concept"),
        "outpatient_visit": _structural("outpatient_visit", "9202", "visit"),
        "iv": _structural("iv", "4171047", "route"),
        "oral": _structural("oral", "4132161", "route"),
        "cdm": _structural("cdm", "705800", "metadata"),
        "vocab": _structural("vocab", "1146958", "metadata"),
    }


@pytest.fixture
def static_index() -> dict[tuple[str, str], StaticConcept]:
    return {
        ("sex", "m"): _static("sex", "m", "8507", "gender"),
        ("sex", "f"): _static("sex", "f", "8532", "gender"),
    }


@pytest.fixture
def male_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=8507,
        concept_code="M",
        concept_name="Male",
        domain_id="Gender",
        vocabulary_id="Gender",
        validity="S",
    )


@pytest.fixture
def female_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=8532,
        concept_code="F",
        concept_name="Female",
        domain_id="Gender",
        vocabulary_id="Gender",
        validity="S",
    )


@pytest.fixture
def ecrf_type_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=32817,
        concept_code="OMOP4822053",
        concept_name="EHR encounter record",
        domain_id="Type Concept",
        vocabulary_id="Type Concept",
        validity="S",
    )


@pytest.fixture
def cdm_version_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=756265,
        concept_code="v5.4",
        concept_name="CDM v5.4",
        domain_id="Metadata",
        vocabulary_id="CDM",
        validity="S",
    )


@pytest.fixture
def vocab_version_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=0,
        concept_code="v5.0",
        concept_name="Vocabulary v5.0",
        domain_id="Metadata",
        vocabulary_id="Vocabulary",
        validity="S",
    )


@pytest.fixture
def mock_concepts(
    male_concept,
    female_concept,
    ecrf_type_concept,
    cdm_version_concept,
    vocab_version_concept,
) -> Mock:
    mock = Mock(spec=ConceptLookupService)

    def lookup_static(value_set: str, value: str, **kwargs):
        if value_set == "sex":
            if value == "M":
                return male_concept
            if value == "F":
                return female_concept
        return None

    def lookup_structural(value_set: str, **kwargs):
        if value_set == "ecrf":
            return ecrf_type_concept
        if value_set == "cdm":
            return cdm_version_concept
        if value_set == "vocab":
            return vocab_version_concept
        return None

    mock.lookup_static.side_effect = lookup_static
    mock.lookup_structural.side_effect = lookup_structural
    return mock


@pytest.fixture
def patient_complete() -> Patient:
    """Patient with all required fields populated."""
    p = Patient(patient_id="P001", trial_id="TEST")
    p.date_of_birth = dt.date(1980, 5, 15)
    p.sex = "M"
    p.treatment_start_date = dt.date(2023, 1, 1)
    p.end_of_treatment_date = dt.date(2023, 6, 30)
    return p


@pytest.fixture
def patient_female() -> Patient:
    """Female patient with all required fields."""
    p = Patient(patient_id="P002", trial_id="TEST")
    p.date_of_birth = dt.date(1990, 3, 20)
    p.sex = "F"
    p.treatment_start_date = dt.date(2023, 2, 1)
    p.end_of_treatment_date = dt.date(2023, 7, 15)
    return p


@pytest.fixture
def patient_missing_dob() -> Patient:
    """Patient without date of birth (required for Person)."""
    p = Patient(patient_id="P003", trial_id="TEST")
    p.sex = "M"
    p.treatment_start_date = dt.date(2023, 1, 1)
    p.end_of_treatment_date = dt.date(2023, 6, 30)
    return p


@pytest.fixture
def patient_missing_treatment_start() -> Patient:
    """Patient without treatment start date (required for ObservationPeriod)."""
    p = Patient(patient_id="P004", trial_id="TEST")
    p.date_of_birth = dt.date(1985, 8, 10)
    p.sex = "F"
    return p


@pytest.fixture
def patient_no_sex() -> Patient:
    """Patient without sex mapping."""
    p = Patient(patient_id="P005", trial_id="TEST")
    p.date_of_birth = dt.date(1975, 12, 1)
    p.treatment_start_date = dt.date(2023, 3, 1)
    return p


@pytest.fixture
def patient_minimal() -> Patient:
    """Patient with only required fields for both Person and ObservationPeriod."""
    p = Patient(patient_id="P006", trial_id="TEST")
    p.date_of_birth = dt.date(2000, 1, 1)
    p.treatment_start_date = dt.date(2024, 1, 1)
    return p


@pytest.fixture
def outpatient_visit_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=9202,
        concept_code="OP",
        concept_name="Outpatient Visit",
        domain_id="Visit",
        vocabulary_id="Visit",
        validity="S",
    )


@pytest.fixture
def mock_concepts_visit(
    male_concept,
    female_concept,
    ecrf_type_concept,
    cdm_version_concept,
    vocab_version_concept,
    outpatient_visit_concept,
) -> Mock:
    """Mock ConceptLookupService for visit occurrence builder."""
    mock = Mock(spec=ConceptLookupService)

    def lookup_static(value_set: str, value: str, **kwargs):
        if value_set == "sex":
            if value == "M":
                return male_concept
            if value == "F":
                return female_concept
        return None

    def lookup_structural(name: str, **kwargs):
        if name == "outpatient_visit":
            return outpatient_visit_concept
        if name == "ecrf":
            return ecrf_type_concept
        return None

    mock.lookup_static.side_effect = lookup_static
    mock.lookup_structural.side_effect = lookup_structural
    return mock
