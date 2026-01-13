import datetime as dt
from unittest.mock import Mock

import pytest

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.patient import Patient


@pytest.fixture
def male_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=8507,
        concept_code="M",
        concept_name="Male",
        domain_id="Gender",
        vocabulary_id="Gender",
        standard_flag="S",
    )


@pytest.fixture
def female_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=8532,
        concept_code="F",
        concept_name="Female",
        domain_id="Gender",
        vocabulary_id="Gender",
        standard_flag="S",
    )


@pytest.fixture
def ecrf_type_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=32817,
        concept_code="OMOP4822053",
        concept_name="EHR encounter record",
        domain_id="Type Concept",
        vocabulary_id="Type Concept",
        standard_flag="S",
    )


@pytest.fixture
def cdm_version_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=756265,
        concept_code="v5.4",
        concept_name="CDM v5.4",
        domain_id="Metadata",
        vocabulary_id="CDM",
        standard_flag="S",
    )


@pytest.fixture
def vocab_version_concept() -> MappedConcept:
    return MappedConcept(
        concept_id=0,
        concept_code="v5.0",
        concept_name="Vocabulary v5.0",
        domain_id="Metadata",
        vocabulary_id="Vocabulary",
        standard_flag="S",
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

    def lookup_static(value_set: str, value: str):
        if value_set == "sex":
            if value == "M":
                return male_concept
            if value == "F":
                return female_concept
        return None

    def lookup_structural(value_set: str):
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
    p.treatment_end_date = dt.date(2023, 6, 30)
    return p


@pytest.fixture
def patient_female() -> Patient:
    """Female patient with all required fields."""
    p = Patient(patient_id="P002", trial_id="TEST")
    p.date_of_birth = dt.date(1990, 3, 20)
    p.sex = "F"
    p.treatment_start_date = dt.date(2023, 2, 1)
    p.treatment_end_date = dt.date(2023, 7, 15)
    return p


@pytest.fixture
def patient_missing_dob() -> Patient:
    """Patient without date of birth (required for Person)."""
    p = Patient(patient_id="P003", trial_id="TEST")
    p.sex = "M"
    p.treatment_start_date = dt.date(2023, 1, 1)
    p.treatment_end_date = dt.date(2023, 6, 30)
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
        standard_flag="S",
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

    def lookup_static(value_set: str, value: str):
        if value_set == "sex":
            if value == "M":
                return male_concept
            if value == "F":
                return female_concept
        return None

    def lookup_structural(name: str):
        if name == "outpatient_visit":
            return outpatient_visit_concept
        if name == "ecrf":
            return ecrf_type_concept
        return None

    mock.lookup_static.side_effect = lookup_static
    mock.lookup_structural.side_effect = lookup_structural
    return mock
