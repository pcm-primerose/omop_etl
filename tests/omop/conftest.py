import datetime as dt
from collections import defaultdict
from dataclasses import dataclass

import pytest

from omop_etl.concept_mapping.core.models import StructuralConcept, StaticConcept
from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.harmonization.models.patient import Patient
from omop_etl.semantic_mapping.core.models import SemanticRow, QueryResult, Query, BatchQueryResult


def create_patient(patient_id: str, trial: str, **scalars: str | dt.date | None | bool | float | int) -> Patient:
    patient = Patient(patient_id=patient_id, trial_id=trial)
    for attr, value in scalars.items():
        setattr(patient, attr, value)

    return patient


@dataclass(frozen=True, slots=True)
class SemanticEntry:
    """Semantic mapping entry for test fixture construction."""

    patient_id: str
    field_path: tuple[str, ...]
    leaf_index: int | None
    concept_id: int | str
    name: str
    domain: str
    vocab: str = "snomed"
    concept_code: str = ""
    concept_class: str = ""
    standard_concept: str = "standard"
    validity: str = "valid"
    source_term: str = "test"
    source_col: str = "test"
    term_id: str = "test"
    frequency: int = 1


def create_semantic_index(*entries: SemanticEntry) -> SemanticResultIndex:
    """
    Build a SemanticResultIndex from one or more SemanticEntry instances.
    Entries with the same (patient_id, field_path, leaf_index) are grouped into one QueryResult.
    """
    grouped: dict[tuple, list[SemanticRow]] = defaultdict(list)

    for e in entries:
        grouped[(e.patient_id, e.field_path, e.leaf_index)].append(
            SemanticRow(
                term_id=e.term_id,
                source_col=e.source_col,
                source_term=e.source_term,
                frequency=e.frequency,
                omop_concept_id=str(e.concept_id),
                omop_concept_code=e.concept_code or str(e.concept_id),
                omop_concept_name=e.name,
                omop_concept_class=e.concept_class,
                omop_standard_concept=e.standard_concept,
                omop_validity=e.validity,
                omop_domain=e.domain,
                omop_vocab=e.vocab,
            )
        )

    results: list[QueryResult] = []
    for (pid, fp, li), rows in grouped.items():
        query = Query(patient_id=pid, id="test", query="test", field_path=fp, raw_value="test", leaf_index=li)
        results.append(QueryResult(patient_id=pid, query=query, results=rows))

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
