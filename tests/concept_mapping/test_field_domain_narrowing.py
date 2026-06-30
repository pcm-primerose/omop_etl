"""
A single source value can map to concepts in multiple domains,
Different builders narrow to different domains at lookup time.
This must work through the real extract-match-index-lookup pipeline.

Regression test for the field/domain collision: with domain filtering at match
time, and a value-keyed index, two configs for one field_path collided and one
domain's concepts were silently dropped.
"""

import datetime as dt

import pytest

from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.patient import Patient
from omop_etl.semantic_mapping.core.models import (
    FieldConfig,
    OmopDomain,
    SemanticRow,
)
from omop_etl.semantic_mapping.core.query_extractor import extract_queries
from omop_etl.semantic_mapping.core.semantic_index import SemanticIndex

FIELD = (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME)
DRUG_CONCEPT = 1001
REGIMEN_CONCEPT = 2002


def _row(concept_id: int, domain: str) -> SemanticRow:
    return SemanticRow(
        term_id="t",
        source_col="TRT",
        source_term="erivedge",
        frequency=1,
        omop_concept_id=str(concept_id),
        omop_concept_code=str(concept_id),
        omop_concept_name="x",
        omop_concept_class="",
        omop_standard_concept="standard",
        omop_validity="valid",
        omop_domain=domain,
        omop_vocab="rxnorm",
    )


def _patient_with_cycle() -> Patient:
    patient = Patient(patient_id="p1", trial_id="test")
    cycle = TreatmentCycleComponent(patient_id="p1")
    cycle.source_treatment_name = "Erivedge"
    cycle.treatment_number = 1
    cycle.cycle_number = 1
    cycle.start_date = dt.date(2023, 1, 1)
    patient.treatment_cycles = [cycle]
    return patient


def _build_index(configs: tuple[FieldConfig, ...]) -> SemanticResultIndex:
    # the source term maps to both DRUG and REGIMEN concepts
    corpus = {"erivedge": [_row(DRUG_CONCEPT, "drug"), _row(REGIMEN_CONCEPT, "regimen")]}
    queries = extract_queries(_patient_with_cycle(), list(configs))
    batch = SemanticIndex(corpus).lookup_exact(queries)
    return SemanticResultIndex.from_batch(batch)


def test_one_field_two_domains_both_resolve():
    # two builders look up the same field under different domains, both must resolve
    configs = (
        FieldConfig(name="drug", field_path=FIELD),
        FieldConfig(name="regimen", field_path=FIELD),
    )
    service = ConceptLookupService(static_index={}, semantic_index=_build_index(configs))

    drug = service.resolve(FIELD, "Erivedge", domains={OmopDomain.DRUG})
    regimen = service.resolve(FIELD, "Erivedge", domains={OmopDomain.REGIMEN})

    assert [c.concept_id for c in drug] == [DRUG_CONCEPT]
    assert [c.concept_id for c in regimen] == [REGIMEN_CONCEPT]


def test_unconfigured_field_raises():
    # looking up a field_path not in the configured allow-list is a wiring error
    index = _build_index((FieldConfig(name="x", field_path=FIELD),))
    service = ConceptLookupService(static_index={}, semantic_index=index, semantic_field_paths={FIELD})
    other_field = (Patient.Collections.ADVERSE_EVENTS, "term")

    with pytest.raises(ValueError, match="unconfigured field_path"):
        service.resolve(other_field, "anything", domains={OmopDomain.DRUG})


def test_configured_but_unmapped_value_returns_empty():
    # a configured field whose value isn't matched is a data gap (-> 0), not an error
    index = _build_index((FieldConfig(name="x", field_path=FIELD),))
    service = ConceptLookupService(static_index={}, semantic_index=index, semantic_field_paths={FIELD})

    result = service.resolve(FIELD, "not-a-real-drug", domains={OmopDomain.DRUG})

    assert result == ()
