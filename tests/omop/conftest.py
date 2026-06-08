import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
import pytest

from omop_etl.concept_mapping.core.models import StructuralConcept, StaticConcept
from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import (
    OmopRowReference,
    SourceReference,
)
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import (
    SemanticRow,
    QueryResult,
    Query,
    BatchQueryResult,
)


def create_build_context(patient: Patient, person_id: int | None = None) -> BuildContext:
    if person_id is None:
        person_id = sha1_bigint("person", patient.patient_id)
    return BuildContext(patient=patient, person_id=person_id)


def publish_ae_condition(
    ctx: BuildContext,
    ae: AdverseEvent,
    condition_row_id: int,
    concept_id: int = 0,
) -> None:
    """
    Test helper: simulate ConditionOccurrenceBuilder publishing a
    condition_occurrence row for an AE, so a downstream consumer can resolve
    `(ae.patient_id, "adverse_events", ae.natural_key())` -> condition_row_id."""
    ctx.publish_rows(
        OmopTables.CONDITION_OCCURRENCE,
        SourceReference(ae.patient_id, Patient.Collections.ADVERSE_EVENTS, ae.natural_key()),
        [OmopRowReference(table=OmopTables.CONDITION_OCCURRENCE, row_id=condition_row_id, primary_concept_id=concept_id)],
    )


def publish_tumor_condition(
    ctx: BuildContext,
    tumor: TumorType,
    condition_row_id: int,
    concept_id: int = 0,
) -> None:
    """
    Test helper: simulate ConditionOccurrenceBuilder publishing the primary
    cancer condition_occurrence row, so MeasurementBuilder can resolve the
    tumor SourceReference for lesion-size / biomarker FK linkage.
    """
    ctx.publish_rows(
        OmopTables.CONDITION_OCCURRENCE,
        SourceReference(tumor.patient_id, Patient.Singletons.TUMOR_TYPE, tumor.natural_key()),
        [OmopRowReference(table=OmopTables.CONDITION_OCCURRENCE, row_id=condition_row_id, primary_concept_id=concept_id)],
    )


def publish_cycle_drug_exposure(
    ctx: BuildContext,
    cycle: TreatmentCycleComponent,
    drug_exposure_row_id: int,
    concept_id: int = 0,
) -> None:
    """
    Test helper: simulate DrugExposureBuilder publishing a drug_exposure row for
    a treatment cycle, so ObservationBuilder can resolve the cycle's metadata /
    deviation modifier observations to it.
    """
    ctx.publish_rows(
        OmopTables.DRUG_EXPOSURE,
        SourceReference(cycle.patient_id, Patient.Collections.TREATMENT_CYCLES, cycle.natural_key()),
        [OmopRowReference(table=OmopTables.DRUG_EXPOSURE, row_id=drug_exposure_row_id, primary_concept_id=concept_id)],
    )


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


def _structural(value_set: str, concept_id: int, domain_id: str) -> StructuralConcept:
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


def _static(value_set: str, local_value: str, concept_id: int, domain_id: str) -> StaticConcept:
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
        "ecrf": _structural("ecrf", 32809, "type concept"),
        "patient_withdrawn": _structural("patient_withdrawn", 4087907, "observation"),
        "outpatient_visit": _structural("outpatient_visit", 9202, "visit"),
        "iv": _structural("iv", 4171047, "route"),
        "oral": _structural("oral", 4132161, "route"),
        "cdm": _structural("cdm", 705800, "metadata"),
        "vocab": _structural("vocab", 1146958, "metadata"),
        "ecog": _structural("ecog", 36305384, "measurement"),
        # measurement builder: target lesion absolute size
        "lesion_size": _structural("lesion_size", 36768664, "measurement"),
        # measurement builder: C30 questions
        "c30_q1": _structural("c30_q1", 701340, "measurement"),
        "c30_q2": _structural("c30_q2", 701341, "measurement"),
        "c30_q29": _structural("c30_q29", 701367, "measurement"),
        # EQ5D VAS
        "eq5d_qol_score": _structural("eq5d_qol_score", 42537274, "measurement"),
        "response_recist": _structural("response_recist", 734317, "measurement"),
        "response_irecist": _structural("response_irecist", 734318, "measurement"),
        "response_ranop": _structural("response_rano", 734345, "measurement"),
    }


@pytest.fixture
def static_index() -> dict[tuple[str, str], StaticConcept]:
    return {
        ("sex", "m"): _static("sex", "m", 8507, "gender"),
        ("sex", "f"): _static("sex", "f", 8532, "gender"),
        # cdm_field for FK linkage (observation_event_id / measurement_event_id
        # to condition_occurrence.condition_occurrence_id, or to drug_exposure.drug_exposure_id)
        ("cdm_field", "condition_occurrence.condition_occurrence_id"): _static(
            "cdm_field", "condition_occurrence.condition_occurrence_id", 1147127, "metadata"
        ),
        ("cdm_field", "drug_exposure.drug_exposure_id"): _static("cdm_field", "drug_exposure.drug_exposure_id", 1147094, "metadata"),
        ("ecog_code", "1"): _static("ecog_code", "1", 36310827, "meas value"),
        ("ecog_code", "0"): _static("ecog_code", "0", 36309661, "meas value"),
        # C30 shared answer scale (Q1–Q28)
        ("c30_answer_code", "1"): _static("c30_answer_code", "1", 45883172, "meas value"),
        ("c30_answer_code", "2"): _static("c30_answer_code", "2", 45876949, "meas value"),
        ("c30_answer_code", "3"): _static("c30_answer_code", "3", 45884456, "meas value"),
        ("c30_answer_code", "4"): _static("c30_answer_code", "4", 45885256, "meas value"),
        # C30 global answer scale (Q29–Q30)
        ("c30_global_answer_code", "1"): _static("c30_global_answer_code", "1", 45878558, "meas value"),
        ("c30_global_answer_code", "2"): _static("c30_global_answer_code", "2", 1094227, "meas value"),
        ("c30_global_answer_code", "3"): _static("c30_global_answer_code", "3", 45878305, "meas value"),
        ("c30_global_answer_code", "4"): _static("c30_global_answer_code", "4", 45878304, "meas value"),
        ("c30_global_answer_code", "5"): _static("c30_global_answer_code", "5", 45878730, "meas value"),
        ("c30_global_answer_code", "6"): _static("c30_global_answer_code", "6", 45878254, "meas value"),
        ("c30_global_answer_code", "7"): _static("c30_global_answer_code", "7", 45881924, "meas value"),
        # EQ5D
        ("eq5d_q1_answer_code", "1"): _static("eq5d_q1_answer_code", "1", 742346, "measurement"),
        ("eq5d_q1_answer_code", "2"): _static("eq5d_q1_answer_code", "2", 742347, "measurement"),
        ("eq5d_q1_answer_code", "3"): _static("eq5d_q1_answer_code", "3", 742348, "measurement"),
        ("eq5d_q1_answer_code", "4"): _static("eq5d_q1_answer_code", "4", 742349, "measurement"),
        ("eq5d_q1_answer_code", "5"): _static("eq5d_q1_answer_code", "5", 742350, "measurement"),
        ("eq5d_q2_answer_code", "1"): _static("eq5d_q2_answer_code", "1", 742351, "measurement"),
        ("eq5d_q2_answer_code", "2"): _static("eq5d_q2_answer_code", "2", 742352, "measurement"),
        ("eq5d_q2_answer_code", "3"): _static("eq5d_q2_answer_code", "3", 742353, "measurement"),
        ("eq5d_q2_answer_code", "4"): _static("eq5d_q2_answer_code", "4", 742354, "measurement"),
        ("eq5d_q2_answer_code", "5"): _static("eq5d_q2_answer_code", "5", 742355, "measurement"),
        # tumor-response scales:
        # recist
        ("response_recist", "not evaluable"): _static("response_recist", "Not evaluable", 45878793, "Meas value"),
        ("response_recist", "not evaluable (ne)"): _static("response_recist", "Not evaluable (NE)", 45878793, "Meas value"),
        ("response_recist", "complete response (cr)"): _static("response_recist", "complete response (cr)", 1634772, "measurement"),
        ("response_recist", "partial response (pr)"): _static("response_recist", "partial response (pr)", 1633368, "measurement"),
        ("response_recist", "stable disease (sd)"): _static("response_recist", "stable disease (sd)", 1634680, "measurement"),
        ("response_recist", "progressive disease (pd)"): _static("response_recist", "progressive disease (pd)", 1633597, "measurement"),
        # irecist
        ("response_irecist", "not evaluable"): _static("response_irecist", "Not evaluable", 45878793, "Meas value"),
        ("response_irecist", "not evaluable (ne)"): _static("response_irecist", "Not evaluable (NE)", 45878793, "Meas value"),
        ("response_irecist", "icomplete response (cr)"): _static("response_irecist", "icomplete response (cr)", 1633954, "measurement"),
        ("response_irecist", "ipartial response (pr)"): _static("response_irecist", "ipartial response (pr)", 1635284, "measurement"),
        ("response_irecist", "istable disease"): _static("response_irecist", "istable disease", 1635887, "measurement"),
        ("response_irecist", "iconfirmed progressive disease"): _static("response_irecist", "iconfirmed progressive disease", 1633423, "measurement"),
        ("response_irecist", "iunconfirmed progressive disease"): _static("response_irecist", "iunconfirmed progressive disease", 1633423, "measurement"),
        # rano
        ("response_rano", "not evaluable"): _static("response_rano", "Not evaluable", 45878793, "Meas value"),
        ("response_rano", "not evaluable (ne)"): _static("response_rano", "Not evaluable (NE)", 45878793, "Meas value"),
        ("response_rano", "complete response (cr)"): _static("response_rano", "complete response (cr)", 1634853, "measurement"),
        ("response_rano", "partial response (pr)"): _static("response_rano", "partial response (pr)", 1634574, "measurement"),
        ("response_rano", "stable disease (sd)"): _static("response_rano", "stable disease (sd)", 1633447, "measurement"),
        ("response_rano", "progressive disease (pd)"): _static("response_rano", "progressive disease (pd)", 1634653, "measurement"),
    }
