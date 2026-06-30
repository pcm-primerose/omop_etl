import datetime as dt
from dataclasses import dataclass
import pytest

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.concept_mapping.core.semantic_loader import SemanticResultIndex
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.id_generator import sha256_bigint
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
        person_id = sha256_bigint("person", patient.patient_id)
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
    event_date: dt.date | None = None,
) -> None:
    """
    Test helper: simulate ConditionOccurrenceBuilder publishing the primary
    cancer condition_occurrence row, so MeasurementBuilder can resolve the
    tumor SourceReference for lesion-size / biomarker FK linkage, and
    EpisodeBuilder can read the condition's start date for the Disease Episode.
    """
    ctx.publish_rows(
        OmopTables.CONDITION_OCCURRENCE,
        SourceReference(tumor.patient_id, Patient.Singletons.TUMOR_TYPE, tumor.natural_key()),
        [
            OmopRowReference(
                table=OmopTables.CONDITION_OCCURRENCE,
                row_id=condition_row_id,
                primary_concept_id=concept_id,
                event_date=event_date,
            )
        ],
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
class _SemanticGroup:
    """One (field_path, value) -> concept(s) entry produced by mapping()."""

    field_path: tuple[str, ...]
    value: str
    rows: tuple[SemanticRow, ...]


def concept(
    concept_id: int | str,
    domain: str,
    *,
    name: str = "concept",
    vocab: str = "snomed",
    concept_code: str = "",
    concept_class: str = "",
    standard_concept: str = "standard",
    validity: str = "valid",
) -> SemanticRow:
    """
    One OMOP concept a semantic term resolves to.
    """
    return SemanticRow(
        term_id="test",
        source_col="test",
        source_term="test",
        frequency=1,
        omop_concept_id=str(concept_id),
        omop_concept_code=concept_code or str(concept_id),
        omop_concept_name=name,
        omop_concept_class=concept_class,
        omop_standard_concept=standard_concept,
        omop_validity=validity,
        omop_domain=domain,
        omop_vocab=vocab,
    )


def mapping(field_path: tuple[str, ...], value: str, *concepts: SemanticRow) -> _SemanticGroup:
    """Group the concept(s) a semantic term (field_path, value) resolves to."""
    return _SemanticGroup(field_path=field_path, value=value, rows=concepts)


def semantic_index(*groups: _SemanticGroup) -> SemanticResultIndex:
    """
    Build a SemanticResultIndex from mapping() groups.
    Each group is one (field_path, value) -> concept(s) entry.
    """
    results: list[QueryResult] = []
    for g in groups:
        query = Query(patient_id="test", id="test", query=g.value, field_path=g.field_path, raw_value=g.value)
        results.append(QueryResult(patient_id="test", query=query, results=list(g.rows)))

    return SemanticResultIndex.from_batch(BatchQueryResult(results=tuple(results)))


def _structural(concept_id: int, domain_id: str) -> MappedConcept:
    # the value_set key is carried by the caller
    return MappedConcept(
        concept_id=concept_id,
        concept_code="",
        concept_name="",
        domain_id=domain_id,
        vocabulary_id="",
        validity="valid",
    )


def _static(concept_id: int, domain_id: str) -> MappedConcept:
    # the (value_set, local_value) key is carried by the caller
    return MappedConcept(
        concept_id=concept_id,
        concept_code="",
        concept_name="",
        domain_id=domain_id,
        vocabulary_id="",
        validity="valid",
    )


@pytest.fixture
def structural_index() -> dict[str, MappedConcept]:
    return {
        "ecrf": _structural(32809, "type concept"),
        "patient_withdrawn": _structural(4087907, "observation"),
        "outpatient_visit": _structural(9202, "visit"),
        "iv": _structural(4171047, "route"),
        "oral": _structural(4132161, "route"),
        "cdm": _structural(705800, "metadata"),
        "vocab": _structural(1146958, "metadata"),
        "ecog": _structural(36305384, "measurement"),
        # measurement builder: target lesion absolute size
        "lesion_size": _structural(36768664, "measurement"),
        # measurement builder: C30 questions
        "c30_q1": _structural(701340, "measurement"),
        "c30_q2": _structural(701341, "measurement"),
        "c30_q29": _structural(701367, "measurement"),
        # EQ5D VAS
        "eq5d_qol_score": _structural(42537274, "measurement"),
        "response_recist": _structural(734317, "measurement"),
        "response_irecist": _structural(734318, "measurement"),
        "response_ranop": _structural(734345, "measurement"),
        # episode builder: Episode-vocabulary kind concepts
        "treatment_regimen": _structural(32531, "episode"),
        "treatment_cycle": _structural(32532, "episode"),
        "disease_episode": _structural(32533, "episode"),
        # cohort_definition builder: subject domain concept (Person table)
        "cohort_subject": _structural(1147314, "metadata"),
    }


@pytest.fixture
def static_index() -> dict[tuple[str, str], MappedConcept]:
    return {
        ("sex", "m"): _static(8507, "gender"),
        ("sex", "f"): _static(8532, "gender"),
        ("cdm_field", "condition_occurrence.condition_occurrence_id"): _static(1147127, "metadata"),
        ("cdm_field", "drug_exposure.drug_exposure_id"): _static(1147094, "metadata"),
        ("cdm_field", "measurement.measurement_id"): _static(1147139, "metadata"),
        ("ecog_code", "1"): _static(36310827, "meas value"),
        ("ecog_code", "0"): _static(36309661, "meas value"),
        # C30 shared answer scale (Q1–Q28)
        ("c30_answer_code", "1"): _static(45883172, "meas value"),
        ("c30_answer_code", "2"): _static(45876949, "meas value"),
        ("c30_answer_code", "3"): _static(45884456, "meas value"),
        ("c30_answer_code", "4"): _static(45885256, "meas value"),
        # C30 global answer scale (Q29–Q30)
        ("c30_global_answer_code", "1"): _static(45878558, "meas value"),
        ("c30_global_answer_code", "2"): _static(1094227, "meas value"),
        ("c30_global_answer_code", "3"): _static(45878305, "meas value"),
        ("c30_global_answer_code", "4"): _static(45878304, "meas value"),
        ("c30_global_answer_code", "5"): _static(45878730, "meas value"),
        ("c30_global_answer_code", "6"): _static(45878254, "meas value"),
        ("c30_global_answer_code", "7"): _static(45881924, "meas value"),
        # EQ5D
        ("eq5d_q1_answer_code", "1"): _static(742346, "measurement"),
        ("eq5d_q1_answer_code", "2"): _static(742347, "measurement"),
        ("eq5d_q1_answer_code", "3"): _static(742348, "measurement"),
        ("eq5d_q1_answer_code", "4"): _static(742349, "measurement"),
        ("eq5d_q1_answer_code", "5"): _static(742350, "measurement"),
        ("eq5d_q2_answer_code", "1"): _static(742351, "measurement"),
        ("eq5d_q2_answer_code", "2"): _static(742352, "measurement"),
        ("eq5d_q2_answer_code", "3"): _static(742353, "measurement"),
        ("eq5d_q2_answer_code", "4"): _static(742354, "measurement"),
        ("eq5d_q2_answer_code", "5"): _static(742355, "measurement"),
        # tumor-response scales:
        # recist
        ("response_recist", "not evaluable"): _static(45878793, "Meas value"),
        ("response_recist", "not evaluable (ne)"): _static(45878793, "Meas value"),
        ("response_recist", "complete response (cr)"): _static(1634772, "measurement"),
        ("response_recist", "partial response (pr)"): _static(1633368, "measurement"),
        ("response_recist", "stable disease (sd)"): _static(1634680, "measurement"),
        ("response_recist", "progressive disease (pd)"): _static(1633597, "measurement"),
        # irecist
        ("response_irecist", "not evaluable"): _static(45878793, "Meas value"),
        ("response_irecist", "not evaluable (ne)"): _static(45878793, "Meas value"),
        ("response_irecist", "icomplete response (cr)"): _static(1633954, "measurement"),
        ("response_irecist", "ipartial response (pr)"): _static(1635284, "measurement"),
        ("response_irecist", "istable disease"): _static(1635887, "measurement"),
        ("response_irecist", "iconfirmed progressive disease"): _static(1633423, "measurement"),
        ("response_irecist", "iunconfirmed progressive disease"): _static(1633423, "measurement"),
        # rano
        ("response_rano", "not evaluable"): _static(45878793, "Meas value"),
        ("response_rano", "not evaluable (ne)"): _static(45878793, "Meas value"),
        ("response_rano", "complete response (cr)"): _static(1634853, "measurement"),
        ("response_rano", "partial response (pr)"): _static(1634574, "measurement"),
        ("response_rano", "stable disease (sd)"): _static(1633447, "measurement"),
        ("response_rano", "progressive disease (pd)"): _static(1634653, "measurement"),
        # disease dynamic episode: response Measurement concept to dynamic Episode concept
        ("dynamic_status", "1634772"): _static(32946, "episode"),  # RECIST CR
        ("dynamic_status", "1633954"): _static(32946, "episode"),  # iRECIST CR
        ("dynamic_status", "1634853"): _static(32946, "episode"),  # RANO CR
        ("dynamic_status", "1633368"): _static(32947, "episode"),  # RECIST PR
        ("dynamic_status", "1635284"): _static(32947, "episode"),  # iRECIST PR
        ("dynamic_status", "1634574"): _static(32947, "episode"),  # RANO PR
        ("dynamic_status", "1634680"): _static(32948, "episode"),  # RECIST SD
        ("dynamic_status", "1635887"): _static(32948, "episode"),  # iRECIST SD
        ("dynamic_status", "1633447"): _static(32948, "episode"),  # RANO SD
        ("dynamic_status", "1633597"): _static(32949, "episode"),  # RECIST PD
        ("dynamic_status", "1633423"): _static(32949, "episode"),  # iRECIST PD
        ("dynamic_status", "1634653"): _static(32949, "episode"),  # RANO PD
    }
