from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.semantic_mapping.core.models import FieldConfig


# DEFAULT_FIELD_CONFIGS is the allow-list of Patient fields that are extracted for semantic mapping.
# It's domain-agnostic: a field can map to concepts in several domains, and consumers narrows
# by domain at lookup time. This config also drives the service-level wiring check,
# a `resolve()` for a field_path not declared here raises.
DEFAULT_FIELD_CONFIGS: tuple[FieldConfig, ...] = (
    # adverse events
    FieldConfig(
        name="adverse_event.term",
        field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
    ),
    # concomitant medications
    FieldConfig(
        name="concomitant_medications.medication_name",
        field_path=(Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
    ),
    # previous treatments
    FieldConfig(
        name="previous_treatments.treatment",
        field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.TREATMENT),
    ),
    FieldConfig(
        name="previous_treatments.additional_treatment",
        field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.ADDITIONAL_TREATMENT),
    ),
    # medical history
    FieldConfig(
        name="medical_history.term",
        field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
    ),
    # biomarker
    FieldConfig(
        name="biomarkers.target_biomarker",
        field_path=(Patient.Singletons.BIOMARKERS, Biomarkers.Fields.TARGET_BIOMARKER),
    ),
    # study drugs
    FieldConfig(
        name="study_drugs.primary",
        field_path=(Patient.Singletons.STUDY_DRUGS, StudyDrugs.Fields.PRIMARY_TREATMENT_DRUG),
    ),
    FieldConfig(
        name="study_drugs.secondary",
        field_path=(Patient.Singletons.STUDY_DRUGS, StudyDrugs.Fields.SECONDARY_TREATMENT_DRUG),
    ),
    # treatment cycles
    FieldConfig(
        name="treatment_cycles.source_treatment_name",
        field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
    ),
    FieldConfig(
        name="treatment_cycles.ingredient_name",
        field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
    ),
    # tumor
    FieldConfig(
        name="tumor.main",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
    ),
    FieldConfig(
        name="tumor.other",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.OTHER_TUMOR_TYPE),
    ),
    FieldConfig(
        name="tumor.cohort",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.COHORT_TUMOR_TYPE),
    ),
    FieldConfig(
        name="tumor.icd10",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
    ),
    FieldConfig(
        name="tumor.icd10_description",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_DESCRIPTION),
    ),
    # ecog baseline
    FieldConfig(
        name="ecog_baseline.grade",
        field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.GRADE),
    ),
    FieldConfig(
        name="ecog_baseline.description",
        field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.DESCRIPTION),
    ),
)
