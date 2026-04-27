from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.semantic_mapping.core.models import (
    FieldConfig,
    OmopDomain,
    QueryTarget,
)


DEFAULT_FIELD_CONFIGS: tuple[FieldConfig, ...] = (
    # adverse events
    FieldConfig(
        name="adverse_event.term",
        field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS, OmopDomain.MEAS_VALUE}),
        tags={"adverse_event", "term"},
    ),
    # concomitant medications
    FieldConfig(
        name="concomitant_medications.medication_name",
        field_path=(Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
        target=QueryTarget(domains={OmopDomain.DRUG}),
        tags={"concomitant_medication", "medication", "drug"},
    ),
    # previous treatments
    FieldConfig(
        name="previous_treatments.treatment",
        field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
        target=QueryTarget(domains={OmopDomain.PROCEDURE, OmopDomain.DRUG}),
        tags={"previous_treatments", "term"},
    ),
    FieldConfig(
        name="previous_treatments.additional_treatment",
        field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
        target=QueryTarget(domains={OmopDomain.PROCEDURE, OmopDomain.DRUG}),
        tags={"previous_treatments", "additional_term"},
    ),
    # medical history — same shape as adverse_event.term: free-text, may map to a
    # Measurement attribute + Meas Value qualifier (e.g. "decreased hemoglobin").
    FieldConfig(
        name="medical_history.term",
        field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS, OmopDomain.PROCEDURE, OmopDomain.MEAS_VALUE}),
        tags={"medical_history", "term"},
    ),
    # biomarkers
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=(Patient.Singletons.BIOMARKERS, Biomarkers.Fields.GENE_AND_MUTATION),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarker", "gene and mutation"},
    ),
    FieldConfig(
        name="biomarkers.cohort_target_name",
        field_path=(Patient.Singletons.BIOMARKERS, Biomarkers.Fields.COHORT_TARGET_NAME),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarker", "cohort target name"},
    ),
    FieldConfig(
        name="biomarkers.cohort_target_mutation",
        field_path=(Patient.Singletons.BIOMARKERS, Biomarkers.Fields.COHORT_TARGET_MUTATION),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarker", "cohort target mutation"},
    ),
    # study drugs
    FieldConfig(
        name="study_drugs.primary",
        field_path=(Patient.Singletons.STUDY_DRUGS, StudyDrugs.Fields.PRIMARY_TREATMENT_DRUG),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"study_drug", "primary"},
    ),
    FieldConfig(
        name="study_drugs.secondary",
        field_path=(Patient.Singletons.STUDY_DRUGS, StudyDrugs.Fields.SECONDARY_TREATMENT_DRUG),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"study_drug", "secondary"},
    ),
    # treatment cycles
    FieldConfig(
        name="treatment_cycles.source_treatment_name",
        field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"treatment_cycle", "drug", "source"},
    ),
    FieldConfig(
        name="treatment_cycles.ingredient_name",
        field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"treatment_cycle", "drug", "ingredient"},
    ),
    # tumor
    FieldConfig(
        name="tumor.main",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "main"},
    ),
    FieldConfig(
        name="tumor.other",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.OTHER_TUMOR_TYPE),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "other"},
    ),
    FieldConfig(
        name="tumor.cohort",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.COHORT_TUMOR_TYPE),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "cohort"},
    ),
    FieldConfig(
        name="tumor.icd10",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "icd10"},
    ),
    FieldConfig(
        name="tumor.icd10_description",
        field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_DESCRIPTION),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "icd10_description"},
    ),
    FieldConfig(
        name="ecog_baseline.grade",
        field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.GRADE),
        target=QueryTarget(domains={OmopDomain.MEASUREMENTS, OmopDomain.MEAS_VALUE}),
        tags={"ecog_basline_grade"},
    ),
    FieldConfig(
        name="ecog_baseline.description",
        field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.DESCRIPTION),
        target=QueryTarget(domains={OmopDomain.MEASUREMENTS, OmopDomain.MEAS_VALUE}),
        tags={"ecog_basline_description"},
    ),
)
