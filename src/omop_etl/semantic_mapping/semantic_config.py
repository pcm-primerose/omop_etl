from omop_etl.semantic_mapping.models import FieldConfig, OmopDomain, QueryTarget


DEFAULT_FIELD_CONFIGS: tuple[FieldConfig, ...] = (
    # adverse events
    FieldConfig(
        name="adverse_event.term",
        field_path=("adverse_event", "term"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"adverse_event", "term"},
    ),
    # concomitant medications
    FieldConfig(
        name="concomitant_medication.medication_name",
        field_path=("concomitant_medication", "medication_name"),
        target=QueryTarget(domains={OmopDomain.DRUG}),
        tags={"concomitant_medication", "medication", "drug"},
    ),
    # previous treatments
    FieldConfig(
        name="previous_treatments.treatment",
        field_path=("previous_treatment", "treatment"),
        target=QueryTarget(domains={OmopDomain.PROCEDURES}),
        tags={"previous_treatments", "term"},
    ),
    FieldConfig(
        name="previous_treatments.additional_treatment",
        field_path=("previous_treatment", "additional_treatment"),
        target=QueryTarget(domains={OmopDomain.PROCEDURES}),
        tags={"previous_treatments", "additional_term"},
    ),
    # medical history
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=("medical_history", "term"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"medical_history", "term"},
    ),
    # biomarkers
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=("biomarkers", "gene_and_mutation"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarker", "gene and mutation"},
    ),
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=("biomarkers", "gene_and_mutation"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarkergene and mutation"},
    ),
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=("biomarkers", "gene_and_mutation"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"biomarkergene and mutation"},
    ),
    # study drugs
    FieldConfig(
        name="study_drugs.primary",
        field_path=("study_drugs", "primary_treatment_drug"),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"study_drug", "primary"},
    ),
    FieldConfig(
        name="study_drugs.secondary",
        field_path=("study_drugs", "secondary_treatment_drug"),
        target=QueryTarget(domains={OmopDomain.DRUG, OmopDomain.DEVICE}),
        tags={"study_drug", "secondary"},
    ),
    # tumor
    FieldConfig(
        name="tumor.main",
        field_path=("tumor_type", "main_tumor_type"),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "main"},
    ),
    FieldConfig(
        name="tumor.other",
        field_path=("tumor_type", "other_tumor_type"),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "other"},
    ),
    FieldConfig(
        name="tumor.cohort",
        field_path=("tumor_type", "cohort_tumor_type"),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "cohort"},
    ),
    FieldConfig(
        name="tumor.icd10",
        field_path=("tumor_type", "icd10_code"),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "icd10"},
    ),
    FieldConfig(
        name="tumor.icd10_description",
        field_path=("tumor_type", "icd10_code"),
        target=QueryTarget(domains={OmopDomain.CONDITION}),
        tags={"tumor", "icd10_description"},
    ),
)
