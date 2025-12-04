from omop_etl.semantic_mapping.models import FieldConfig, OmopDomain, QueryTarget

# class PreviousTreatments:
#     def __init__(self, patient_id: str):
#         self._patient_id = patient_id
#         self._treatment: Optional[str] = None
#         self._additional_treatment: Optional[str] = None

DEFAULT_FIELD_CONFIGS: tuple[FieldConfig, ...] = (
    #
    # medical history
    FieldConfig(
        name="biomarkers.gene_and_mutation",
        field_path=("medicalhistory", "term"),
        target=QueryTarget(domains={OmopDomain.CONDITION, OmopDomain.MEASUREMENTS}),
        tags={"medical_historyterm"},
    ),
    # biomarkers
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
    # tumor data
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
