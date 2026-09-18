# Table-name registries for the DB loader, derived from cdm5.5_ddl.sql.

# Athena-bundle tables: one CSV per table in athena_dir (e.g. CONCEPT.csv to concept)
VOCAB_TABLES: frozenset[str] = frozenset(
    {
        "concept",
        "vocabulary",
        "domain",
        "concept_class",
        "concept_relationship",
        "relationship",
        "concept_synonym",
        "concept_ancestor",
        "drug_strength",
        "pack_content",
        "concept_metadata",
        "concept_relationship_metadata",
    }
)

# Every table cdm5.5_ddl.sql declares.
ALL_TABLES = frozenset(
    {
        "care_site",
        "cdm_source",
        "cohort",
        "cohort_definition",
        "concept",
        "concept_ancestor",
        "concept_class",
        "concept_metadata",
        "concept_relationship",
        "concept_relationship_metadata",
        "concept_synonym",
        "condition_era",
        "condition_occurrence",
        "cost",
        "death",
        "device_exposure",
        "domain",
        "dose_era",
        "drug_era",
        "drug_exposure",
        "drug_strength",
        "episode",
        "episode_event",
        "fact_relationship",
        "location",
        "measurement",
        "metadata",
        "note",
        "note_nlp",
        "observation",
        "observation_period",
        "pack_content",
        "payer_plan_period",
        "person",
        "procedure_occurrence",
        "provider",
        "relationship",
        "source_to_concept_map",
        "specimen",
        "visit_detail",
        "visit_occurrence",
        "vocabulary",
    }
)
