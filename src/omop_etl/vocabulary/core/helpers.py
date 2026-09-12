def validity_from_invalid_reason(invalid_reason: str) -> str:
    """OMOP convention: blank `invalid_reason` = valid, `D`/`U` (or anything else) = invalid."""
    return "valid" if invalid_reason.strip() == "" else "invalid"


# S=standard, C=classification, nothing=non-standard
ACCEPTABLE_STANDARD_CONCEPT_VALUES = {"S", "C"}


ATHENA_CONCEPT_COLUMNS = (
    "concept_id",
    "concept_name",
    "domain_id",
    "vocabulary_id",
    "concept_class_id",
    "standard_concept",
    "concept_code",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
)

CONCEPT_ANCESTOR_COLUMNS = (
    "ancestor_concept_id",
    "descendant_concept_id",
    "min_levels_of_separation",
    "max_levels_of_separation",
)

# every file the CDM vocab schema needs, loaded from Athena bundle
REQUIRED_ATHENA_FILES = (
    "CONCEPT.csv",
    "VOCABULARY.csv",
    "DOMAIN.csv",
    "CONCEPT_CLASS.csv",
    "CONCEPT_RELATIONSHIP.csv",
    "RELATIONSHIP.csv",
    "CONCEPT_SYNONYM.csv",
    "CONCEPT_ANCESTOR.csv",
    "DRUG_STRENGTH.csv",
)
