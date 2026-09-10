import polars as pl

# The Athena-aligned concept columns every mapping file (static/structural/semantic)
# carries — identical to Athena's own CONCEPT columns, except `validity` (stores the
# derived "valid"/"invalid" form, not raw `invalid_reason`). Genuinely shared across
# concept_mapping, semantic_mapping, and vocabulary — the single source of truth for
# the header names, so a rename is a one-place edit instead of a multi-file hunt.
MAPPING_CONCEPT_COLUMNS = (
    "concept_id",
    "concept_code",
    "concept_name",
    "concept_class_id",
    "standard_concept",
    "validity",
    "domain_id",
    "vocabulary_id",
)


def read_mapping_csv(source) -> pl.DataFrame:
    """
    Read a mapping CSV (static/structural/semantic) — the single place that knows
    how: skips the leading `#` documentation preamble every mapping file carries,
    reads every column as str (matching `MAPPING_CONCEPT_COLUMNS` consumers'
    expectations). `source` is anything `pl.read_csv` accepts (a path, or an
    already-open file object, e.g. semantic_mapping's `Traversable.open()` result).
    """
    return pl.read_csv(source, comment_prefix="#", infer_schema_length=0)
