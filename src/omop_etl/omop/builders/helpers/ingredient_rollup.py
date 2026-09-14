import polars as pl

from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.vocabulary.core.vocabulary import Vocabulary

INGREDIENT_EXPOSURES_SCHEMA = {
    "person_id": pl.Int64,
    "ingredient_concept_id": pl.Int64,
    "drug_exposure_id": pl.Int64,
    "start": pl.Date,
    "end": pl.Date,
}


def ingredient_exposures(
    drug_exposure: list[DrugExposureRow],
    concept_ancestor: pl.DataFrame,
    vocabulary: Vocabulary,
) -> pl.DataFrame:
    """
    Roll up each drug_exposure row's drug_concept_id to its Ingredient-class
    ancestor(s) via CONCEPT_ANCESTOR. Shared by DrugEraBuilder and DoseEraBuilder.

    A combination-drug exposure (one drug_concept_id with multiple Ingredient
    ancestors) expands to one row per ingredient, via a plain join, no assumption
    of a 1:1 drug to ingredient mapping.

    Ancestor filtering is domain_id == "Drug" and concept_class_id == "Ingredient",
    not restricted to vocabulary_id == "RxNorm", since our mappings use "RxNorm Extension"
    for several ingredients and that restriction would drop these.

    Excludes drug_concept_id == 0 (unmapped) and exposures with no Ingredient-class
    ancestor at all.

    Returns one row per (exposure, ingredient) pair: `person_id`,
    `ingredient_concept_id`, `drug_exposure_id` (to re-join back to the original
    DrugExposureRow for whatever else a caller needs: quantity, days_supply, ...),
    `start`, `end`.
    """
    mapped = [row for row in drug_exposure if row.drug_concept_id != 0]
    if not mapped or concept_ancestor.is_empty():
        return pl.DataFrame(schema=INGREDIENT_EXPOSURES_SCHEMA)

    ingredient_ancestor_ids = {
        ancestor_id for ancestor_id in {int(cid) for cid in concept_ancestor.get_column("ancestor_concept_id")} if _is_ingredient(vocabulary, ancestor_id)
    }
    if not ingredient_ancestor_ids:
        return pl.DataFrame(schema=INGREDIENT_EXPOSURES_SCHEMA)

    ancestor_links = (
        concept_ancestor.with_columns(
            ancestor_concept_id=pl.col("ancestor_concept_id").cast(pl.Int64),
            descendant_concept_id=pl.col("descendant_concept_id").cast(pl.Int64),
        )
        .filter(pl.col("ancestor_concept_id").is_in(ingredient_ancestor_ids))
        .select("ancestor_concept_id", "descendant_concept_id")
        .unique()
    )

    exposures = pl.DataFrame(
        {
            "person_id": [row.person_id for row in mapped],
            "drug_concept_id": [row.drug_concept_id for row in mapped],
            "drug_exposure_id": [row.drug_exposure_id for row in mapped],
            "start": [row.drug_exposure_start_date for row in mapped],
            "end": [row.drug_exposure_end_date for row in mapped],
        }
    )

    return exposures.join(ancestor_links, left_on="drug_concept_id", right_on="descendant_concept_id", how="inner").select(
        "person_id",
        pl.col("ancestor_concept_id").alias("ingredient_concept_id"),
        "drug_exposure_id",
        "start",
        "end",
    )


def _is_ingredient(vocabulary: Vocabulary, concept_id: int) -> bool:
    concept = vocabulary.hydrate(concept_id)
    return concept is not None and concept.domain_id == "Drug" and concept.concept_class_id == "Ingredient"
