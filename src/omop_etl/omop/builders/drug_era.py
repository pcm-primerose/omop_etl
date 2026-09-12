import polars as pl

from omop_etl.omop.builders.intervals import collapse_intervals
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import DrugEraRow, DrugExposureRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.vocabulary import Vocabulary

_INGREDIENT_EXPOSURES_SCHEMA = {
    "person_id": pl.Int64,
    "ingredient_concept_id": pl.Int64,
    "start": pl.Date,
    "end": pl.Date,
}


class DrugEraBuilder:
    """
    Derives drug_era rows from already-built drug_exposure rows:
    one era per (person_id, ingredient_concept_id), rolling each exposure's drug_concept_id up
    to its Ingredient-class ancestor(s) from CONCEPT_ANCESTOR, then collapsing in two
    phases (OHDSI standard):
    - first merge overlapping exposures into sub-exposures (persistence_days=0, so gap_days
      reflects only genuinely uncovered time)
    - then merge sub-exposures separated by up to a 30-day gap into the final era.

    A combination-drug exposure (one drug_concept_id with multiple Ingredient
    ancestors) expands into one row per ingredient, each ingredient gets its own
    era, independently, using a plain join (so no assumption of a 1:1 drug to ingredient mapping).

    Rows with drug_concept_id == 0 (unmapped) are excluded.
    Ancestor filtering is domain_id=="Drug" and concept_class_id=="Ingredient",
    not restricted to vocabulary_id == "RxNorm", since we have mappings using "RxNorm Extension"
    for several ingredients and that restriction would drop them.
    """

    OVERLAP_PERSISTENCE_DAYS = 0
    ERA_PERSISTENCE_DAYS = 30

    def build(
        self,
        drug_exposure: list[DrugExposureRow],
        concept_ancestor: pl.DataFrame,
        vocabulary: Vocabulary,
    ) -> list[DrugEraRow]:
        ingredient_exposures = self._ingredient_exposures(drug_exposure, concept_ancestor, vocabulary)
        if ingredient_exposures.is_empty():
            return []

        sub_exposures = collapse_intervals(
            ingredient_exposures,
            group_by=["person_id", "ingredient_concept_id"],
            start_col="start",
            end_col="end",
            persistence_days=self.OVERLAP_PERSISTENCE_DAYS,
        ).with_columns(days_exposed=(pl.col("interval_end") - pl.col("interval_start")).dt.total_days())

        eras = collapse_intervals(
            sub_exposures,
            group_by=["person_id", "ingredient_concept_id"],
            start_col="interval_start",
            end_col="interval_end",
            persistence_days=self.ERA_PERSISTENCE_DAYS,
            count_col="occurrence_count",
            extra_agg=[pl.col("days_exposed").sum().alias("days_exposed")],
        ).with_columns(gap_days=(pl.col("interval_end") - pl.col("interval_start")).dt.total_days() - pl.col("days_exposed"))

        return [
            DrugEraRow(
                drug_era_id=row_id(
                    OmopTables.DRUG_ERA,
                    era["person_id"],
                    era["ingredient_concept_id"],
                    era["interval_start"],
                ),
                person_id=era["person_id"],
                drug_concept_id=era["ingredient_concept_id"],
                drug_era_start_date=era["interval_start"],
                drug_era_end_date=era["interval_end"],
                drug_exposure_count=era["occurrence_count"],
                gap_days=era["gap_days"],
            )
            for era in eras.iter_rows(named=True)
        ]

    def _ingredient_exposures(
        self,
        drug_exposure: list[DrugExposureRow],
        concept_ancestor: pl.DataFrame,
        vocabulary: Vocabulary,
    ) -> pl.DataFrame:
        mapped = [row for row in drug_exposure if row.drug_concept_id != 0]
        if not mapped or concept_ancestor.is_empty():
            return pl.DataFrame(schema=_INGREDIENT_EXPOSURES_SCHEMA)

        ingredient_ancestor_ids = {
            ancestor_id
            for ancestor_id in {int(cid) for cid in concept_ancestor.get_column("ancestor_concept_id")}
            if self._is_ingredient(vocabulary, ancestor_id)
        }
        if not ingredient_ancestor_ids:
            return pl.DataFrame(schema=_INGREDIENT_EXPOSURES_SCHEMA)

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
                "start": [row.drug_exposure_start_date for row in mapped],
                "end": [row.drug_exposure_end_date for row in mapped],
            }
        )

        return exposures.join(ancestor_links, left_on="drug_concept_id", right_on="descendant_concept_id", how="inner").select(
            "person_id",
            pl.col("ancestor_concept_id").alias("ingredient_concept_id"),
            "start",
            "end",
        )

    @staticmethod
    def _is_ingredient(vocabulary: Vocabulary, concept_id: int) -> bool:
        concept = vocabulary.hydrate(concept_id)
        return concept is not None and concept.domain_id == "Drug" and concept.concept_class_id == "Ingredient"
