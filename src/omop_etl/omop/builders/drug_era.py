import polars as pl

from omop_etl.omop.builders.helpers.ingredient_rollup import ingredient_exposures
from omop_etl.omop.builders.helpers.intervals import collapse_intervals
from omop_etl.omop.core.id_generator import RowIdGenerator
from omop_etl.omop.models.rows import DrugEraRow, DrugExposureRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.vocabulary import Vocabulary


class DrugEraBuilder:
    """
    Derives drug_era rows from already-built drug_exposure rows:
    one era per (person_id, ingredient_concept_id), rolling each exposure's drug_concept_id up
    to its Ingredient-class ancestor(s) from CONCEPT_ANCESTOR, then collapsing in two
    phases (OHDSI standard):
    - first merge overlapping exposures into sub-exposures (persistence_days=0, so gap_days
      reflects only genuinely uncovered time)
    - then merge sub-exposures separated by up to a 30-day gap into the final era.

    The ingredient rollup itself (the ancestor join + Ingredient-class filtering,
    including why it's not restricted to vocabulary_id == "RxNorm") is shared with
    DoseEraBuilder, see `ingredient_rollup.ingredient_exposures`.
    """

    OVERLAP_PERSISTENCE_DAYS = 0
    ERA_PERSISTENCE_DAYS = 30

    def __init__(self, row_id_generator: RowIdGenerator):
        self._row_id_generator = row_id_generator

    def build(
        self,
        drug_exposure: list[DrugExposureRow],
        concept_ancestor: pl.DataFrame,
        vocabulary: Vocabulary,
    ) -> list[DrugEraRow]:
        exposures = ingredient_exposures(drug_exposure, concept_ancestor, vocabulary)
        if exposures.is_empty():
            return []

        sub_exposures = collapse_intervals(
            exposures,
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
                drug_era_id=self._row_id_generator.generate(
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
