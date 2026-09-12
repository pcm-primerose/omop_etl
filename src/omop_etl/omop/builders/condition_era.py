import datetime as dt
import polars as pl

from omop_etl.omop.builders.intervals import collapse_intervals
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import ConditionEraRow, ConditionOccurrenceRow
from omop_etl.omop.models.tables import OmopTables


class ConditionEraBuilder:
    """
    Derives condition_era rows from already-built condition_occurrence rows:
    one era per (person_id, condition_concept_id), collapsing occurrences separated by
    up to a 30-day gap (OHDSI's standard persistence window). This is cross-patient
    and runs after the main buidler loop (since this is just derived from other rows).

    Rows with condition_concept_id == 0 (the "unmapped" sentinel) are excluded,
    if not unmapped conditions are mered into shared eras.
    A null condition_end_date falls back to condition_start_date + 1 day, matching
    the OHDSI convention.
    """

    PERSISTENCE_DAYS = 30

    def build(self, condition_occurrence: list[ConditionOccurrenceRow]) -> list[ConditionEraRow]:
        mapped = [row for row in condition_occurrence if row.condition_concept_id != 0]
        if not mapped:
            return []

        df = pl.DataFrame(
            {
                "person_id": [row.person_id for row in mapped],
                "condition_concept_id": [row.condition_concept_id for row in mapped],
                "start": [row.condition_start_date for row in mapped],
                "end": [row.condition_end_date or row.condition_start_date + dt.timedelta(days=1) for row in mapped],
            }
        )

        eras = collapse_intervals(
            df,
            group_by=["person_id", "condition_concept_id"],
            start_col="start",
            end_col="end",
            persistence_days=self.PERSISTENCE_DAYS,
        )

        return [
            ConditionEraRow(
                condition_era_id=row_id(
                    OmopTables.CONDITION_ERA,
                    era["person_id"],
                    era["condition_concept_id"],
                    era["interval_start"],
                ),
                person_id=era["person_id"],
                condition_concept_id=era["condition_concept_id"],
                condition_era_start_date=era["interval_start"],
                condition_era_end_date=era["interval_end"],
                condition_occurrence_count=era["occurrence_count"],
            )
            for era in eras.iter_rows(named=True)
        ]
