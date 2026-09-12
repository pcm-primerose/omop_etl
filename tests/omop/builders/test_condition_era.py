import datetime as dt

from omop_etl.omop.builders.condition_era import ConditionEraBuilder
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import ConditionOccurrenceRow
from omop_etl.omop.models.tables import OmopTables

D = dt.date


def _condition(
    person_id: int,
    concept_id: int,
    start: dt.date,
    end: dt.date | None = None,
    *,
    occurrence_id: int = 0,
) -> ConditionOccurrenceRow:
    return ConditionOccurrenceRow(
        condition_occurrence_id=occurrence_id,
        person_id=person_id,
        condition_concept_id=concept_id,
        condition_start_date=start,
        condition_end_date=end,
        condition_type_concept_id=0,
    )


class TestConditionEraBuilder:
    def test_collapses_occurrences_into_one_era(self):
        rows = [
            _condition(1, 100, D(2023, 1, 1), D(2023, 1, 1)),
            _condition(1, 100, D(2023, 1, 15), D(2023, 1, 15)),
        ]

        eras = ConditionEraBuilder().build(rows)

        assert len(eras) == 1
        era = eras[0]
        assert era.person_id == 1
        assert era.condition_concept_id == 100
        assert era.condition_era_start_date == D(2023, 1, 1)
        assert era.condition_era_end_date == D(2023, 1, 15)
        assert era.condition_occurrence_count == 2

    def test_gap_beyond_persistence_window_yields_separate_eras(self):
        rows = [
            _condition(1, 100, D(2023, 1, 1), D(2023, 1, 1)),
            _condition(1, 100, D(2023, 3, 1), D(2023, 3, 1)),
        ]

        eras = ConditionEraBuilder().build(rows)

        assert len(eras) == 2

    def test_unmapped_concept_id_zero_is_excluded(self):
        rows = [
            _condition(1, 0, D(2023, 1, 1), D(2023, 1, 1)),
            _condition(1, 0, D(2023, 1, 5), D(2023, 1, 5)),
        ]

        eras = ConditionEraBuilder().build(rows)

        assert eras == []

    def test_null_end_date_falls_back_to_start_plus_one_day(self):
        rows = [_condition(1, 100, D(2023, 1, 1), end=None)]

        era = ConditionEraBuilder().build(rows)[0]

        assert era.condition_era_start_date == D(2023, 1, 1)
        assert era.condition_era_end_date == D(2023, 1, 2)
        assert era.condition_occurrence_count == 1

    def test_different_concepts_and_people_produce_separate_eras(self):
        rows = [
            _condition(1, 100, D(2023, 1, 1)),
            _condition(1, 200, D(2023, 1, 1)),
            _condition(2, 100, D(2023, 1, 1)),
        ]

        eras = ConditionEraBuilder().build(rows)

        assert len(eras) == 3

    def test_era_id_is_deterministic_and_matches_row_id_convention(self):
        rows = [_condition(1, 100, D(2023, 1, 1), D(2023, 1, 1))]

        era = ConditionEraBuilder().build(rows)[0]

        assert era.condition_era_id == row_id(OmopTables.CONDITION_ERA, 1, 100, D(2023, 1, 1))

    def test_no_condition_occurrence_rows_yields_no_eras(self):
        assert ConditionEraBuilder().build([]) == []
