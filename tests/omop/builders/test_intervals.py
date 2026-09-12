import datetime as dt
import polars as pl

from omop_etl.omop.builders.intervals import collapse_intervals


def _rows(*rows: tuple[str, int, dt.date, dt.date]) -> pl.DataFrame:
    person_ids, concept_ids, starts, ends = zip(*rows)
    return pl.DataFrame(
        {
            "person_id": list(person_ids),
            "concept_id": list(concept_ids),
            "start": list(starts),
            "end": list(ends),
        }
    )


def _collapse(df: pl.DataFrame, persistence_days: int = 30, count_col: str | None = None) -> pl.DataFrame:
    return collapse_intervals(
        df,
        group_by=["person_id", "concept_id"],
        start_col="start",
        end_col="end",
        persistence_days=persistence_days,
        count_col=count_col,
    ).sort("person_id", "concept_id", "interval_start")


D = dt.date


class TestCollapseIntervals:
    def test_isolated_occurrences_each_get_their_own_era(self):
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 1, D(2023, 6, 1), D(2023, 6, 1)),
        )

        result = _collapse(df)

        assert result["occurrence_count"].to_list() == [1, 1]
        assert result["interval_start"].to_list() == [D(2023, 1, 1), D(2023, 6, 1)]

    def test_gap_of_exactly_persistence_days_merges(self):
        # A ends Jan 1, B starts Jan 31: gap is exactly 30 days
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 1, D(2023, 1, 31), D(2023, 1, 31)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 1
        assert result["occurrence_count"].item() == 2
        assert result["interval_start"].item() == D(2023, 1, 1)
        assert result["interval_end"].item() == D(2023, 1, 31)

    def test_gap_of_persistence_days_plus_one_does_not_merge(self):
        # A ends Jan 1, B starts Feb 1: gap is 31 days
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 1, D(2023, 2, 1), D(2023, 2, 1)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 2
        assert result["occurrence_count"].to_list() == [1, 1]

    def test_overlapping_occurrences_merge_into_one_era(self):
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 15)),
            ("p1", 1, D(2023, 1, 10), D(2023, 1, 20)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 1
        assert result["interval_start"].item() == D(2023, 1, 1)
        assert result["interval_end"].item() == D(2023, 1, 20)
        assert result["occurrence_count"].item() == 2

    def test_short_nested_interval_does_not_truncate_the_running_end(self):
        # A: Jan1-Jan30 (long)
        # B: Jan5-Jan6 (short, nested inside A)
        # C starts day 50.
        # A naive "compare only to the previous row's end" would compare C to B's
        # short end (Jan6) and wrongly split, the running max (carried from A) must
        # be what C is compared against, merging all three into one era.
        base = D(2023, 1, 1)
        df = _rows(
            ("p1", 1, base, base + dt.timedelta(days=29)),
            ("p1", 1, base + dt.timedelta(days=4), base + dt.timedelta(days=5)),
            ("p1", 1, base + dt.timedelta(days=49), base + dt.timedelta(days=49)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 1
        assert result["occurrence_count"].item() == 3
        assert result["interval_end"].item() == base + dt.timedelta(days=49)

    def test_identical_start_and_end_dates_collapse_to_one_occurrence_each(self):
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 1
        assert result["occurrence_count"].item() == 2

    def test_different_concepts_for_the_same_person_are_separate_eras(self):
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 2, D(2023, 1, 1), D(2023, 1, 1)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 2
        assert set(result["concept_id"].to_list()) == {1, 2}

    def test_same_concept_for_different_people_are_separate_eras(self):
        df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p2", 1, D(2023, 1, 1), D(2023, 1, 1)),
        )

        result = _collapse(df, persistence_days=30)

        assert result.height == 2
        assert set(result["person_id"].to_list()) == {"p1", "p2"}

    def test_unsorted_input_gives_the_same_result_as_sorted_input(self):
        sorted_df = _rows(
            ("p1", 1, D(2023, 1, 1), D(2023, 1, 1)),
            ("p1", 1, D(2023, 1, 15), D(2023, 1, 15)),
        )
        unsorted_df = sorted_df.reverse()

        assert _collapse(sorted_df, persistence_days=30).equals(_collapse(unsorted_df, persistence_days=30))

    def test_count_col_sums_an_existing_occurrence_count_instead_of_counting_rows(self):
        # simulates drug_era's second collapse pass over already-collapsed sub-exposures
        df = pl.DataFrame(
            {
                "person_id": ["p1", "p1"],
                "concept_id": [1, 1],
                "start": [D(2023, 1, 1), D(2023, 1, 15)],
                "end": [D(2023, 1, 1), D(2023, 1, 15)],
                "prior_count": [3, 5],
            }
        )

        result = collapse_intervals(
            df,
            group_by=["person_id", "concept_id"],
            start_col="start",
            end_col="end",
            persistence_days=30,
            count_col="prior_count",
        )

        assert result.height == 1
        assert result["occurrence_count"].item() == 8
