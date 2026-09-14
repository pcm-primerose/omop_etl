import polars as pl


def collapse_intervals(
    df: pl.DataFrame,
    *,
    group_by: list[str],
    start_col: str,
    end_col: str,
    persistence_days: int,
    count_col: str | None = None,
    extra_agg: list[pl.Expr] | None = None,
) -> pl.DataFrame:
    """
    Collapse (possibly overlapping) [start, end] intervals per `group_by` key into
    contiguous eras, merging occurrences separated by a gap of up to
    `persistence_days` days, a gap of exactly `persistence_days` still merges,
    `persistence_days + 1` does not (verified against the OHDSI reference SQL's
    same-day start-before-end tie-break).

    OMOP-agnostic on purpose: knows nothing about concept_id, era tables, or any
    fallback-date logic, that's callers' responsibility (e.g. filtering out missing end dates),
    so `start_col` and `end_col` must both be non-null `pl.Date` (or `pl.Datetime`).

    `count_col`: pass the name of an existing per-row occurrence count to SUM it
    into the collapsed era's count (for a second collapse pass over
    already-collapsed output, e.g. drug_era's overlap-merge-then-persistence-merge
    two-phase collapse). Omit to just COUNT(*) the input rows per era.

    `extra_agg`: additional `.agg()` expressions in the collapse
    (e.g. summing a per-row "days_exposed" column from a prior pass, for
    drug_era's gap_days). Purely additive, it doesn't affect `count_col`'s behavior.

    Returns one row per era: `group_by` columns + `interval_start`, `interval_end`,
    `occurrence_count`, plus whatever `extra_agg` adds.
    """
    sorted_df = df.sort(group_by + [start_col, end_col])

    running_end = pl.col(end_col).cum_max().over(group_by)
    previous_running_end = running_end.shift(1).over(group_by)

    starts_new_era = previous_running_end.is_null() | (pl.col(start_col) > previous_running_end + pl.duration(days=persistence_days))
    era_id = starts_new_era.cum_sum().over(group_by)

    occurrence_count = pl.col(count_col).sum() if count_col else pl.len()

    return (
        sorted_df.with_columns(_era_id=era_id)
        .group_by(group_by + ["_era_id"])
        .agg(
            pl.col(start_col).min().alias("interval_start"),
            pl.col(end_col).max().alias("interval_end"),
            occurrence_count.alias("occurrence_count"),
            *(extra_agg or []),
        )
        .drop("_era_id")
        .sort(group_by + ["interval_start"])
    )
