import polars as pl
from ..core.models import EcrfConfig, PreprocessingRunOptions


def preprocess_impress(df: pl.DataFrame, ecfg: EcrfConfig, run_opts: PreprocessingRunOptions) -> pl.DataFrame:
    """
    Pre-processing pipeline for IMPRESS.
    """
    trial = (ecfg.trial or "impress").upper()
    base = _filter_valid_cohort(df) if run_opts.filter_valid_cohort else df
    return base.pipe(_add_trial, trial).pipe(_prefix_subject, trial).pipe(_aggregate_no_conflicts).pipe(_reorder_subject_trial_first)


def _filter_valid_cohort(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep every row for subjects that have at least one valid cohort row.
    """
    name = pl.col("COH_COHORTNAME")
    valid = name.is_not_null() & (name.str.strip_chars().str.len_chars() > 0) & (name.str.to_uppercase() != "NA")
    any_valid = df.select(pl.col("SubjectId"), valid.alias("v")).group_by("SubjectId").agg(pl.any("v").alias("ok"))
    return df.join(any_valid.filter(pl.col("ok")), on="SubjectId", how="semi")


def _add_trial(df: pl.DataFrame, name: str) -> pl.DataFrame:
    return df.with_columns(pl.lit(name.upper()).alias("Trial"))


def _prefix_subject(df: pl.DataFrame, trial: str) -> pl.DataFrame:
    return df.with_columns((pl.lit(trial.upper() + "-") + pl.col("SubjectId")).alias("SubjectId"))


def _aggregate_no_conflicts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Mark cols in rows woth conflics, aggregate on SubjectID if possible,
    for conflicts keep raw data as is (multi-row).
    """
    cols = [c for c in df.columns if c != "SubjectId"]

    # count non-null values per SubjectId per column
    nuniqs = df.group_by("SubjectId").agg([pl.col(c).drop_nulls().n_unique().alias(f"{c}__n") for c in cols])
    # keep raw rows for conflicted subjects
    conflicted = nuniqs.with_columns(pl.max_horizontal([pl.col(f"{c}__n") > 1 for c in cols]).alias("conflicted"))
    keep_raw = df.join(conflicted.filter(pl.col("conflicted")), on="SubjectId", how="semi")
    # collapse non-conflicted to one row
    agg = (
        df.join(conflicted.filter(~pl.col("conflicted")), on="SubjectId", how="semi")
        .group_by("SubjectId")
        .agg([pl.col(c).drop_nulls().first().alias(c) for c in cols])
    )
    return pl.concat([keep_raw, agg]).sort("SubjectId")


def _reorder_subject_trial_first(df: pl.DataFrame) -> pl.DataFrame:
    cols = [c for c in df.columns if c not in {"SubjectId", "Trial"}]
    return df.select(["SubjectId", "Trial", *cols])
