# core/combine.py
import polars as pl

from .models import EcrfConfig


def combine(ecfg: EcrfConfig, on: str = "SubjectId") -> pl.DataFrame:
    if not ecfg.data:
        raise ValueError("No eCRF config data loaded")

    frames: list[pl.DataFrame] = []
    for sheet_data in ecfg.data:
        df = sheet_data.data
        if on not in df.columns:
            raise ValueError(f"'{on}' not in sheet {sheet_data.key}")

        # normalize key dtype across sheets to avoid concat type errors
        df = df.with_columns(pl.col(on).cast(pl.Utf8))

        # prefix everything except the key
        mapping = {c: f"{sheet_data.key}_{c}" for c in df.columns if c != on}
        frames.append(df.rename(mapping))

    # union-of-columns vertical concat, fill missing with nulls
    out = pl.concat(frames, how="diagonal", rechunk=True).sort(on, nulls_last=True)
    return out
