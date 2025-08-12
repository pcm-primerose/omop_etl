import polars as pl
from .types import EcrfConfig


def combine(cfg: EcrfConfig, on: str = "SubjectId") -> pl.DataFrame:
    if not cfg.data:
        raise ValueError("No data loaded")

    frames: list[pl.DataFrame] = []
    for sd in cfg.data:
        df = sd.data
        if on not in df.columns:
            raise ValueError(f"'{on}' not in sheet {sd.key}")

        # normalize key dtype across sheets to avoid concat type errors
        df = df.with_columns(pl.col(on).cast(pl.Utf8))

        # prefix everything except the key
        mapping = {c: f"{sd.key}_{c}" for c in df.columns if c != on}
        frames.append(df.rename(mapping))

    # union-of-columns vertical concat, fill missing with nulls
    out = pl.concat(frames, how="diagonal", rechunk=True).sort(on, nulls_last=True)
    return out
