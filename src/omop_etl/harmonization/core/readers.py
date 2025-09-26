from pathlib import Path
import polars as pl

# For now just assume the correct format is met, can extend preoprocessing module later
# or extend this reader, depending on how the complexity ...
# API will handle state, need to implement other trials first


def load_csv(path: str | Path) -> pl.DataFrame:
    return pl.read_csv(path)


def load_excel(path: str | Path) -> pl.DataFrame:
    return pl.read_excel(path)
