from __future__ import annotations
from pathlib import Path
from typing import Protocol, Sequence
import polars as pl
from .types import EcrfConfig, SheetData

# TODO: need to extend IO for new trials


class DataSourceReader(Protocol):
    def can_read(self, path: Path) -> bool:
        ...

    def load(self, path: Path, cfg: EcrfConfig) -> EcrfConfig:
        ...


class ExcelReader:
    @staticmethod
    def can_read(path: Path) -> bool:
        return path.is_file() and path.suffix.lower() in {".xls", ".xlsx"}

    @staticmethod
    def load(path: Path, cfg: EcrfConfig) -> EcrfConfig:
        # load all sheets once
        sheets = pl.read_excel(
            path, sheet_id=0, has_header=True, read_options={"header_row": 1}
        )
        for source_config in cfg.configs:
            if source_config.key in sheets:
                df = sheets[source_config.key]
                df = _ensure_schema(df, source_config.usecols)
                cfg.data = (cfg.data or []) + [SheetData(source_config.key, df, path)]
        return cfg


class CsvDirReader:
    @staticmethod
    def can_read(path: Path) -> bool:
        return path.is_dir()

    @staticmethod
    def _match_files(dirpath: Path, key: str) -> list[Path]:
        print(f"key: {key}")
        out = [
            file
            for file in dirpath.iterdir()
            if file.is_file()
            and file.suffix.lower() == ".csv"
            and key.lower() == file.stem.lower().split("_")[-1]
        ]
        return out

    @classmethod
    def load(cls, dir_path: Path, cfg: EcrfConfig) -> EcrfConfig:
        index = _index_dir(dir_path)
        for source_config in cfg.configs:
            key = source_config.key.lower()
            if key not in index:
                raise FileNotFoundError(
                    f"No CSV for key '{source_config.key}' in {dir_path}"
                )
            file_path = index[key]
            df = pl.read_csv(file_path, skip_rows=1)
            df = _ensure_schema(df, source_config.usecols)
            cfg.data = (cfg.data or []) + [SheetData(source_config.key, df, file_path)]

        return cfg


def _index_dir(dir_path: Path) -> dict[str, Path]:
    idx = {}
    for p in dir_path.iterdir():
        if p.is_file() and p.suffix.lower() == ".csv":
            idx[p.stem.lower().split("_")[-1]] = p
    return idx


def resolve_input(path: Path, cfg: EcrfConfig) -> EcrfConfig:
    for reader in (ExcelReader(), CsvDirReader()):
        if reader.can_read(path):
            return reader.load(path, cfg)
    raise ValueError(f"Unsupported input type: {path}")


def _ensure_schema(df: pl.DataFrame, expected: Sequence[str]) -> pl.DataFrame:
    upper = {c.upper(): c for c in df.columns}
    present = [c for c in expected if c.upper() in upper]
    missing = [c for c in expected if c.upper() not in upper]

    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    out = df.select([upper[c.upper()] for c in present]).rename(
        {upper[c.upper()]: c for c in present}
    )
    return out.select(present)
