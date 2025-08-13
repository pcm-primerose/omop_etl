from __future__ import annotations
import uuid
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Protocol, Sequence
import polars as pl

from .models import EcrfConfig, SheetData, OutputSpec


# future todo: need to extend IO for new trials


class DataSourceReader(Protocol):
    def can_read(self, path: Path) -> bool:
        ...

    def load(self, path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        ...


class ExcelReader:
    @staticmethod
    def can_read(path: Path) -> bool:
        return (
            path.exists()
            and not path.is_dir()
            and path.suffix.lower() in {".xls", ".xlsx", ".xlsb"}
        )

    @staticmethod
    def load(path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        # load all sheets once
        sheets = pl.read_excel(
            path, sheet_id=0, has_header=True, read_options={"header_row": 1}
        )
        for source_config in ecfg.configs:
            if source_config.key in sheets:
                df = sheets[source_config.key]
                df = _ensure_schema(df, source_config.usecols)
                df = _csv_like_numeric_cast(df)
                ecfg.data = (ecfg.data or []) + [SheetData(source_config.key, df, path)]
        return ecfg


class CsvDirReader:
    @staticmethod
    def can_read(path: Path) -> bool:
        return path.is_dir()

    @staticmethod
    def _match_files(dirpath: Path, key: str) -> list[Path]:
        out = [
            file
            for file in dirpath.iterdir()
            if file.is_file()
            and file.suffix.lower() == ".csv"
            and key.lower() == file.stem.lower().split("_")[-1]
        ]
        return out

    @classmethod
    def load(cls, dir_path: Path, ecfg: EcrfConfig) -> EcrfConfig:
        index = _index_dir(dir_path)
        for source_config in ecfg.configs:
            key = source_config.key.lower()
            if key not in index:
                raise FileNotFoundError(
                    f"No CSV for key '{source_config.key}' in {dir_path}"
                )
            file_path = index[key]
            df = pl.read_csv(file_path, skip_rows=1)
            df = _ensure_schema(df, source_config.usecols)
            ecfg.data = (ecfg.data or []) + [
                SheetData(source_config.key, df, file_path)
            ]

        return ecfg


def _index_dir(dir_path: Path) -> dict[str, Path]:
    idx = {}
    for p in dir_path.iterdir():
        if p.is_file() and p.suffix.lower() == ".csv":
            idx[p.stem.lower().split("_")[-1]] = p
    return idx


_INT_RE = r"^[+-]?\d+$"


def _csv_like_numeric_cast(df: pl.DataFrame) -> pl.DataFrame:
    """
    Mimic CSV inference for ints:
    if a Utf8 column is all digits (ignoring nulls/space), cast to Int64 (e.g. '01' -> 1).
    This allows byte-identical output files for csv and xlsx.
    """
    # int strings ("324", "+4", "0", "-2")
    int_cols: list[str] = []
    for col, data_type in df.schema.items():
        if data_type != pl.Utf8:
            continue
        # are non-null values intish strings?
        # e.g. "123", "+2", "-2", "0"
        ok = df.select(
            (
                pl.col(col).is_null()
                | pl.col(col).cast(pl.Utf8).str.strip_chars().str.contains(_INT_RE)
            ).all()
        ).item()
        if ok:
            int_cols.append(col)

    if not int_cols:
        return df

    return df.with_columns([pl.col(int_cols).cast(pl.Int64)])


def resolve_input(path: Path, ecfg: EcrfConfig) -> EcrfConfig:
    for reader in (ExcelReader(), CsvDirReader()):
        if reader.can_read(path):
            return reader.load(path, ecfg)
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


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _rid() -> str:
    return uuid.uuid4().hex[:8]


def _materialize(spec: OutputSpec, trial: str) -> Path:
    rel = spec.subdir.format(trial=trial.lower(), ts=_now(), run_id=_rid())
    (spec.base_dir / rel).mkdir(parents=True, exist_ok=True)
    ext = {"csv": ".csv", "parquet": ".parquet", "tsv": ".tsv"}[spec.fmt]
    return (spec.base_dir / rel / spec.filename).with_suffix(ext)


def write_output(df: pl.DataFrame, trial: str, spec: OutputSpec) -> Path:
    path = _materialize(spec, trial)
    if spec.fmt == "csv":
        df.write_csv(path, include_header=True, null_value="", float_precision=6)
    elif spec.fmt == "parquet":
        df.write_parquet(path, compression="zstd", statistics=True)
    elif spec.fmt == "tsv":
        df.write_csv(
            path, include_header=True, null_value="", float_precision=6, separator="\t"
        )
    else:
        raise ValueError(
            f"Output format '{spec.fmt}' not supported. Supported: csv, tsv, parquet."
        )
    return path


def write_manifest(
    path: Path, trial: str, input_path: Path, df: pl.DataFrame, opts
) -> None:
    manifest = {
        "trial": trial,
        "input": str(input_path),
        "output": str(path),
        "created": _now(),
        "rows": df.height,
        "cols": df.width,
        "schema": {k: str(v) for k, v in df.schema.items()},
        "options": vars(opts) if opts else {},
    }
    path.with_suffix(path.suffix + ".json").write_text(json.dumps(manifest, indent=2))
