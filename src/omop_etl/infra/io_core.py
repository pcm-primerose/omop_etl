from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import json
import polars as pl

SUPPORTED_FORMATS = {"csv", "tsv", "parquet", "json", "ndjson"}
FORMAT_EXT = {"csv": ".csv", "tsv": ".tsv", "parquet": ".parquet", "json": ".json", "ndjson": ".ndjson"}


@dataclass(frozen=True)
class OutputPath:
    data_file: Path
    manifest_file: Path
    log_file: Path
    directory: Path
    format: str


def _normalize_format(fmt: Optional[str], default: str = "csv") -> str:
    if not fmt:
        return default
    f = fmt.lower()
    if f == "txt":
        f = "tsv"
    if f not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Supported: {', '.join(sorted(SUPPORTED_FORMATS))}")
    return f


def _infer_format(path: Path) -> Optional[str]:
    suf = path.suffix.lower().lstrip(".")
    if suf == "txt":
        suf = "tsv"
    return suf if suf in SUPPORTED_FORMATS else None


def resolve_output_path(
    trial: str,
    timestamp: str,
    run_id: str,
    base_dir: Path,
    output: Optional[Path] = None,
    fmt: Optional[str] = None,
    filename_stem: str = "data",
) -> OutputPath:
    if output and output.suffix:
        f = _infer_format(output) or _normalize_format(fmt)
        out = output.with_suffix(FORMAT_EXT[f])
        out.parent.mkdir(parents=True, exist_ok=True)
        manifest = out.parent / f"manifest_{out.stem}.json"
        logf = out.parent / f"{out.stem}.log"
        return OutputPath(out, manifest, logf, out.parent, f)

    base = output if (output and output.is_dir()) else base_dir
    outdir = base / trial.lower() / f"{timestamp}_{run_id}"
    outdir.mkdir(parents=True, exist_ok=True)
    f = _normalize_format(fmt)
    data = (outdir / f"data_{filename_stem}").with_suffix(FORMAT_EXT[f])
    manifest = outdir / f"manifest_{filename_stem}.json"
    logf = outdir / f"{filename_stem}.log"
    return OutputPath(data, manifest, logf, outdir, f)


def write_single_frame(df: pl.DataFrame, path: Path, fmt: str) -> None:
    if fmt == "csv":
        print(f"{df.schema}")
        print(f"head: {df.head(100)}")
        df.write_csv(path, include_header=True, null_value=None, float_precision=6)
    elif fmt == "tsv":
        df.write_csv(path, include_header=True, null_value=None, float_precision=6, separator="\t")
    elif fmt == "parquet":
        df.write_parquet(path, compression="zstd", statistics=True)
    else:
        raise ValueError(f"Unsupported frame format for single-file export: {fmt}")


def write_frames_dir(frames: Dict[str, pl.DataFrame], outdir: Path, fmt: str) -> Dict[str, Path]:
    files: Dict[str, Path] = {}
    outdir.mkdir(parents=True, exist_ok=True)
    for name, df in frames.items():
        if df.height == 0:
            continue
        path = outdir / f"{name}{FORMAT_EXT[fmt]}"
        write_single_frame(df, path, fmt)
        files[name] = path
    return files


def write_manifest(
    path: Path,
    trial: str,
    timestamp: str,
    run_id: str,
    input_path: Path,
    output_file: Path,
    directory: Path,
    fmt: str,
    mode: str,
    tables: Dict[str, pl.DataFrame],
    table_files: Optional[Dict[str, Path]] = None,
    options: Optional[dict] = None,
) -> None:
    manifest = {
        "trial": trial,
        "timestamp": timestamp,
        "run_id": run_id,
        "input": str(input_path.absolute()),
        "output": str(output_file.absolute()),
        "directory": str(directory.absolute()),
        "format": fmt,
        "mode": mode,
        "tables": {
            name: {
                "rows": df.height,
                "cols": df.width,
                "schema": {c: str(t) for c, t in df.schema.items()},
                "file": str(table_files[name].absolute()) if table_files and name in table_files else None,
            }
            for name, df in tables.items()
        },
        "options": options or {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
