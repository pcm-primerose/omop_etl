import json
from pathlib import Path
from typing import Dict, Optional

import polars as pl

from ..datamodels import HarmonizedData
from .serialize import to_primitive

# TODO: extract to separate module after main
OutputFormat = str  # "csv" | "tsv" | "parquet" | "xlsx" | "json" | "ndjson"


class RunContext:
    def __init__(self, trial: str, timestamp: str, run_id: str):
        self.trial = trial
        self.timestamp = timestamp
        self.run_id = run_id

    def as_dict(self) -> dict:
        return {"trial": self.trial, "timestamp": self.timestamp, "run_id": self.run_id}


class OutputPath:  # minimal stand-in
    def __init__(self, data_file: Path, manifest_file: Path, log_file: Path, directory: Path, format: str):
        self.data_file = data_file
        self.manifest_file = manifest_file
        self.log_file = log_file
        self.directory = directory
        self.format = format


class HarmonizedOutputManager:
    """Manage export of HarmonizedData (wide file, normalized dir, or Excel workbook)."""

    DEFAULT_BASE_DIR = Path(".data/harmonized")
    SUPPORTED_FILE_FORMATS = {"csv", "tsv", "parquet", "xlsx", "json", "ndjson"}
    FORMAT_EXT = {"csv": ".csv", "tsv": ".tsv", "parquet": ".parquet", "json": ".json", "ndjson": ".ndjson"}

    def __init__(self, base_dir: Optional[Path] = None, default_format: OutputFormat = "csv"):
        self.base_dir = base_dir or self.DEFAULT_BASE_DIR
        self.default_format = default_format

    def resolve_output_path(
        self,
        ctx: RunContext,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        filename_stem: str = "harmonized",
    ) -> OutputPath:
        # explicit file path -> infer/normalize format, ensure dir exists
        if output and output.suffix:
            fmt_norm = self._infer_format(output) or self._normalize_format(fmt)
            output = output.with_suffix(self.FORMAT_EXT[fmt_norm])
            directory = output.parent
            directory.mkdir(parents=True, exist_ok=True)

            manifest = directory / f"manifest_{output.stem}.json"
            logf = directory / f"{output.stem}.log"

            return OutputPath(
                data_file=output,
                manifest_file=manifest,
                log_file=logf,
                directory=directory,
                format=fmt_norm,
            )

        # dir provided or default
        if output and output.is_dir():
            base = output
        else:
            base = self.base_dir

        directory = base / ctx.trial.lower() / f"{ctx.timestamp}_{ctx.run_id}"
        directory.mkdir(parents=True, exist_ok=True)

        fmt_norm = self._normalize_format(fmt)
        data_file = (directory / f"data_{filename_stem}").with_suffix(self.FORMAT_EXT[fmt_norm])
        manifest = directory / f"manifest_{filename_stem}.json"
        logf = directory / f"{filename_stem}.log"

        return OutputPath(
            data_file=data_file,
            manifest_file=manifest,
            log_file=logf,
            directory=directory,
            format=fmt_norm,
        )

    def write_wide(
        self,
        hd: HarmonizedData,
        ctx: RunContext,
        input_path: Path,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> OutputPath:
        """Single wide file (csv/tsv/parquet/xlsx/json/ndjson)."""
        op = self.resolve_output_path(ctx, output, fmt, filename_stem="harmonized_wide")
        df = hd.to_dataframe_wide_single_csv(
            include_singletons=True,
            include_collections=True,
            prefix_sep="__",
            add_collection_counts=False,
        )

        self._write_single(df, hd, op)
        self._write_manifest(op, ctx, input_path, mode="wide", tables={"wide": df}, options=options)
        return op

    def write_normalized_dir(
        self,
        hd: HarmonizedData,
        ctx: RunContext,
        input_path: Path,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> OutputPath:
        """Directory of normalized tables (csv/parquet)."""
        op = self.resolve_output_path(ctx, output, fmt, filename_stem="harmonized_norm")
        frames = hd.to_frames_normalized()

        main_table = "patients" if "patients" in frames else (next(iter(frames)) if frames else "patients")
        files = self._write_dir(frames, op.directory, op.format, main_table=main_table)
        if main_table in files:
            op.data_file = files[main_table]

        self._write_manifest(op, ctx, input_path, mode="normalized", tables=frames, table_files=files, options=options)
        return op

    @staticmethod
    def _ensure_parent(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)

    def _write_single(self, df: pl.DataFrame, hd: HarmonizedData, op: OutputPath) -> None:
        self._ensure_parent(op.data_file)
        f = op.format
        if f == "csv":
            df.write_csv(op.data_file, include_header=True, null_value=None, float_precision=6)
        elif f == "tsv":
            df.write_csv(op.data_file, include_header=True, null_value=None, float_precision=6, separator="\t")
        elif f == "parquet":
            df.write_parquet(op.data_file, compression="zstd", statistics=True)
        elif f == "xlsx":
            # single sheet named “data”
            df.write_excel(
                workbook=str(op.data_file),
                worksheet="data",
                dtype_formats={pl.Date: "yyyy-mm-dd", pl.Datetime: "yyyy-mm-dd hh:mm:ss"},
                table_style="Table Style Light 16",
                autofit=True,
            )
        elif f == "json":
            op.data_file.write_text(hd.to_json(indent=2), encoding="utf-8")
        elif f == "ndjson":
            payload = hd.to_dict().get("patients", [])
            with op.data_file.open("w", encoding="utf-8") as fp:
                for obj in payload:
                    fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
        else:
            raise ValueError(f"Unsupported format for wide export: {f}")

    def _write_dir(self, frames: Dict[str, pl.DataFrame], outdir: Path, fmt: str, *, main_table: str) -> Dict[str, Path]:
        outdir.mkdir(parents=True, exist_ok=True)
        files: Dict[str, Path] = {}
        for name, df in frames.items():
            if df.height == 0:
                continue
            path = outdir / f"{name}{self.FORMAT_EXT[fmt]}"
            if fmt == "csv":
                df.write_csv(path, include_header=True, null_value=None, float_precision=6)
            elif fmt == "tsv":
                df.write_csv(path, include_header=True, null_value=None, float_precision=6, separator="\t")
            elif fmt == "parquet":
                df.write_parquet(path, compression="zstd", statistics=True)
            else:
                raise ValueError(f"Normalized dir only supports csv/tsv/parquet; got {fmt}")
            files[name] = path
        return files

    @staticmethod
    def _safe_sheet_name(name: str) -> str:
        return (name or "Sheet")[:31]

    def _write_manifest(
        self,
        op: OutputPath,
        ctx: RunContext,
        input_path: Path,
        *,
        mode: str,
        tables: Dict[str, pl.DataFrame],
        table_files: Optional[Dict[str, Path]] = None,
        options: Optional[dict] = None,
    ) -> None:
        if options is None:
            options = {}
        # stringify options (ensure JSON-safe)
        options = json.loads(json.dumps(options, default=to_primitive))

        manifest = {
            "trial": ctx.trial,
            "timestamp": ctx.timestamp,
            "run_id": ctx.run_id,
            "input": str(input_path.absolute()),
            "output": str(op.data_file.absolute()),
            "directory": str(op.directory.absolute()),
            "format": op.format,
            "mode": mode,  # "wide" | "normalized" | "workbook"
            "log_file": str(op.log_file.absolute()) if op.log_file else None,
            "tables": {
                name: {
                    "rows": df.height,
                    "cols": df.width,
                    "schema": {c: str(t) for c, t in df.schema.items()},
                    "file": str(table_files[name].absolute()) if table_files and name in table_files else None,
                }
                for name, df in tables.items()
            },
            "options": options,
        }
        op.manifest_file.parent.mkdir(parents=True, exist_ok=True)
        op.manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
