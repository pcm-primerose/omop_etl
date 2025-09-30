# harmonization/io_export.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import json
import polars as pl

from ..datamodels import HarmonizedData
from ...infra.run_context import RunContext
from ...infra.io_core import (
    OutputPath,
    resolve_output_path,
    write_single_frame,
    write_frames_dir,
    write_manifest,
)

# TODO: fix: to_primitive and FORMAT_EXT unused


class HarmonizedOutputManager:
    DEFAULT_BASE_DIR = Path(".data/harmonized")

    def __init__(self, base_dir: Optional[Path] = None, default_format: str = "csv"):
        self.base_dir = base_dir or self.DEFAULT_BASE_DIR
        self.default_format = default_format

    def write_wide(
        self,
        hd: HarmonizedData,
        ctx: RunContext,
        input_path: Path,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> OutputPath:
        op = resolve_output_path(
            trial=ctx.trial,
            timestamp=ctx.timestamp,
            run_id=ctx.run_id,
            base_dir=self.base_dir,
            output=output,
            fmt=fmt or self.default_format,
            filename_stem="harmonized_wide",
        )

        df = hd.to_dataframe_wide()
        # wide supports csv/tsv/parquet; json/ndjson from hd.to_dict()
        if op.format in {"csv", "tsv", "parquet"}:
            write_single_frame(df, op.data_file, op.format)
            tables = {"wide": df}
        elif op.format == "json":
            op.data_file.write_text(hd.to_json(indent=2), encoding="utf-8")
            tables = {"wide": pl.DataFrame({"rows": [len(hd.patients)]})}
        elif op.format == "ndjson":
            with op.data_file.open("w", encoding="utf-8") as fp:
                for obj in hd.to_dict().get("patients", []):
                    fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
            tables = {"wide": pl.DataFrame({"rows": [len(hd.patients)]})}
        else:
            raise ValueError(f"Unsupported wide format: {op.format}")

        write_manifest(
            op.manifest_file,
            trial=ctx.trial,
            timestamp=ctx.timestamp,
            run_id=ctx.run_id,
            input_path=input_path,
            output_file=op.data_file,
            directory=op.directory,
            fmt=op.format,
            mode="wide",
            tables=tables,
            options=options,
        )
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
        op = resolve_output_path(
            trial=ctx.trial,
            timestamp=ctx.timestamp,
            run_id=ctx.run_id,
            base_dir=self.base_dir,
            output=output,
            fmt=fmt or self.default_format,
            filename_stem="harmonized_norm",
        )

        if op.format not in {"csv", "tsv", "parquet"}:
            raise ValueError("Normalized export supports csv/tsv/parquet only.")

        frames = hd.to_frames_normalized()
        files = write_frames_dir(frames, op.directory, op.format)
        # set main data_file for convenience
        main_table = "patients" if "patients" in files else (next(iter(files.values())) if files else op.data_file)
        if isinstance(main_table, Path):
            op = OutputPath(data_file=main_table, manifest_file=op.manifest_file, log_file=op.log_file, directory=op.directory, format=op.format)

        write_manifest(
            op.manifest_file,
            trial=ctx.trial,
            timestamp=ctx.timestamp,
            run_id=ctx.run_id,
            input_path=input_path,
            output_file=op.data_file,
            directory=op.directory,
            fmt=op.format,
            mode="normalized",
            tables=frames,
            table_files=files,
            options=options,
        )
        return op
