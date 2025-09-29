import logging
from pathlib import Path
from typing import Optional
import json
import polars as pl
from logging import getLogger
from logging.handlers import RotatingFileHandler
import os

from .models import OutputFormat, RunContext, OutputPath
from ...infra.logging_setup import add_file_handler

log = getLogger(__name__)

# TODO: refactor to use dp injection, handle context from main/cli
#   - i.e. just write to a path


class OutputManager:
    """Manages all output operations for the preprocessing pipeline."""

    DEFAULT_BASE_DIR = Path(".data/preprocessing")
    SUPPORTED_FORMATS = {"csv", "tsv", "parquet"}
    FORMAT_EXTENSIONS = {"csv": ".csv", "tsv": ".tsv", "parquet": ".parquet"}

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        default_format: OutputFormat = "csv",
        enable_logfile: bool = True,
    ):
        self.file_handler: Optional[RotatingFileHandler] = None
        self.base_dir = base_dir or self.DEFAULT_BASE_DIR
        self.default_format = default_format
        self.enable_logfile = enable_logfile

    def resolve_output_path(
        self,
        ctx: RunContext,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        filename_stem: str = "preprocessed",
    ) -> OutputPath:
        """
        Resolve the output path based on provided arguments.

        Args:
            ctx: Run context with trial, timestamp, and run_id
            output: Optional explicit output path (file or dir)
            fmt: Optional format override
            filename_stem: Base name for generated files

        Returns:
            OutputPath with resolved paths for data and manifest
        """
        # explicit file path provided
        if output and output.suffix:
            fmt = self._infer_format(output)
            if not fmt:
                # use provided format or default if extension unknown
                fmt = self._normalize_format(fmt)
                output = output.with_suffix(self.FORMAT_EXTENSIONS[fmt])

            directory = output.parent
            directory.mkdir(parents=True, exist_ok=True)

            stem = f"data_{output.stem}"
            manifest_file = directory / f"manifest_{stem}.json"
            log_file = directory / f"{stem}.log"

            return OutputPath(
                data_file=output,
                manifest_file=manifest_file,
                log_file=log_file,
                directory=directory,
                format=fmt,
            )

        # dir provided or use default
        if output and output.is_dir():
            base = output
        else:
            base = self.base_dir

        # create structured dir
        directory = base / ctx.trial.lower() / f"{ctx.timestamp}_{ctx.run_id}"
        directory.mkdir(parents=True, exist_ok=True)

        # determine format and build paths
        fmt = self._normalize_format(fmt)
        data_file = (directory / f"data_{filename_stem}").with_suffix(self.FORMAT_EXTENSIONS[fmt])
        manifest_file = directory / f"manifest_{filename_stem}.json"
        log_file = directory / f"{filename_stem}.log"

        return OutputPath(
            data_file=data_file,
            manifest_file=manifest_file,
            log_file=log_file,
            directory=directory,
            format=fmt,
        )

    @staticmethod
    def write_dataframe(df: pl.DataFrame, output_path: OutputPath) -> None:
        log.debug(f"Writing {df.height} rows to {output_path.data_file}")

        if output_path.format == "csv":
            df.write_csv(
                output_path.data_file,
                include_header=True,
                null_value=None,
                float_precision=6,
            )
        elif output_path.format == "tsv":
            df.write_csv(
                output_path.data_file,
                include_header=True,
                null_value=None,
                float_precision=6,
                separator="\t",
            )
        elif output_path.format == "parquet":
            df.write_parquet(output_path.data_file, compression="zstd", statistics=True)
        else:
            raise ValueError(f"Unsupported format: {output_path.format}")

    @staticmethod
    def write_manifest(
        output_path: OutputPath,
        ctx: RunContext,
        df: pl.DataFrame,
        input_path: Path,
        log_file_created: bool = True,
        options: Optional[dict] = None,
    ) -> None:
        manifest = {
            "trial": ctx.trial,
            "timestamp": ctx.timestamp,
            "run_id": ctx.run_id,
            "input": str(input_path.absolute()),
            "output": str(output_path.data_file.absolute()),
            "format": output_path.format,
            "log_file": str(output_path.log_file.absolute()) if log_file_created else None,
            "statistics": {
                "rows": df.height,
                "columns": df.width,
            },
            "schema": {col: str(dtype) for col, dtype in df.schema.items()},
            "options": options or {},
        }

        log.debug(f"Writing manifest to {output_path.manifest_file}")
        output_path.manifest_file.write_text(json.dumps(manifest, indent=2))

    def write(
        self,
        df: pl.DataFrame,
        ctx: RunContext,
        input_path: Path,
        output: Optional[Path] = None,
        fmt: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> OutputPath:
        """
        Complete write operation: dataframe + manifest + setup logging.

        Args:
            df: DataFrame to write
            ctx: Run context
            input_path: Original input path
            output: Optional output path override
            fmt: Optional format override
            options: Optional processing options to record

        Returns:
            OutputPath with paths to written files
        """
        output_path = self.resolve_output_path(ctx, output, fmt)
        disable_log_file = os.getenv("DISABLE_LOG_FILE", "0") == "1"

        # set up log file if enabled
        if self.enable_logfile and not disable_log_file:
            log_file_json = os.getenv("LOG_FILE_JSON")
            if log_file_json is not None:
                json_format = log_file_json == "1"
            else:
                # fallback to console format setting
                json_format = os.getenv("LOG_JSON", "0") == "1"

            # add file handler for this run
            self.file_handler = add_file_handler(output_path.log_file, json_format=json_format, level=logging.DEBUG)

            # log start of output writing with context
            log.info(
                "Starting output write",
                extra={
                    **ctx.as_dict(),
                    "input": str(input_path),
                    "output_dir": str(output_path.directory),
                    "log_file": str(output_path.log_file),
                    "log_format": "json" if json_format else "plain",
                },
            )
        else:
            log.info(
                "Starting output write (no log file)",
                extra={
                    **ctx.as_dict(),
                    "input": str(input_path),
                    "output_dir": str(output_path.directory),
                },
            )

        try:
            self.write_dataframe(df, output_path)

            log_file_created = self.enable_logfile and not disable_log_file
            self.write_manifest(
                output_path=output_path,
                ctx=ctx,
                df=df,
                input_path=input_path,
                options=options,
                log_file_created=log_file_created,
            )

            log.info(
                "Output written successfully",
                extra={
                    **ctx.as_dict(),
                    "output": str(output_path.data_file),
                    "rows": df.height,
                    "columns": df.width,
                },
            )

        except Exception as e:
            log.error(f"Failed to write output: {e}", extra=ctx.as_dict(), exc_info=True)
            raise

        return output_path

    def _infer_format(self, path: Path) -> Optional[OutputFormat]:
        """Infer format from file extension."""
        suffix = path.suffix.lower().lstrip(".")
        if suffix == "txt":
            suffix = "tsv"
        return suffix if suffix in self.SUPPORTED_FORMATS else None

    def _normalize_format(self, fmt: Optional[str]) -> OutputFormat:
        """Normalize format string to supported type."""
        if not fmt:
            return self.default_format

        normalized = fmt.lower()
        if normalized == "txt":
            normalized = "tsv"

        if normalized not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format '{fmt}'. Supported: {', '.join(sorted(self.SUPPORTED_FORMATS))}")

        return normalized  # type: ignore[return-value]
