from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import typer
from logging import getLogger

from omop_etl.infra.logging_setup import configure
from omop_etl.preprocessing.api import (
    preprocess_trial,
    make_ecrf_config,
    list_trials,
    RunOptions,
    PreprocessResult,
)

app = typer.Typer(
    add_completion=True,
    pretty_exceptions_enable=False,
    help="Preprocess eCRF data using source-specific pipelines.",
)

log = getLogger(__name__)


@app.callback()
def _setup_logging(
    log_level: int = typer.Option(
        20,
        "--log-level",
        "-l",
        case_sensitive=False,
        help="Critical=50 | Error=40 | Warning=30 | Info=20 | Debug=10. Default=20.",
    ),
    log_json: bool = typer.Option(
        False,
        "--log-json/--no-log-json",
        help="Use JSON format for console output.",
    ),
    log_file_json: Optional[bool] = typer.Option(
        None,
        "--log-file-json/--no-log-file-json",
        help="Use JSON format for log files (defaults to same as console).",
    ),
):
    """
    Configure logging for the CLI.

    These options must be specified before the command run:

    Example:
        preprocess --log-json --log-level 10 run -i data.xlsx -s impress
    """
    # configure console logging
    configure(level=log_level, json_out=log_json)

    # update env with log file format
    if log_file_json is not None:
        os.environ["LOG_FILE_JSON"] = "1" if log_file_json else "0"
    else:
        # default to same as console format
        os.environ["LOG_FILE_JSON"] = "1" if log_json else "0"

    log.debug(
        f"Logging configured: level={log_level}, "
        f"console_json={log_json}, "
        f"file_json={os.environ.get('LOG_FILE_JSON', '?')}"
    )


@app.command("list-sources")
def list_sources_cmd() -> None:
    """List all available trial sources."""
    trials = list_trials()
    if trials:
        typer.echo("Available sources:")
        for trial in trials:
            typer.echo(f"  - {trial}")
    else:
        typer.echo("No trial sources registered.")


@app.command()
def run(
    input_path: Path = typer.Option(
        ...,
        "--input",
        "-i",
        help="Excel file or directory of CSVs.",
        exists=True,
        readable=True,
    ),
    source: str = typer.Option(
        "impress",
        "--source",
        "-s",
        help="Source pipeline to use.",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help=(
            "Output path. If it has a known extension (csv/tsv/parquet), "
            "it's used as the exact file. If it's a directory, the file "
            "will be auto-named. If omitted, writes to /.data/preprocessing/."
        ),
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Override default json/json5 config.",
        exists=True,
        readable=True,
    ),
    key: str = typer.Option(
        "SubjectId",
        "--key",
        "-k",
        help="Join key used when combining sheets.",
    ),
    only_cohort: bool = typer.Option(
        False,
        "--only-cohort",
        "-oc",
        help="Only keep subjects in cohorts.",
    ),
    output_format: str = typer.Option(
        "csv",
        "--output-format",
        "-of",
        help="Output format: csv | tsv | parquet",
    ),
    base_dir: Optional[Path] = typer.Option(
        None,
        "--base-dir",
        "-b",
        help="Base directory for outputs (overrides default .data/preprocessing).",
    ),
    no_log_file: bool = typer.Option(
        False,
        "--no-log-file",
        help="Disable creation of log file for this run.",
    ),
) -> None:
    """
    Run preprocessing pipeline for a trial source.

    Examples:
        # Use default settings
        preprocess run -i data.xlsx -s impress

        # Specify exact output file
        preprocess run -i data.xlsx -s impress -o results.parquet

        # Use custom config and filter cohort
        preprocess run -i data/ -s trial_1 -c custom_config.json5 --only-cohort
    """
    log.info(
        "preprocess.start",
        extra={
            "source": source,
            "input": str(input_path),
            "format": output_format,
        },
    )

    # validate source
    available_trials = list_trials()
    if source not in available_trials:
        typer.secho(
            f"Unknown source: '{source}'",
            fg=typer.colors.RED,
        )
        if available_trials:
            typer.echo(f"Available sources: {', '.join(sorted(available_trials))}")
        else:
            typer.echo("No trial sources are registered.")
        raise typer.Exit(code=2)

    try:
        # build runtime options
        run_options = RunOptions(filter_valid_cohort=only_cohort)

        # load or create config
        ecrf_config = make_ecrf_config(trial=source, custom_config_path=config)

        if no_log_file:
            os.environ["DISABLE_LOG_FILE"] = "1"
        else:
            os.environ.pop("DISABLE_LOG_FILE", None)

        # run preprocessing
        result: PreprocessResult = preprocess_trial(
            trial=source,
            input_path=input_path,
            config=ecrf_config,
            run_options=run_options,
            output=output,
            fmt=output_format,
            combine_key=key,
            base_output_dir=base_dir,
        )

        # log and display results
        log.info(
            "Preprocessing Done",
            extra={
                "path": str(result.output_path.data_file),
                "rows": result.rows,
                "columns": result.columns,
                "run_id": result.context.run_id,
            },
        )

        typer.echo(f"Output: {result.output_path.data_file}")
        if not no_log_file:
            typer.echo(f"Log file: {result.output_path.log_file}")
        typer.echo(f"Rows: {result.rows:,}")
        typer.echo(f"Columns: {result.columns}")
        typer.echo(f"Run ID: {result.context.run_id}")

        typer.secho(
            "Preprocessing completed successfully!",
            fg=typer.colors.GREEN,
        )

    except FileNotFoundError as e:
        typer.secho(f"File not found: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except ValueError as e:
        typer.secho(f"Configuration error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("Unexpected error during preprocessing")
        typer.secho(f"Unexpected error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command("validate")
def validate_config(
    config: Path = typer.Argument(
        ...,
        help="Path to config file to validate.",
        exists=True,
        readable=True,
    ),
    source: str = typer.Option(
        None,
        "--source",
        "-s",
        help="Trial source name (for context).",
    ),
) -> None:
    """
    Validate a configuration file.

    Checks that the config file is valid json5 and contains
    the expected structure for eCRF configurations.
    """
    try:
        trial = source or "validation"
        ecrf_config = make_ecrf_config(trial=trial, custom_config_path=config)

        typer.echo(f"Sheets configured: {len(ecrf_config.configs)}")
        for sheet_config in ecrf_config.configs:
            typer.echo(f"  - {sheet_config.key}: {len(sheet_config.usecols)} columns")

        typer.secho(
            "Config file is valid!",
            fg=typer.colors.GREEN,
        )

    except Exception as e:
        typer.secho(
            f"Invalid config: {e}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)


def main():
    app(standalone_mode=False)


if __name__ == "__main__":
    main()
