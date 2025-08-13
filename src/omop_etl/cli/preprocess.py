from __future__ import annotations
from pathlib import Path
from typing import Optional
import typer

from omop_etl.preprocessing.api import (
    preprocess_trial,
    make_ecrf_config,
    list_trials,
    RunOptions,
)

app = typer.Typer(
    add_completion=True,
    pretty_exceptions_enable=False,
    help="Preprocess eCRF data using source-specific pipelines.",
)


@app.command("list-sources")
def list_sources_cmd() -> None:
    from omop_etl.preprocessing.api import list_trials

    typer.echo(", ".join(list_trials()))


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
        "impress", "--source", "-s", help="Source pipeline to use."
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help=(
            "Output path. If it has a known extension (csv/tsv/parquet), it's used as the exact file. "
            "If it's a directory, the file will be auto-named (preprocessed_{source}.{fmt}). "
            "If omitted, writes to ./.data/preprocessing/preprocessed_{source}.{fmt}."
        ),
    ),
    config: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Override default JSON/JSON5 config."
    ),
    key: str = typer.Option(
        "SubjectId", "--key", "-k", help="Join key used when combining sheets."
    ),
    only_cohort: bool = typer.Option(
        False, "--only-cohort", "-oc", help="Only keep subjects in cohorts."
    ),
    fmt: str = typer.Option("csv", "--format", "-f", help="csv | tsv | parquet"),
) -> None:
    trials = set(list_trials())
    if source not in trials:
        typer.secho(
            f"Unknown source: {source}. Available: {', '.join(sorted(trials))}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    # build runtime options and config
    run_opts = RunOptions(filter_valid_cohort=only_cohort)
    ecfg = make_ecrf_config(source, custom_config_path=config)
    ecfg.trial = source

    # API handles output-path policy
    result = preprocess_trial(
        trial=source,
        input_path=input_path,
        cfg=ecfg,
        run_opts=run_opts,
        output=output,
        fmt=fmt,
        combine_on=key,
    )

    typer.echo(str(result.path))


def main():
    app(standalone_mode=False)


if __name__ == "__main__":
    main()
