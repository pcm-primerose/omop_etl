# src/omop_etl/cli/preprocess.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import typer
import polars as pl

from omop_etl.preprocessing.core.config_loader import load_ecrf_config
from omop_etl.preprocessing.core.types import EcrfConfig
from omop_etl.preprocessing.core.pipeline import run_pipeline
from omop_etl.preprocessing.core.registry import SOURCES

# import source modules for each trial
from omop_etl.preprocessing.sources import impress  # noqa: F401

app = typer.Typer(
    add_completion=True,
    pretty_exceptions_enable=False,
    help="Preprocess eCRF data using source-specific pipelines.",
)


@app.command()
def run(
    input_path: Path = typer.Option(
        None, "--input", "-i", help="Excel file or directory of CSVs."
    ),
    source: str = typer.Option(
        "impress", "--source", "-s", help="Source pipeline to use."
    ),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output file path."
    ),
    config: Optional[Path] = typer.Option(
        None, "--config", "-c", help="Override default JSON config."
    ),
    key: str = typer.Option(
        "SubjectId", "--key", "-k", help="Join key used when combining sheets."
    ),
    fmt: str = typer.Option("csv", "--format", "-f", help="csv or tsv/txt"),
    list_sources: bool = typer.Option(
        False, "--list-sources", help="List available sources and exit."
    ),
) -> None:
    # discover and validate source
    if list_sources:
        typer.echo(", ".join(sorted(SOURCES.keys())))
        raise typer.Exit()

    if source not in SOURCES:
        typer.secho(
            f"Unknown source '{source}'. Available: {', '.join(sorted(SOURCES))}",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    # load config
    cfg_map = load_ecrf_config(custom_config_path=config, trial=source)
    cfg = EcrfConfig.from_mapping(cfg_map)
    cfg.trial = source

    # run pipeline
    df: pl.DataFrame = run_pipeline(
        source=source,
        input_path=input_path,
        cfg=cfg,
        combine_on=key,
    )

    # output handling
    fmt = fmt.lower()
    if output is None:
        ext = "csv" if fmt == "csv" else "txt"
        output = Path.cwd() / ".data" / "preprocessing" / f"preprocessed_{source}.{ext}"
    output.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.write_csv(output, null_value="NA")
    elif fmt in ("tsv", "txt"):
        df.write_csv(output, separator="\t", null_value="NA")
    else:
        typer.secho(
            "Unsupported --format. Use 'csv' or 'tsv/txt'.", fg=typer.colors.RED
        )
        raise typer.Exit(code=2)

    typer.echo(str(output))


def main():
    app(standalone_mode=False)


if __name__ == "__main__":
    main()
