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


# from __future__ import annotations
# from pathlib import Path
# from typing import Optional
# import typer
# from importlib.resources import files, as_file
#
# from omop_etl.pre_processing.impress.impress_ecrf import impress_preprocessor, load_ecrf_config
#
# app = typer.Typer(add_completion=False, help="Pre-process eCRF data")
#
# def _must_exist(p: Path) -> Path:
#     if not p.exists():
#         raise typer.BadParameter(f"Path does not exist: {p}")
#     return p
#
# @app.command()
# def run(
#     input_path: Path = typer.Argument(..., callback=_must_exist, help="Excel file or directory of CSVs."),
#     output_path: Optional[Path] = typer.Option(None, "--output", "-o", help="Where to write the preprocessed dataset."),
#     config: Optional[Path] = typer.Option(None, "--config", "-c", help="Custom JSON config; defaults to packaged one."),
#     log_path: Optional[Path] = typer.Option(None, "--log", help="Optional log file path."),
#     output_format: str = typer.Option("csv", "--format", "-f", help="csv or txt", case_sensitive=False),
#     mock_data: bool = typer.Option(False, "--mock-data/--no-mock-data", help="Use mock CSV naming."),
# ) -> None:
#
#     fmt = output_format.lower()
#     if fmt not in {"csv", "txt"}:
#         raise typer.BadParameter("format must be 'csv' or 'txt'")
#
#     if output_path is None:
#         default_name = f"preprocessed_impress_synthetic.{fmt}"
#         output_path = Path.cwd() / ".data" / "preprocessing" / default_name
#     output_path.parent.mkdir(parents=True, exist_ok=True)
#
#     if config is None:
#         res = files("omop_etl.resources") / "impress_ecrf_variables.json"
#         with as_file(res) as p:
#             config = Path(p)
#
#     cfg_data = load_ecrf_config(custom_config_path=config)
#
#     written = impress_preprocessor(
#         input_path=input_path,
#         output_path=output_path,
#         config=cfg_data,
#         log_path=log_path,
#         output_format=fmt,
#         mock_data=mock_data,
#     )
#     typer.echo(str(written))
#
# def main():
#     app()
