import argparse
from pathlib import Path
from dotenv import load_dotenv

from omop_etl.db.postgres import PostgresOmopWriter
from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.logging.logging_setup import configure_logger
from omop_etl.preprocessing.api import make_ecrf_config, PreprocessService
from omop_etl.preprocessing.core.models import PreprocessResult
from omop_etl.semantic_mapping.api import SemanticService
from omop_etl.semantic_mapping.models import BatchQueryResult

from omop_etl.omop.build import BuildOmopRows
from omop_etl.omop.models.tables import OmopTables


def run_pipeline(preprocessing_input: Path, base_root: Path, trial: str) -> HarmonizedData:
    base_root.mkdir(parents=True, exist_ok=True)

    ecrf_config = make_ecrf_config(trial=trial)
    meta = RunMetadata.create(trial)

    preprocessor = PreprocessService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    preprocessing_result: PreprocessResult = preprocessor.run(
        trial=trial,
        input_path=preprocessing_input,
        config=ecrf_config,
        formats="csv",
        meta=meta,
        combine_key="SubjectId",
        filter_valid_cohorts=True,
    )

    harmonizer = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    harmonized_result: HarmonizedData = harmonizer.run(
        trial=trial,
        input_path=preprocessing_result.output_path.data_file,
        formats="csv",
        write_wide=True,
        write_normalized=True,
        meta=meta,
    )
    return harmonized_result


def _build_tables(harmonized: HarmonizedData, *, static_mapping: Path, structural_mapping: Path, with_semantic: bool) -> OmopTables:
    semantic_batch: BatchQueryResult | None = None
    if with_semantic:
        semantic_batch = SemanticService(harmonized_data=harmonized).run()

    builder = BuildOmopRows(
        harmonized_data=harmonized,
        static_mapping_path=static_mapping,
        semantic_batch=semantic_batch,
        structural_mapping_path=structural_mapping,
    )
    return builder.build_all_rows()


def cmd_load(args: argparse.Namespace) -> int:
    configure_logger(level=args.log_level)

    harmonized = run_pipeline(
        preprocessing_input=args.input,
        base_root=args.outdir,
        trial=args.trial,
    )

    tables = _build_tables(
        harmonized,
        static_mapping=args.static_mapping,
        structural_mapping=args.structural_mapping,
        with_semantic=args.with_semantic,
    )

    dsn = args.dsn or args.database_url
    if not dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set DATABASE_URL.")

    writer = PostgresOmopWriter(dsn=dsn, truncate_first=args.truncate)
    writer.write(tables)
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    p = argparse.ArgumentParser(prog="etl")
    sub = p.add_subparsers(dest="cmd", required=True)

    load = sub.add_parser("load", help="Run ETL and load OMOP tables into Postgres")
    load.add_argument("--input", type=Path, required=True)
    load.add_argument("--outdir", type=Path, required=True)
    load.add_argument("--trial", default="IMPRESS")
    load.add_argument("--static-mapping", type=Path, required=True)
    load.add_argument("--structural-mapping", type=Path, required=True)

    load.add_argument("--dsn", default=None)
    load.add_argument("--truncate", action="store_true")
    load.add_argument("--with-semantic", action="store_true", help="Enable semantic mapping (may require internet)")
    load.add_argument("--log-level", default="INFO")
    load.set_defaults(func=cmd_load)

    # allow DATABASE_URL env without forcing python-dotenv to be present in prod,
    # dotenv is in deps anyways
    args = p.parse_args(argv)
    args.database_url = __import__("os").environ.get("DATABASE_URL")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
