import argparse
from logging import getLogger
from pathlib import Path
from dotenv import load_dotenv

from omop_etl.db.service import DbLoadService
from omop_etl.harmonization.core.cohort_lookups import load_cohort_lookups
from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.harmonization.service import HarmonizationService
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.infra.logging.logging_setup import configure_logger
from omop_etl.preprocessing.service import make_ecrf_config, PreprocessService
from omop_etl.preprocessing.core.models import PreprocessResult
from omop_etl.semantic_mapping.service import SemanticService
from omop_etl.semantic_mapping.core.models import BatchQueryResult
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.infra.utils.mapping_io import resolve_mapping_paths
from omop_etl.vocabulary.service import VocabularyResult, VocabularyService

from omop_etl.omop.service import OmopService
from omop_etl.omop.core.io import OmopTableExporter
from omop_etl.omop.models.tables import OmopTables

log = getLogger(__name__)


def _resolve_target_biomarker(value: str | None) -> str | None:
    """Casefold and validate against the harmonized biomarker vocabulary, failing on a typo."""
    if value is None:
        return None
    wanted = value.casefold()
    known = {v.casefold() for v in load_cohort_lookups().biomarker.values()}
    if wanted not in known:
        raise SystemExit(f"Unknown --target-biomarker {value!r}; not a harmonized biomarker value.")
    return wanted


def run_pipeline(preprocessing_input: Path, base_root: Path, trial: str, meta: RunMetadata) -> HarmonizedData:
    base_root.mkdir(parents=True, exist_ok=True)

    ecrf_config = make_ecrf_config(trial=trial)

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

    # todo: move rest of services to here, re-use run context


# todo: always use semantic & static (remove args basically)
def _build_tables(
    harmonized: HarmonizedData,
    meta: RunMetadata,
    outdir: Path,
    *,
    mapping_dir: Path,
    with_semantic: bool,
    vocabulary_result: VocabularyResult,
) -> OmopTables:
    paths = resolve_mapping_paths(mapping_dir)

    semantic_batch: BatchQueryResult | None = None
    if with_semantic:
        result = SemanticService().run(harmonized_data=harmonized, meta=meta, trial=meta.trial, semantic_path=paths.semantic)
        semantic_batch = result.batch_result

    concept_service = ConceptLookupService.from_paths(
        static_path=paths.static,
        structural_path=paths.structural,
        semantic_batch=semantic_batch,
        meta=meta,
        outdir=outdir,
        layout=Layout.TRIAL_TIMESTAMP_RUN,
    )

    omop_service = OmopService(
        concepts=concept_service,
        vocabulary=vocabulary_result.vocabulary,
        concept_ancestor=vocabulary_result.concept_ancestor,
        athena_version=vocabulary_result.athena_version,
        outdir=outdir,
    )
    return omop_service.build(harmonized.patients, meta=meta)


def cmd_load(args: argparse.Namespace) -> int:
    configure_logger(level=args.log_level)
    meta = RunMetadata.create(args.trial)
    wanted_biomarker = _resolve_target_biomarker(args.target_biomarker)

    # The ETL must never run on invalid mappings: this validates the mapping files
    # against Athena and raises before anything else runs if there's a problem.
    vocabulary_result = VocabularyService(
        outdir=args.outdir,
        athena_dir=args.athena_dir,
        mapping_files=resolve_mapping_paths(args.mapping_dir).as_list(),
    ).run(meta)

    harmonized = run_pipeline(
        preprocessing_input=args.input,
        base_root=args.outdir,
        trial=args.trial,
        meta=meta,
    )

    if wanted_biomarker is not None:
        observed = sorted({biomarker for p in harmonized.patients if (cohort := p.cohort) is not None if (biomarker := cohort.target_biomarker) is not None})
        harmonized = harmonized.filter(
            lambda p: p.cohort is not None and p.cohort.target_biomarker is not None and p.cohort.target_biomarker.casefold() == wanted_biomarker
        )
        if not harmonized.patients:
            raise SystemExit(
                f"No patients matched --target-biomarker {args.target_biomarker!r}, stopping before OMOP build. "
                f"target_biomarker values actually present in this run: {observed}"
            )
        log.info(f"--target-biomarker {args.target_biomarker!r} matched {len(harmonized.patients)} patients")

    _build_tables(
        harmonized,
        meta=meta,
        outdir=args.outdir,
        mapping_dir=args.mapping_dir,
        with_semantic=args.with_semantic,
        vocabulary_result=vocabulary_result,
    )

    dsn = args.dsn or args.database_url
    if not dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set DATABASE_URL.")

    omop_dir = OmopTableExporter(args.outdir).output_dir(meta)
    DbLoadService(dsn=dsn).load(
        omop_dir=omop_dir,
        athena_dir=args.athena_dir,
        athena_version=vocabulary_result.athena_version,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    p = argparse.ArgumentParser(prog="etl")
    sub = p.add_subparsers(dest="cmd", required=True)

    load = sub.add_parser("load", help="Run ETL and load OMOP tables into Postgres")
    load.add_argument("--input", type=Path, required=True)
    load.add_argument("--outdir", type=Path, required=True)
    load.add_argument("--trial", default="IMPRESS")
    load.add_argument("--athena-dir", type=Path, required=True, help="Dir containing this release's Athena CSVs (CONCEPT.csv, VOCABULARY.csv, ...)")
    load.add_argument("--mapping-dir", type=Path, required=True, help="Dir containing static.csv/structural.csv/semantic.csv")
    load.add_argument("--target-biomarker", default=None, help="Only load patients whose Cohort.target_biomarker matches this (case-insensitive)")

    load.add_argument("--dsn", default=None)
    load.add_argument("--with-semantic", action="store_true", help="Enable semantic mapping")
    load.add_argument("--log-level", default="INFO")
    load.set_defaults(func=cmd_load)

    # allow DATABASE_URL env without forcing python-dotenv to be present in prod,
    # dotenv is in deps anyways
    args = p.parse_args(argv)
    args.database_url = __import__("os").environ.get("DATABASE_URL")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
