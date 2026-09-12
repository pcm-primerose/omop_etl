import argparse
from logging import getLogger
from pathlib import Path

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.harmonized import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.service import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure_logger
from omop_etl.omop.service import OmopService
from omop_etl.preprocessing.service import (
    make_ecrf_config,
    PreprocessService,
)
from omop_etl.preprocessing.core.models import PreprocessResult
from omop_etl.semantic_mapping.service import SemanticService
from omop_etl.semantic_mapping.core.models import SemanticMappingResult
from omop_etl.infra.utils.mapping_io import resolve_mapping_paths
from omop_etl.env_config import (
    ATHENA_DIR,
    DATA_ROOT,
    DEFAULT_DATASET,
    MAPPING_DIR,
    SYNTHETIC_DATASETS,
    resolve_dataset,
    LOG_LEVEL,
)
from omop_etl.vocabulary.service import VocabularyService, VocabularyResult

log = getLogger(__name__)


def run_pipeline(preprocessing_input: Path, base_root: Path, athena_dir: Path, mapping_dir: Path, trial: str = "IMPRESS") -> int:
    """
    End-to-end run of OMOP ETL.
    """
    base_root.mkdir(parents=True, exist_ok=True)
    paths = resolve_mapping_paths(mapping_dir)

    # set up configs & meta
    ecrf_config = make_ecrf_config(trial=trial)
    _meta = RunMetadata.create(trial)

    # validate mappings against Athena and hydrate the concept lookup vocabulary,
    # raises and stops the pipeline before anything else runs if mappings are invalid
    vocabulary_service = VocabularyService(
        outdir=base_root,
        athena_dir=athena_dir,
        mapping_files=paths.as_list(),
    )
    vocabulary_result: VocabularyResult = vocabulary_service.run(_meta)
    log.info("Validated mappings against Athena %s", vocabulary_result.athena_version)

    # run preprocessing
    preprocessor = PreprocessService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    preprocessing_result: PreprocessResult = preprocessor.run(
        trial=trial,
        input_path=preprocessing_input,
        config=ecrf_config,
        formats="csv",
        meta=_meta,
        combine_key="SubjectId",
        filter_valid_cohorts=True,
    )

    # run harmonization
    harmonizer = HarmonizationService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    harmonized_result: HarmonizedData = harmonizer.run(
        trial=trial,
        input_path=preprocessing_result.output_path.data_file,
        formats="csv",
        write_wide=True,
        write_normalized=True,
        meta=_meta,
    )

    # print(f"Harmonized: {harmonized_result.patients[0:10]}")

    # run semantic mapping
    semantic_mapper = SemanticService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    semantic_result: SemanticMappingResult = semantic_mapper.run(
        trial=trial,
        input_path=None,
        harmonized_data=harmonized_result,
        meta=_meta,
        semantic_path=paths.semantic,
        write_output=True,
    )

    # concept lookup service: loads static/structural mappings, tracks lookups
    concept_service = ConceptLookupService.from_paths(
        static_path=paths.static,
        structural_path=paths.structural,
        semantic_batch=semantic_result.batch_result,
        meta=_meta,
        outdir=base_root,
        layout=Layout.TRIAL_TIMESTAMP_RUN,
    )

    # build OMOP rows using the concept service
    omop_service = OmopService(concepts=concept_service)
    tables = omop_service.build(harmonized_result.patients)
    # print(f"cohort: {tables.location}")

    # export concept lookup tracking (missed lookups, coverage stats)
    concept_service.export(formats="csv")

    # just use a static default for testing locally
    # todo: integrate later
    dsn = "postgresql://omop:omop@localhost:5433/omop"
    if not dsn:
        raise SystemExit("Missing DSN. Provide --dsn or set DATABASE_URL.")

    # writer = PostgresOmopWriter(dsn=dsn, truncate_first=True)
    # writer.write(tables)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="OMOP ETL pipeline")
    parser.add_argument(
        "--dataset",
        default=None,
        help=f"Dataset name ({', '.join(SYNTHETIC_DATASETS)}) or explicit path. Defaults to 'impress_150'.",
    )
    parser.add_argument(
        "--athena-dir",
        type=Path,
        default=ATHENA_DIR,
        help=f"Dir containing this release's Athena CSVs (CONCEPT.csv, VOCABULARY.csv, ...). Defaults to {ATHENA_DIR}.",
    )
    parser.add_argument(
        "--mapping-dir",
        type=Path,
        default=MAPPING_DIR,
        help=f"Dir containing static.csv/structural.csv/semantic.csv. Defaults to {MAPPING_DIR}.",
    )
    args = parser.parse_args()

    dataset_path = resolve_dataset(args.dataset) if args.dataset else DEFAULT_DATASET
    if not dataset_path.exists():
        parser.error(f"Dataset path does not exist: {dataset_path}")
    if not args.athena_dir.exists():
        parser.error(f"Athena dir does not exist: {args.athena_dir}")
    if not args.mapping_dir.exists():
        parser.error(f"Mapping dir does not exist: {args.mapping_dir}")
    configure_logger(level=LOG_LEVEL)

    return run_pipeline(
        preprocessing_input=dataset_path,
        base_root=DATA_ROOT,
        athena_dir=args.athena_dir,
        mapping_dir=args.mapping_dir,
    )


if __name__ == "__main__":
    raise SystemExit(main())
