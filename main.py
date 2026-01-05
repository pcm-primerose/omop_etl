from pathlib import Path

from omop_etl.concept_mapping.api import ConceptLookupService
from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.infra.io.types import Layout
from omop_etl.infra.utils.run_context import RunMetadata
from omop_etl.harmonization.api import HarmonizationService
from omop_etl.infra.logging.logging_setup import configure_logger
from omop_etl.omop.build import BuildOmopRows
from omop_etl.omop.models.tables import OmopTables
from omop_etl.preprocessing.api import make_ecrf_config, PreprocessService
from omop_etl.preprocessing.core.models import PreprocessResult
from omop_etl.semantic_mapping.api import SemanticService
from omop_etl.semantic_mapping.core.models import SemanticMappingResult

# default resource paths
RESOURCES_DIR = Path(__file__).parent / "src" / "omop_etl" / "resources" / "static_mapped"
DEFAULT_STATIC_CSV = RESOURCES_DIR / "static_mapping.csv"
DEFAULT_STRUCTURAL_CSV = RESOURCES_DIR / "structural_mapping.csv"


def run_pipeline(preprocessing_input: Path, base_root: Path, trial: str = "IMPRESS") -> SemanticMappingResult:
    """
    End-to-end test run on synthetic data for implemented modules.
    """
    base_root.mkdir(parents=True, exist_ok=True)

    # set up configs & meta
    ecrf_config = make_ecrf_config(trial=trial)
    _meta = RunMetadata.create(trial)

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

    # run semantic mapping
    semantic_mapper = SemanticService(outdir=base_root, layout=Layout.TRIAL_TIMESTAMP_RUN)
    semantic_result: SemanticMappingResult = semantic_mapper.run(
        trial=trial,
        input_path=None,  # todo: test running from just harmonized files later
        harmonized_data=harmonized_result,
        meta=_meta,
        write_output=True,
    )

    # concept lookup service - loads static/structural mappings, tracks lookups
    concept_service = ConceptLookupService.from_paths(
        static_path=DEFAULT_STATIC_CSV,
        structural_path=DEFAULT_STRUCTURAL_CSV,
        semantic_batch=semantic_result.batch_result,
        meta=_meta,
        outdir=base_root,
        layout=Layout.TRIAL_TIMESTAMP_RUN,
    )

    # build OMOP rows using the concept service
    builder = BuildOmopRows(
        harmonized_data=harmonized_result,
        concepts=concept_service,
    )
    tables: OmopTables = builder.build_all_rows()

    # export concept lookup tracking (missed lookups, coverage stats)
    concept_service.export(formats="csv")

    print(f"Tables: {tables}")

    return semantic_result


if __name__ == "__main__":
    configure_logger(level="DEBUG")
    run_pipeline(
        preprocessing_input=Path(__file__).parent / ".data" / "synthetic" / "nonv600_cohorts", base_root=Path(__file__).parent / ".data"
    )
