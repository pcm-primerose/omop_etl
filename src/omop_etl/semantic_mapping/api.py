from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.semantic_mapping.pipeline import SemanticLookupPipeline

# todo later
# [ ] create polars backend for indexing and querying entire Athena database: OOM, lazy, vectorized
# [ ] either extend models or create new ones in separate retrieval modules (lexical, vector etc) passed to rerankers
#     - but need to finish basic impl & set up evals first
# [ ] BM25 & vector search needs to yield struct similar to SemanticRow


class SemanticService:
    def __init__(self, harmonized_data: HarmonizedData, semantic_path: Path | None = None, output_path: Path | None = None):
        self.semantic_path = semantic_path
        self.harmonized_data = harmonized_data
        self.output_path = output_path

    def run(self):
        # pass field configs to override by name
        pipeline = SemanticLookupPipeline(semantics_path=self.semantic_path)
        batch = pipeline.run_lookup(harmonized_data=self.harmonized_data)
        return batch
