from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.semantic_mapping.pipeline import SemanticLookupPipeline

# todo later
# [ ] add semantic file to env, load from env if avail
# [ ] extract to separate files later; types, datamodels, etc, think about how to make it nice for extension later
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

        for sample in batch.matches:
            print(f"FP: {sample.query.field_path}")

        # for q in batch.missing:
        #     print("NO MATCH:", q.patient_id, q.field_path, q.raw_value)
        #
        # for qr in batch.matches:
        #     print(f"MATCH: {qr.query.patient_id, qr.query.field_path, qr.query.raw_value} --- {qr.results}")

        return batch
