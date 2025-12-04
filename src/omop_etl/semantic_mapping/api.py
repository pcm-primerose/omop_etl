from pathlib import Path

from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.semantic_mapping.models import FieldConfig, QueryTarget, OmopDomain
from omop_etl.semantic_mapping.pipeline import SemanticLookupPipeline


# todo b4 mvp done
# [ ] add semantic mapping file to resources, load from env, override if provided
# [ ] create default config set, override by name if provided

# todo later
# [ ] add semantic file to env, load from env if avail
# [ ] extract to separate files later; types, datamodels, etc, think about how to make it nice for extension later
# [ ] create polars backend for indexing and querying entire Athena database: OOM, lazy, vectorized
# [ ] either extend models or create new ones in separate retrieval modules (lexical, vector etc) passed to rerankers
#     - but need to finish basic impl & set up evals first
# [ ] BM25 & vector search needs to yield struct similar to SemanticRow


class SemanticService:
    def __init__(self, semantic_path: Path, harmonized_data: HarmonizedData, output_path: Path | None = None):
        self.semantic_path = semantic_path
        self.harmonized_data = harmonized_data
        self.output_path = output_path

    # todo: create all configs
    def run(self):
        field_configs = [
            FieldConfig(
                name="tumor.main",
                field_path=("tumor_type", "main_tumor_type"),
                target=QueryTarget([OmopDomain.CONDITION, OmopDomain.OBSERVATIONS]),
                tags="tumor",
            ),
            FieldConfig(
                name="ae.term",
                field_path=("adverse_events", "term"),
                target=QueryTarget(domains=[OmopDomain.CONDITION]),
                tags="adverse_events",
            ),
            # FieldConfig(name=""),
        ]

        pipeline = SemanticLookupPipeline(
            semantics_path=Path(self.semantic_path),
            field_configs=field_configs,
        )

        batch = pipeline.run_lookup(harmonized_data=self.harmonized_data)

        for q in batch.missing:
            print("NO MATCH:", q.patient_id, q.field_path, q.raw_value)

        for qr in batch.matches:
            print(f"MATCH: {qr.query.patient_id, qr.query.field_path, qr.query.raw_value} --- {qr.results}")

        return batch
