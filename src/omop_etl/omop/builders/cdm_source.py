import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.models.rows import CdmSourceRow


class CdmSourceBuilder:
    """
    Builds the singleton CdmSource row with CDM metadata.
    """

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    def build(self) -> CdmSourceRow:
        cdm_concept = self.concepts.lookup_structural("cdm", domains={"Metadata"})
        vocab_concept = self.concepts.lookup_structural("vocab", domains={"Metadata"})
        cdm_version_concept_id = int(cdm_concept.concept_id) if cdm_concept else 0
        cdm_vocabulary_version = str(vocab_concept.concept_id) if vocab_concept else ""

        return CdmSourceRow(
            cdm_source_name="PRIME-ROSE OMOP ETL",
            cdm_source_abbreviation="PR-OMOP-ETL",
            cdm_holder="PRIME-ROSE",
            source_description="PRIME-ROSE OMOP ETL",
            source_documentation_reference="PRIME-ROSE github",
            cdm_etl_reference=None,
            source_release_date=dt.date.today(),
            cdm_release_date=dt.date.today(),
            cdm_version="v5.4",
            cdm_version_concept_id=cdm_version_concept_id,
            vocabulary_version=cdm_vocabulary_version,
        )
