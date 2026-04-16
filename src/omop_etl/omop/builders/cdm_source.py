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
        cdm_version_concept = self.concepts.lookup_structural("cdm", domains={"Metadata"}).concept_id
        cdm_vocabulary_version = str(self.concepts.lookup_structural("vocab", domains={"Metadata"}).concept_id)

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
            cdm_version_concept_id=cdm_version_concept,
            vocabulary_version=cdm_vocabulary_version,
        )
