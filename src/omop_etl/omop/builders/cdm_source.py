import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.models.rows import CdmSourceRow


class CdmSourceBuilder:
    """
    Builds the singleton CdmSource row with CDM metadata.
    """

    def __init__(self, concepts: ConceptLookupService, athena_version: str):
        self.concepts = concepts
        self.athena_version = athena_version

    def build(self) -> CdmSourceRow:
        cdm_concept = self.concepts.resolve("cdm", domains={"Metadata"})
        cdm_version_concept_id = int(cdm_concept[0].concept_id) if cdm_concept else 0

        # todo: improve this later
        return CdmSourceRow(
            cdm_source_name="PRIME-ROSE OMOP ETL",
            cdm_source_abbreviation="PR-OMOP-ETL",
            cdm_holder="PRIME-ROSE",
            source_description="PRIME-ROSE OMOP ETL",
            source_documentation_reference="PRIME-ROSE github docs",
            cdm_etl_reference=None,
            source_release_date=dt.date.today(),
            cdm_release_date=dt.date.today(),
            cdm_version="v5.5",
            cdm_version_concept_id=cdm_version_concept_id,
            vocabulary_version=self.athena_version,
        )
