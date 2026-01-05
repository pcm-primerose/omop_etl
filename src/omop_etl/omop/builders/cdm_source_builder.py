import datetime as dt

from omop_etl.omop.models.rows import CdmSourceRow
from omop_etl.concept_mapping.concept_service import ConceptMappingService

# todo: clean this up, make configurable


class CdmSourceBuilder:
    def __init__(self, concept_service: ConceptMappingService):
        self._concepts = concept_service

    def build_cdm_source(self):
        cdm_version_concept = self._concepts.row_concepts_for_value_set("cdm").concept_id
        cdm_vocabulary_version = str(self._concepts.row_concepts_for_value_set("vocab").concept_id)

        return CdmSourceRow(
            cdm_source_name="test ETL",
            cdm_source_abbreviation="test ETL",
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
