import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.cdm_source import CdmSourceBuilder


class TestCdmSourceBuilder:
    def test_builds_cdm_source(self, static_index, structural_index):
        row = CdmSourceBuilder(ConceptLookupService(static_index, structural_index), athena_version="v5.0 01-JAN-26").build()

        assert row.cdm_source_name == "PRIME-ROSE OMOP ETL"
        assert row.cdm_holder == "PRIME-ROSE"
        assert row.cdm_version == "v5.4"
        assert row.cdm_version_concept_id == 705800
        assert row.vocabulary_version == "v5.0 01-JAN-26"
        assert row.source_release_date == dt.date.today()
