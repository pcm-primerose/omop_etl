import datetime as dt


from omop_etl.omop.builders.cdm_source_builder import CdmSourceBuilder


class TestCdmSourceBuilder:
    def test_builds_cdm_source(self, mock_concepts):
        builder = CdmSourceBuilder(mock_concepts)

        row = builder.build()

        assert row.cdm_source_name == "test ETL"
        assert row.cdm_holder == "PRIME-ROSE"
        assert row.cdm_version == "v5.4"
        assert row.cdm_version_concept_id == 756265
        assert row.vocabulary_version == "0"  # str(concept_id)
        assert row.source_release_date == dt.date.today()
