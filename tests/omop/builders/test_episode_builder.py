from omop.conftest import create_patient, create_build_context
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.episode import EpisodeBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
from omop_etl.omop.core.linkage import BuildResult

PID = "p1"
TRIAL = "test"
PERSON_ID = sha256_bigint("person", PID)


class TestEpisodeBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert EpisodeBuilder(concepts).table_name == "episode"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        result = EpisodeBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())
