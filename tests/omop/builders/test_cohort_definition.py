import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.cohort import Cohort
from omop_etl.harmonization.models.domain.end_of_treatment import EndOfTreatment
from omop_etl.omop.builders.cohort import CohortBuilder
from omop_etl.omop.builders.cohort_definition import CohortDefinitionBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
from tests.omop.conftest import (
    create_patient,
    create_build_context,
)


PID = "p1"
TRIAL = "IMPRESS"
PERSON_ID = sha256_bigint("person", PID)


def _cohort(
    normalized_name: str | None,
    *,
    patient_id: str = PID,
    raw_name: str = "raw cohort",
) -> Cohort:
    c = Cohort(patient_id)
    c.raw_name = raw_name
    c.normalized_name = normalized_name
    return c


def _eot(date: dt.date, patient_id: str = PID) -> EndOfTreatment:
    eot = EndOfTreatment(patient_id)
    eot.date = date
    return eot


class TestCohortDefinitionBuilder:
    def test_one_definition_per_distinct_cohort(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p1 = create_patient("p1", TRIAL)
        p1.cohort = _cohort("Cohort A", patient_id="p1")
        p2 = create_patient("p2", TRIAL)
        p2.cohort = _cohort("Cohort A", patient_id="p2")  # same cohort as p1
        p3 = create_patient("p3", TRIAL)
        p3.cohort = _cohort("Cohort B", patient_id="p3")

        definitions = CohortDefinitionBuilder(concepts).build([p1, p2, p3])

        assert len(definitions) == 2
        assert {d.cohort_definition_name for d in definitions} == {"Cohort A", "Cohort B"}
        assert len({d.cohort_definition_id for d in definitions}) == 2

    def test_name_description_and_concepts(self, static_index, structural_index):
        # the real normalized name embeds the drugs, the description is the raw source string
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.cohort = _cohort(
            "BRAFV600 / Melanoma / Vemurafenib + Cobimetinib",
            raw_name="BRAFV600 melanoma, vemurafenib + cobimetinib",
        )

        definitions = CohortDefinitionBuilder(concepts).build([patient])

        assert len(definitions) == 1
        d = definitions[0]
        assert d.cohort_definition_name == "BRAFV600 / Melanoma / Vemurafenib + Cobimetinib"
        assert d.cohort_definition_description == "BRAFV600 melanoma, vemurafenib + cobimetinib"  # raw_name
        assert d.definition_type_concept_id == structural_index["ecrf"].concept_id
        assert d.subject_concept_id == structural_index["cohort_subject"].concept_id

    def test_skips_unnormalized_cohort(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p1 = create_patient("p1", TRIAL)
        p1.cohort = _cohort(None, patient_id="p1")  # didn't normalize
        p2 = create_patient("p2", TRIAL)
        p2.cohort = _cohort("Cohort A", patient_id="p2")

        definitions = CohortDefinitionBuilder(concepts).build([p1, p2])

        assert [d.cohort_definition_name for d in definitions] == ["Cohort A"]

    def test_no_cohorts_yields_no_definitions(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)  # no cohort

        assert CohortDefinitionBuilder(concepts).build([patient]) == []

    def test_concepts_default_to_zero_when_unseeded(self, static_index):
        # no structural concepts (no ecrf, no cohort_subject): both default to 0
        concepts = ConceptLookupService(static_index, {})
        patient = create_patient(PID, TRIAL)
        patient.cohort = _cohort("Cohort A")

        d = CohortDefinitionBuilder(concepts).build([patient])[0]

        assert d.definition_type_concept_id == 0
        assert d.subject_concept_id == 0


class TestCohortIdentity:
    def test_definition_id_joins_cohort_row(self, static_index, structural_index):
        # the membership row and the cohort definition resolve to the same id, so they join
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))
        patient.cohort = _cohort("Cohort A")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))
        ctx = create_build_context(patient, PERSON_ID)

        cohort_row = CohortBuilder(concepts).build(ctx).rows[0]
        definition = CohortDefinitionBuilder(concepts).build([patient])[0]

        assert cohort_row.cohort_definition_id == definition.cohort_definition_id

    def test_id_is_trial_independent(self, static_index, structural_index):
        # the id keys only on the (cross-trial canonical) normalized name,
        # so the same cohort in different trials merges into one definition
        concepts = ConceptLookupService(static_index, structural_index)
        impress = create_patient("p1", "IMPRESS")
        impress.cohort = _cohort("Cohort A", patient_id="p1")
        drup = create_patient("p2", "DRUP")
        drup.cohort = _cohort("Cohort A", patient_id="p2")

        definitions = CohortDefinitionBuilder(concepts).build([impress, drup])

        assert len(definitions) == 1
