import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.cohort import Cohort
from omop_etl.harmonization.models.domain.end_of_treatment import EndOfTreatment
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.omop.builders.cohort import CohortBuilder
from omop_etl.omop.core.id_generator import row_id, sha256_bigint
from omop_etl.omop.models.tables import OmopTables
from tests.omop.conftest import create_patient, create_build_context


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


def _cycle(start: dt.date) -> TreatmentCycleComponent:
    c = TreatmentCycleComponent(patient_id=PID)
    c.start_date = start
    return c


class TestCohortBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert CohortBuilder(concepts).table_name == "cohort"

    def test_emits_membership_row(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))
        patient.cohort = _cohort("BRAF V600 / Melanoma")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.subject_id == PERSON_ID
        # FK to cohort_definition: the content id keyed on the name only, recomputable here
        assert row.cohort_definition_id == row_id(OmopTables.COHORT_DEFINITION, "BRAF V600 / Melanoma")
        assert row.cohort_start_date == dt.date(2023, 1, 1)
        assert row.cohort_end_date == dt.date(2023, 6, 1)

    def test_start_falls_back_to_earliest_cycle(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.cohort = _cohort("Cohort A")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))
        patient.treatment_cycles = [_cycle(dt.date(2023, 2, 1)), _cycle(dt.date(2023, 1, 15))]

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows[0].cohort_start_date == dt.date(2023, 1, 15)

    def test_end_prefers_eot_over_death(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1), date_of_death=dt.date(2024, 3, 1))
        patient.cohort = _cohort("Cohort A")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows[0].cohort_end_date == dt.date(2023, 6, 1)

    def test_end_falls_back_to_death(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1), date_of_death=dt.date(2024, 3, 1))
        patient.cohort = _cohort("Cohort A")

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows[0].cohort_end_date == dt.date(2024, 3, 1)

    def test_end_falls_back_to_last_cycle(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1), treatment_start_last_cycle=dt.date(2023, 9, 1))
        patient.cohort = _cohort("Cohort A")

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows[0].cohort_end_date == dt.date(2023, 9, 1)

    def test_skips_unnormalized_cohort(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))
        patient.cohort = _cohort(None, raw_name="some unmapped cohort")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()

    def test_skips_without_cohort(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()

    def test_skips_without_start(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        patient.cohort = _cohort("Cohort A")
        patient.end_of_treatment = _eot(dt.date(2023, 6, 1))

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()

    def test_skips_without_end(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))
        patient.cohort = _cohort("Cohort A")

        result = CohortBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result.rows == ()
