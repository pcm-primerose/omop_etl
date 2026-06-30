import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.visit import Visit
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.service import OmopService
from omop_etl.harmonization.models.domain.end_of_treatment import (
    EndOfTreatment,
    TrialOutcomeStatus,
)
from tests.omop.conftest import (
    concept,
    create_patient,
    mapping,
    semantic_index,
)


def _with_eot(patient: Patient, date: dt.date) -> None:
    """Attach an EndOfTreatment singleton with WITHDRAWN status and date."""
    eot = EndOfTreatment(patient_id=patient.patient_id)
    eot.status = TrialOutcomeStatus.WITHDRAWN
    eot.date = date
    patient.end_of_treatment = eot


class TestOmopServiceOrchestration:
    def test_builds_person_and_observation_period(self, static_index, structural_index):
        """Minimal patient produces person and observation_period rows."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 5, 15),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(patient, dt.date(2023, 6, 30))

        tables = OmopService(concepts).build([patient])

        assert len(tables.person) == 1
        assert len(tables.observation_period) == 1
        assert tables.cdm_source is not None

    def test_all_builders_produce_output(self, static_index, structural_index):
        """A fully-populated patient with semantic entries produces rows in all tables."""
        semantic = semantic_index(
            mapping((Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE), "C50.9", concept(4000, "condition")),
            mapping((Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM), "Hypertension", concept(316866, "condition")),
            mapping((Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM), "Fever", concept(437663, "condition")),
            mapping(
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                "Trametinib",
                concept(1234, "drug", vocab="rxnorm"),
            ),
            mapping(
                (Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
                "Oxynorm",
                concept(1124957, "drug", vocab="rxnorm"),
            ),
            mapping((Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.TREATMENT), "Surgery", concept(4301351, "procedure")),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)

        patient = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 5, 15),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(patient, dt.date(2023, 6, 30))
        tumor = TumorType(patient_id="p1")
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        mh = MedicalHistory(patient_id="p1")
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        ae = AdverseEvent(patient_id="p1")
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        cycle = TreatmentCycleComponent(patient_id="p1")
        cycle.source_treatment_name = "Trametinib"
        cycle.start_date = dt.date(2023, 1, 15)
        patient.treatment_cycles = [cycle]

        concom = ConcomitantMedication(patient_id="p1")
        concom.medication_name = "Oxynorm"
        concom.start_date = dt.date(2023, 2, 1)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        prev = PreviousTreatment(patient_id="p1")
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        baseline = TumorAssessment(patient_id="p1")
        baseline.was_baseline = True
        baseline.assessment_type = "recist"
        baseline.date = dt.date(2023, 1, 10)
        baseline.event_id = "V00"
        patient.tumor_assessments = [baseline]

        # visits drive visit_occurrence, the baseline visit shares the assessment's date
        visit = Visit(patient_id="p1")
        visit.date = dt.date(2023, 1, 10)
        visit.event_id = "V00"
        patient.visits = [visit]

        tables = OmopService(concepts).build([patient])

        assert len(tables.person) == 1
        assert len(tables.observation_period) == 1
        assert len(tables.visit_occurrence) >= 1
        assert len(tables.condition_occurrence) >= 1
        assert len(tables.drug_exposure) >= 1
        assert len(tables.procedure_occurrence) >= 1
        assert tables.cdm_source is not None


class TestMultiPatient:
    def test_multiple_patients(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p1 = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 5, 15),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p1, dt.date(2023, 6, 30))
        p2 = create_patient(
            "p2",
            "test",
            sex="f",
            date_of_birth=dt.date(1990, 3, 20),
            treatment_start_date=dt.date(2023, 2, 1),
        )
        _with_eot(p2, dt.date(2023, 7, 15))

        tables = OmopService(concepts).build([p1, p2])

        assert len(tables.person) == 2
        assert len(tables.observation_period) == 2

    def test_person_ids_are_unique_across_patients(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p1 = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p1, dt.date(2023, 6, 30))
        p2 = create_patient(
            "p2",
            "test",
            sex="f",
            date_of_birth=dt.date(1990, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p2, dt.date(2023, 6, 30))

        tables = OmopService(concepts).build([p1, p2])

        person_ids = [r.person_id for r in tables.person]
        assert len(person_ids) == len(set(person_ids))

    def test_person_ids_are_deterministic(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(patient, dt.date(2023, 6, 30))

        t1 = OmopService(concepts).build([patient])
        t2 = OmopService(concepts).build([patient])

        assert t1.person[0].person_id == t2.person[0].person_id


class TestSkipBehavior:
    def test_missing_dob_skips_person_but_not_observation_period(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p_ok = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p_ok, dt.date(2023, 6, 30))
        p_no_dob = create_patient(
            "p2",
            "test",
            sex="m",
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p_no_dob, dt.date(2023, 6, 30))

        tables = OmopService(concepts).build([p_ok, p_no_dob])

        assert len(tables.person) == 1
        assert len(tables.observation_period) == 2

    def test_missing_treatment_dates_skips_observation_period_but_not_person(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        p_ok = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(p_ok, dt.date(2023, 6, 30))
        p_no_dates = create_patient("p2", "test", sex="f", date_of_birth=dt.date(1985, 8, 10))

        tables = OmopService(concepts).build([p_ok, p_no_dates])

        assert len(tables.person) == 2
        assert len(tables.observation_period) == 1


class TestEmptyInput:
    def test_empty_patients_list(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)

        tables = OmopService(concepts).build([])

        assert len(tables.person) == 0
        assert len(tables.observation_period) == 0
        assert len(tables.visit_occurrence) == 0
        assert len(tables.drug_exposure) == 0
        assert len(tables.condition_occurrence) == 0
        assert len(tables.procedure_occurrence) == 0
        assert tables.cdm_source is not None


class TestOmopTablesApi:
    def test_getitem(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 1, 1),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(patient, dt.date(2023, 6, 30))

        tables = OmopService(concepts).build([patient])

        assert len(tables["person"]) == 1
        assert len(tables["observation_period"]) == 1
        assert len(tables["cdm_source"]) == 1

    def test_get_with_default(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)

        tables = OmopService(concepts).build([])

        assert tables.get("condition_occurrence") == []
        assert tables.get("nonexistent", []) == []

    def test_typed_properties(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(
            "p1",
            "test",
            sex="m",
            date_of_birth=dt.date(1980, 5, 15),
            treatment_start_date=dt.date(2023, 1, 1),
        )
        _with_eot(patient, dt.date(2023, 6, 30))

        tables = OmopService(concepts).build([patient])

        assert tables.person[0].person_source_value == "p1"
        assert tables.observation_period[0].observation_period_start_date == dt.date(2023, 1, 1)
        assert tables.cdm_source is not None
        cdm_row = tables.cdm_source
        assert cdm_row is not None
        assert cdm_row.cdm_holder == "PRIME-ROSE"
