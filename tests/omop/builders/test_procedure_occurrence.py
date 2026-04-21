import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.procedure_occurrence import ProcedureOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import (
    create_build_context,
    create_patient,
    create_semantic_index,
    SemanticEntry,
)

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestProcedureOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert ProcedureOccurrenceBuilder(concepts).table_name == "procedure_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestPreviousTreatmentMainRows:
    def test_all_fields(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=4301351,
                name="surgical procedure",
                domain="procedure",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        prev.end_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == PERSON_ID
        assert row.procedure_concept_id == 4301351
        assert row.procedure_date == dt.date(2021, 3, 1)
        assert row.procedure_end_date == dt.date(2021, 3, 1)
        assert row.procedure_type_concept_id == 32817
        assert row.procedure_source_value == "Surgery"

    def test_no_procedure_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_missing_start_date_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_end_date_can_be_none(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=4301351,
                name="surgical procedure",
                domain="procedure",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].procedure_end_date is None


class TestPreviousTreatmentAdditionalRows:
    def test_additional_treatment_produces_row(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
                leaf_index=0,
                concept_id=4061650,
                name="hormone therapy",
                domain="procedure",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Other"
        prev.additional_treatment = "Hormone therapy"
        prev.start_date = dt.date(2021, 5, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].procedure_concept_id == 4061650
        assert rows[0].procedure_source_value == "Hormone therapy"

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Other"
        prev.additional_treatment = "Something unmapped"
        prev.start_date = dt.date(2021, 5, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_both_fields_produce_separate_rows(self, static_index, structural_index):
        """When both treatment and additional_treatment map to Procedure, emit one row each."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=4301351,
                name="surgical procedure",
                domain="procedure",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
                leaf_index=0,
                concept_id=4061650,
                name="hormone therapy",
                domain="procedure",
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.additional_treatment = "Hormone therapy"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2
        assert rows[0].procedure_occurrence_id != rows[1].procedure_occurrence_id
        source_values = {r.procedure_source_value for r in rows}
        assert source_values == {"Surgery", "Hormone therapy"}


class TestMedicalHistoryRows:
    def test_past_surgery_produces_row(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=4194253,
                name="operation on breast",
                domain="procedure",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "ca mamma, opr"
        mh.start_date = dt.date(2019, 6, 1)
        mh.end_date = dt.date(2019, 6, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        row = rows[0]
        assert row.procedure_concept_id == 4194253
        assert row.procedure_date == dt.date(2019, 6, 1)
        assert row.procedure_end_date == dt.date(2019, 6, 1)
        assert row.procedure_source_value == "ca mamma, opr"

    def test_missing_start_date_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "ca mamma, opr"
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_no_procedure_match_skips(self, static_index, structural_index):
        """Medical history that maps to Condition (not Procedure) produces no row here."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []


class TestCombinedSources:
    def test_all_sources_combined(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=4301351,
                name="surgical procedure",
                domain="procedure",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=4194253,
                name="operation on breast",
                domain="procedure",
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)

        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        mh = MedicalHistory(patient_id=PID)
        mh.term = "ca mamma, opr"
        mh.start_date = dt.date(2019, 6, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 2
        ids = [r.procedure_occurrence_id for r in rows]
        assert len(ids) == len(set(ids)), "All procedure_occurrence_ids must be unique"

    def test_row_ids_are_deterministic(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=4301351,
                name="surgical procedure",
                domain="procedure",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Surgery"
        prev.start_date = dt.date(2021, 3, 1)
        patient.previous_treatments = [prev]

        rows_a = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        rows_b = ProcedureOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows_a[0].procedure_occurrence_id == rows_b[0].procedure_occurrence_id
