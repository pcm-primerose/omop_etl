import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import create_patient, create_semantic_index, SemanticEntry

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestConditionOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert ConditionOccurrenceBuilder(concepts).table_name == "condition_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []


class TestTumorTypeRows:
    def test_all_fields_with_icd10(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                leaf_index=None,
                concept_id=4000,
                name="malignant neoplasm",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.person_id == PERSON_ID
        assert row.condition_concept_id == 4000
        assert row.condition_start_date == dt.date(2022, 6, 1)
        assert row.condition_type_concept_id == 32817
        assert row.condition_source_value == "C50.9"
        assert row.condition_end_date is None

    def test_falls_back_to_main_tumor_type(self, static_index, structural_index):
        """When icd10_code is None, uses main_tumor_type for lookup."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                leaf_index=None,
                concept_id=4001,
                name="breast cancer",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.main_tumor_type = "Breast cancer"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].condition_concept_id == 4001
        assert rows[0].condition_source_value == "Breast cancer"

    def test_icd10_preferred_over_main_tumor_type(self, static_index, structural_index):
        """When both exist, icd10_code is used"""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                leaf_index=None,
                concept_id=4000,
                name="icd10 concept",
                domain="condition",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                leaf_index=None,
                concept_id=4001,
                name="main type concept",
                domain="condition",
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.main_tumor_type = "Breast cancer"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].condition_concept_id == 4000
        assert rows[0].condition_source_value == "C50.9"

    def test_no_icd10_or_main_type_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_no_semantic_match_skips(self, static_index, structural_index):
        """CDM policy: no row emitted for unmapped condition."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_date_falls_back_to_treatment_start(self, static_index, structural_index):
        """When tumor.date is None, uses patient.treatment_start_date."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                leaf_index=None,
                concept_id=4000,
                name="neoplasm",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL, treatment_start_date=dt.date(2023, 1, 1))
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].condition_start_date == dt.date(2023, 1, 1)

    def test_no_usable_date_skips(self, static_index, structural_index):
        """When both tumor.date and treatment_start_date are None, skip."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                leaf_index=None,
                concept_id=4000,
                name="neoplasm",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []


class TestMedicalHistoryRows:
    def test_all_fields(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=316866,
                name="hypertension",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.end_date = dt.date(2022, 6, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.condition_concept_id == 316866
        assert row.condition_start_date == dt.date(2020, 1, 1)
        assert row.condition_end_date == dt.date(2022, 6, 1)
        assert row.condition_source_value == "Hypertension"

    def test_missing_start_date_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_ongoing_condition_has_no_end_date(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=316866,
                name="hypertension",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].condition_end_date is None


class TestAdverseEventRows:
    def test_all_fields(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                leaf_index=0,
                concept_id=437663,
                name="fever",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        ae.end_date = dt.date(2023, 3, 10)
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.condition_concept_id == 437663
        assert row.condition_start_date == dt.date(2023, 3, 1)
        assert row.condition_end_date == dt.date(2023, 3, 10)
        assert row.condition_source_value == "Fever"

    def test_missing_start_date_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows == []

    def test_long_term_is_truncated_to_50_chars(self, static_index, structural_index):
        long_term = "A" * 60
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                leaf_index=0,
                concept_id=437663,
                name="long condition",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = long_term
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert len(rows[0].condition_source_value) == 50


class TestCombinedSources:
    def test_all_sources_combined(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                leaf_index=None,
                concept_id=4000,
                name="neoplasm",
                domain="condition",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=316866,
                name="hypertension",
                domain="condition",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
                leaf_index=0,
                concept_id=437663,
                name="fever",
                domain="condition",
            ),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)

        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert len(rows) == 3
        ids = [r.condition_occurrence_id for r in rows]
        assert len(ids) == len(set(ids)), "All condition_occurrence_ids must be unique"

    def test_row_ids_are_deterministic(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
                leaf_index=0,
                concept_id=316866,
                name="hypertension",
                domain="condition",
            )
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows_a = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)
        rows_b = ConditionOccurrenceBuilder(concepts).build(patient, PERSON_ID)

        assert rows_a[0].condition_occurrence_id == rows_b[0].condition_occurrence_id
