import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
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


class TestConditionOccurrenceBuilder:
    def test_table_name(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        assert ConditionOccurrenceBuilder(concepts).table_name == "condition_occurrence"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].condition_concept_id == 4000
        assert rows[0].condition_source_value == "C50.9"

    def test_no_icd10_or_main_type_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_no_semantic_match_skips(self, static_index, structural_index):
        """CDM policy: no row emitted for unmapped condition."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows == []

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(rows) == 1
        assert rows[0].condition_source_value is not None
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

        rows = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

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

        rows_a = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        rows_b = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert rows_a[0].condition_occurrence_id == rows_b[0].condition_occurrence_id


class TestAdverseEventFKLinkage:
    """
    CDM 5.4 observation_event_id linkage: ConditionOccurrenceBuilder publishes
    each AE's sequence_id, condition_occurrence_id into BuildContext so
    ObservationBuilder can attribute was_serious & turned_serious_date back to
    the AE's condition row.
    """

    def _ae_semantic(self, leaf_index: int, concept_id: int, name: str) -> SemanticEntry:  # noqa
        return SemanticEntry(
            patient_id=PID,
            field_path=(Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
            leaf_index=leaf_index,
            concept_id=concept_id,
            name=name,
            domain="condition",
        )

    def test_publishes_link_when_sequence_id_set(self, static_index, structural_index):
        semantic = create_semantic_index(self._ae_semantic(0, 437663, "fever"))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 42
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 1
        assert ctx.condition_id_by_ae_sequence_id == {42: rows[0].condition_occurrence_id}

    def test_no_link_when_sequence_id_missing_but_row_still_emitted(self, static_index, structural_index, caplog):
        """AE without sequence_id: row is emitted, but produces no FK entry and warns."""
        import logging

        semantic = create_semantic_index(self._ae_semantic(0, 437663, "fever"))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        with caplog.at_level(logging.WARNING):
            rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 1, "AE row must still be emitted when sequence_id is missing"
        assert ctx.condition_id_by_ae_sequence_id == {}
        assert any("missing sequence_id" in rec.message for rec in caplog.records)

    def test_no_link_when_no_semantic_match(self, static_index, structural_index):
        """AE with sequence_id but no semantic match emits no row and no FK entry."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "UnmappedTerm"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 7
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert rows == []
        assert ctx.condition_id_by_ae_sequence_id == {}

    def test_multi_ae_each_linked_by_sequence_id(self, static_index, structural_index):
        """Multiple AEs get their own FK entry keyed by their sequence_id."""
        semantic = create_semantic_index(
            self._ae_semantic(0, 437663, "fever"),
            self._ae_semantic(1, 4329847, "nausea"),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)

        ae1 = AdverseEvent(patient_id=PID)
        ae1.term = "Fever"
        ae1.start_date = dt.date(2023, 3, 1)
        ae1.sequence_id = 1

        ae2 = AdverseEvent(patient_id=PID)
        ae2.term = "Nausea"
        ae2.start_date = dt.date(2023, 4, 1)
        ae2.sequence_id = 2

        patient.adverse_events = [ae1, ae2]
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 2
        # both sequence_ids present and pointing to existing row ids
        emitted_ids = {r.condition_occurrence_id for r in rows}
        assert set(ctx.condition_id_by_ae_sequence_id.keys()) == {1, 2}
        assert set(ctx.condition_id_by_ae_sequence_id.values()).issubset(emitted_ids)

    def test_mixed_seq_id_present_and_missing(self, static_index, structural_index):
        """One AE with sequence_id and one without: only the first is linked, both emit rows."""
        semantic = create_semantic_index(
            self._ae_semantic(0, 437663, "fever"),
            self._ae_semantic(1, 4329847, "nausea"),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)

        ae1 = AdverseEvent(patient_id=PID)
        ae1.term = "Fever"
        ae1.start_date = dt.date(2023, 3, 1)
        ae1.sequence_id = 1

        ae2 = AdverseEvent(patient_id=PID)
        ae2.term = "Nausea"
        ae2.start_date = dt.date(2023, 4, 1)

        patient.adverse_events = [ae1, ae2]
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 2
        assert set(ctx.condition_id_by_ae_sequence_id.keys()) == {1}

    def test_multi_concept_ae_links_to_first_row(self, static_index, structural_index):
        """When one AE term maps to multiple condition concepts, FK links to the first emitted row."""
        semantic = create_semantic_index(
            self._ae_semantic(0, 437663, "fever"),
            self._ae_semantic(0, 999999, "alternative fever concept"),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 99
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(rows) == 2
        # one FK entry: pointing to the first emitted row
        assert ctx.condition_id_by_ae_sequence_id == {99: rows[0].condition_occurrence_id}

    def test_fk_publication_deterministic(self, static_index, structural_index):
        """Two independent builds of the same patient produce identical FK state."""
        semantic = create_semantic_index(self._ae_semantic(0, 437663, "fever"))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 5
        patient.adverse_events = [ae]

        ctx_a = create_build_context(patient, PERSON_ID)
        ctx_b = create_build_context(patient, PERSON_ID)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_a)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_b)

        assert ctx_a.condition_id_by_ae_sequence_id == ctx_b.condition_id_by_ae_sequence_id
        assert ctx_a.condition_id_by_ae_sequence_id != {}


class TestPrimaryCancerFKPublication:
    """
    Oncology CDM guideline: cancer-modifier Measurement rows (dimensions,
    biomarkers, optional future metastasis/node/stage) should link back to the
    primary cancer's condition_occurrence_id. ConditionOccurrenceBuilder
    publishes that id from the tumor_type emission.
    """

    @staticmethod
    def _tumor_semantic(concept_id: int) -> SemanticEntry:
        return SemanticEntry(
            patient_id=PID,
            field_path=(Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
            leaf_index=None,
            concept_id=concept_id,
            name="neoplasm",
            domain="condition",
        )

    def test_publishes_primary_cancer_id_from_tumor_type(self, static_index, structural_index):
        semantic = create_semantic_index(self._tumor_semantic(4000))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor
        ctx = create_build_context(patient, PERSON_ID)

        rows = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        tumor_row = next(r for r in rows if r.condition_concept_id == 4000)
        assert ctx.condition_id_primary_cancer == tumor_row.condition_occurrence_id

    def test_no_primary_cancer_id_when_tumor_type_absent(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ctx = create_build_context(patient, PERSON_ID)

        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert ctx.condition_id_primary_cancer is None

    def test_no_primary_cancer_id_when_tumor_unmapped(self, static_index, structural_index):
        """Tumor type present but no semantic match: no row and no FK published."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C99.99"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor
        ctx = create_build_context(patient, PERSON_ID)

        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert ctx.condition_id_primary_cancer is None

    def test_multi_concept_tumor_picks_first_row_deterministically(self, static_index, structural_index):
        """Two semantic matches for the tumor: two rows, FK is first row's id."""
        semantic = create_semantic_index(
            self._tumor_semantic(4000),
            self._tumor_semantic(4001),
        )
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor
        ctx_a = create_build_context(patient, PERSON_ID)
        ctx_b = create_build_context(patient, PERSON_ID)

        rows_a = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_a)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_b)

        assert len(rows_a) == 2
        assert ctx_a.condition_id_primary_cancer == rows_a[0].condition_occurrence_id
        assert ctx_a.condition_id_primary_cancer == ctx_b.condition_id_primary_cancer
