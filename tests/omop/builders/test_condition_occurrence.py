import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.core.linkage import BuildResult, SourceReference
from omop_etl.omop.models.tables import OmopTables
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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].condition_concept_id == 4001
        assert result.rows[0].condition_source_value == "Breast cancer"

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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].condition_concept_id == 4000
        assert result.rows[0].condition_source_value == "C50.9"

    def test_no_icd10_or_main_type_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_semantic_match_skips(self, static_index, structural_index):
        """CDM policy: no row emitted for unmapped condition."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].condition_start_date == dt.date(2023, 1, 1)

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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())


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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        mh = MedicalHistory(patient_id=PID)
        mh.term = "Hypertension"
        mh.start_date = dt.date(2020, 1, 1)
        mh.sequence_id = 1
        patient.medical_histories = [mh]

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].condition_end_date is None


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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        row = result.rows[0]
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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

    def test_no_match_skips(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        patient.adverse_events = [ae]

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result == BuildResult(rows=(), publications=())

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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 1
        assert result.rows[0].condition_source_value is not None
        assert len(result.rows[0].condition_source_value) == 50


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

        result = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert len(result.rows) == 3
        ids = [r.condition_occurrence_id for r in result.rows]
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

        result_a = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))
        result_b = ConditionOccurrenceBuilder(concepts).build(create_build_context(patient, PERSON_ID))

        assert result_a.rows[0].condition_occurrence_id == result_b.rows[0].condition_occurrence_id


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

    @staticmethod
    def _ae_source_ref(ae: AdverseEvent) -> SourceReference:
        return SourceReference(PID, Patient.Collections.ADVERSE_EVENTS, ae.natural_key())

    def test_publishes_link_for_ae(self, static_index, structural_index):
        semantic = create_semantic_index(self._ae_semantic(0, 437663, "fever"))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "Fever"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 42
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        result = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(result) == 1
        refs = ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._ae_source_ref(patient.adverse_events[0]))
        assert len(refs) == 1
        assert refs[0].row_id == result[0].condition_occurrence_id

    def test_no_link_when_no_semantic_match(self, static_index, structural_index):
        """AE with no semantic match emits no row and no publication."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ae = AdverseEvent(patient_id=PID)
        ae.term = "UnmappedTerm"
        ae.start_date = dt.date(2023, 3, 1)
        ae.sequence_id = 7
        patient.adverse_events = [ae]
        ctx = create_build_context(patient, PERSON_ID)

        result = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert list(result) == []
        assert ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._ae_source_ref(patient.adverse_events[0])) == ()

    def test_multi_ae_each_published_independently(self, static_index, structural_index):
        """Multiple AEs each publish under their own SourceReference (per-NK keying)."""
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

        result = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(result) == 2
        emitted_ids = {r.condition_occurrence_id for r in result}
        for ae in patient.adverse_events:
            refs = ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._ae_source_ref(ae))
            assert len(refs) == 1
            assert refs[0].row_id in emitted_ids

    def test_multi_concept_ae_publishes_all_rows(self, static_index, structural_index):
        """One AE term mapping to multiple condition concepts: all rows published
        under the same AE SourceReference for 1:N downstream expansion."""
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

        result = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert len(result) == 2
        refs = ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._ae_source_ref(patient.adverse_events[0]))
        assert len(refs) == 2
        assert {r.row_id for r in refs} == {r.condition_occurrence_id for r in result}

    def test_fk_publication_deterministic(self, static_index, structural_index):
        """Two independent builds of the same patient produce identical published_rows state."""
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

        assert ctx_a.published_rows == ctx_b.published_rows
        assert ctx_a.published_rows != {}


class TestPrimaryCancerFKPublication:
    """
    Oncology CDM guideline: cancer-modifier Measurement result (dimensions,
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

    @staticmethod
    def _tumor_source_ref(tumor: TumorType) -> SourceReference:
        return SourceReference(PID, Patient.Singletons.TUMOR_TYPE, tumor.natural_key())

    def test_publishes_primary_cancer_id_from_tumor_type(self, static_index, structural_index):
        semantic = create_semantic_index(self._tumor_semantic(4000))
        concepts = ConceptLookupService(static_index, structural_index, semantic)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C50.9"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor
        ctx = create_build_context(patient, PERSON_ID)

        result = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        tumor_row = next(r for r in result if r.condition_concept_id == 4000)
        refs = ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._tumor_source_ref(tumor))
        assert len(refs) == 1
        assert refs[0].row_id == tumor_row.condition_occurrence_id

    def test_no_primary_cancer_id_when_tumor_type_absent(self, static_index, structural_index):
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        ctx = create_build_context(patient, PERSON_ID)

        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert ctx.published_rows == {}

    def test_no_primary_cancer_id_when_tumor_unmapped(self, static_index, structural_index):
        """Tumor type present but no semantic match: no row and no publication."""
        concepts = ConceptLookupService(static_index, structural_index)
        patient = create_patient(PID, TRIAL)
        tumor = TumorType(patient_id=PID)
        tumor.icd10_code = "C99.99"
        tumor.date = dt.date(2022, 6, 1)
        patient.tumor_type = tumor
        ctx = create_build_context(patient, PERSON_ID)

        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx)

        assert ctx.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._tumor_source_ref(tumor)) == ()

    def test_multi_concept_tumor_publishes_all_rows(self, static_index, structural_index):
        """Two semantic matches for the tumor: both rows published under the tumor SourceReference."""
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

        result_a = ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_a)
        ConditionOccurrenceBuilder(concepts).build_and_populate(ctx_b)

        assert len(result_a) == 2
        refs_a = ctx_a.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._tumor_source_ref(tumor))
        refs_b = ctx_b.resolve_rows(OmopTables.CONDITION_OCCURRENCE, self._tumor_source_ref(tumor))
        assert len(refs_a) == 2
        assert {r.row_id for r in refs_a} == {r.condition_occurrence_id for r in result_a}
        # determinism across runs
        assert refs_a == refs_b
