import datetime as dt

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.drug_exposure import DrugExposureBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from tests.omop.conftest import create_patient, create_semantic_index, SemanticEntry

PID = "p1"
TRIAL = "test"
PERSON_ID = sha1_bigint("person", PID)


class TestDrugExposureBuilder:
    def test_table_name(self, static_index, structural_index):
        assert DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).table_name == "drug_exposure"

    def test_empty_patient_returns_empty(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)
        assert rows == []


class TestTreatmentCycleRows:
    def test_iv_cycle_produces_row_with_concept_id_zero(self, static_index, structural_index):
        """No semantic mapping, fallback concept_id=0 and row still emitted."""
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Trametinib"
        cycle.cycle_type = "iv"
        cycle.start_date = dt.date(2023, 1, 1)
        cycle.end_date = dt.date(2023, 1, 15)
        cycle.iv_dose_prescribed = 200.0
        cycle.iv_dose_prescribed_unit = "mg"
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.drug_concept_id == 0
        assert row.drug_exposure_start_date == dt.date(2023, 1, 1)
        assert row.drug_exposure_end_date == dt.date(2023, 1, 15)
        assert row.drug_source_value == "Trametinib"
        assert row.route_source_value == "iv"
        assert row.route_concept_id == 4171047
        assert row.quantity == 200.0
        assert row.dose_unit_source_value == "mg"
        assert row.drug_type_concept_id == 32817

    def test_oral_cycle_produces_row(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Dabrafenib"
        cycle.cycle_type = "oral"
        cycle.start_date = dt.date(2023, 2, 1)
        cycle.oral_dose_prescribed_per_day = 150.0
        cycle.oral_dose_unit = "mg"
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.drug_source_value == "Dabrafenib"
        assert row.route_source_value == "oral"
        assert row.route_concept_id == 4132161
        assert row.quantity == 150.0
        assert row.dose_unit_source_value == "mg"

    def test_end_date_falls_back_to_start_date(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Drug A"
        cycle.start_date = dt.date(2023, 3, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_exposure_end_date == dt.date(2023, 3, 1)

    def test_no_cycle_type_omits_route_and_dose(self, static_index, structural_index):
        """When cycle_type is None, route and dose fields are not populated."""
        patient = create_patient(PID, TRIAL)
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Drug A"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.route_concept_id is None
        assert row.route_source_value is None
        assert row.quantity is None
        assert row.dose_unit_source_value is None

    def test_missing_start_date_skips(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Drug A"
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert rows == []

    def test_multiple_cycles_produce_multiple_rows(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        c1 = TreatmentCycleComponent(patient_id=PID)
        c1.source_treatment_name = "Drug A"
        c1.treatment_number = 1
        c1.cycle_number = 1
        c1.start_date = dt.date(2023, 1, 1)
        c2 = TreatmentCycleComponent(patient_id=PID)
        c2.source_treatment_name = "Drug A"
        c2.treatment_number = 1
        c2.cycle_number = 2
        c2.start_date = dt.date(2023, 2, 1)
        patient.treatment_cycles = [c1, c2]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 2
        assert rows[0].drug_exposure_id != rows[1].drug_exposure_id

    def test_combination_components_use_ingredient_not_source(self, static_index, structural_index):
        """Combination drug (Phesgo = Pertuzumab + Trastuzumab) is split into two
        TreatmentCycleComponent instances with the same source_treatment_name but
        different ingredient_name. Each should map through its ingredient path."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                leaf_index=0,
                concept_id=6001,
                name="pertuzumab",
                domain="drug",
                vocab="rxnorm",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                leaf_index=1,
                concept_id=6002,
                name="trastuzumab",
                domain="drug",
                vocab="rxnorm",
            ),
        )
        patient = create_patient(PID, TRIAL)
        c1 = TreatmentCycleComponent(patient_id=PID)
        c1.source_treatment_name = "Phesgo (Pertuzumab and Trastuzumab)"
        c1.ingredient_name = "Pertuzumab"
        c1.component_index = 0
        c1.start_date = dt.date(2023, 1, 1)
        c2 = TreatmentCycleComponent(patient_id=PID)
        c2.source_treatment_name = "Phesgo (Pertuzumab and Trastuzumab)"
        c2.ingredient_name = "Trastuzumab"
        c2.component_index = 1
        c2.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [c1, c2]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 2
        concept_ids = {r.drug_concept_id for r in rows}
        assert concept_ids == {6001, 6002}
        assert rows[0].drug_exposure_id != rows[1].drug_exposure_id

    def test_with_semantic_match_uses_mapped_concept(self, static_index, structural_index):
        """When a semantic mapping exists, the row uses the mapped concept_id."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                leaf_index=0,
                concept_id=1234,
                name="trametinib",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Trametinib"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_concept_id == 1234


class TestSemanticLookupStrategy:
    def test_uses_ingredient_name_when_available(self, static_index, structural_index):
        """When ingredient_name is populated, builder queries ingredient path instead of source_treatment_name"""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                leaf_index=0,
                concept_id=5001,
                name="atezolizumab",
                domain="drug",
                vocab="rxnorm",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                leaf_index=0,
                concept_id=7777,
                name="atezolizumab",
                domain="drug",
                vocab="rxnorm",
            ),
        )
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Atezolizumab"
        cycle.ingredient_name = "Atezolizumab"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_concept_id == 5001

    def test_falls_back_to_source_name_when_no_ingredient(self, static_index, structural_index):
        """When ingredient_name is None, builder queries source_treatment_name path."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                leaf_index=0,
                concept_id=5002,
                name="fulvestrant",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Fulvestrant"
        cycle.ingredient_name = None
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_concept_id == 5002

    def test_missing_mappable_data_returns_empty_list(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                leaf_index=0,
                concept_id=5003,
                name="fulvestrant",
                domain="drug",
                vocab="rxnorm",
            )
        )

        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = None
        cycle.ingredient_name = None
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 0
        assert rows == []

    def test_missing_source_treatment_name_maps_ingredient(self, static_index, structural_index):
        """
        When indgredient_name is populated and source_treatment_name is None, use ingredient_name for mapping,
        this should never happen but should still work
        """
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                leaf_index=0,
                concept_id=5001,
                name="ingredient",
                domain="drug",
                vocab="rxnorm",
            ),
        )

        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = None
        cycle.ingredient_name = "ingredient"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_concept_id == 5001


class TestPreviousTreatmentRows:
    """Previous treatments try both treatment and additional_treatment fields
    independently with a Drug domain filter. Each match produces its own row."""

    def test_additional_treatment_maps_to_drug(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
                leaf_index=0,
                concept_id=1524674,
                name="zoledronic acid",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Chemotherapy"
        prev.additional_treatment = "Zometa"
        prev.start_date = dt.date(2022, 6, 1)
        prev.end_date = dt.date(2022, 12, 1)
        patient.previous_treatments = [prev]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.drug_concept_id == 1524674
        assert row.drug_source_value == "Zometa"
        assert row.drug_exposure_start_date == dt.date(2022, 6, 1)
        assert row.drug_exposure_end_date == dt.date(2022, 12, 1)

    def test_main_treatment_maps_to_drug(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=1304850,
                name="letrozole",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Letrozole"
        prev.additional_treatment = "Additional"
        prev.start_date = dt.date(2022, 1, 1)
        patient.previous_treatments = [prev]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_source_value == "Letrozole"

    def test_both_fields_map_to_drug_emits_two_rows(self, static_index, structural_index):
        """When both main and additional treatment map to drug, emit one row each."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
                leaf_index=0,
                concept_id=100,
                name="drug a",
                domain="drug",
                vocab="rxnorm",
            ),
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
                leaf_index=0,
                concept_id=200,
                name="drug b",
                domain="drug",
                vocab="rxnorm",
            ),
        )
        patient = create_patient(PID, "test")
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Drug A"
        prev.additional_treatment = "Drug B"
        prev.start_date = dt.date(2022, 1, 1)
        patient.previous_treatments = [prev]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 2
        assert rows[0].drug_exposure_id != rows[1].drug_exposure_id
        source_values = {r.drug_source_value for r in rows}
        assert source_values == {"Drug A", "Drug B"}

    def test_no_drug_mapping_produces_no_row(self, static_index, structural_index):
        """Treatment fields without drug domain mappings produce no rows."""
        patient = create_patient(PID, "test")
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "paracetamol"
        prev.start_date = dt.date(2022, 6, 1)
        patient.previous_treatments = [prev]

        # note: previous treatment fields map to multiple domains,
        # currently the builders won't know if there are off-domain target matches,
        # so a drug concept missing a mapping is indistinguishable from data mapping
        # to another domain (e.g. "chemetherapy" in previous treatments).
        # so, can't default to emit "0-rows" for previous treatments, unless
        # adding "off-target-matches" as a field to the semantic lookup return type,
        # but this still doesn't descriminate between no match inside target domain
        # versus no match outside target domain, need classified or default domain in configs for this.

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert rows == []

    def test_missing_start_date_skips(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Chemotherapy"
        prev.additional_treatment = "Zometa"
        patient.previous_treatments = [prev]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert rows == []


class TestConcomitantMedicationRows:
    def test_produces_row_with_concept_id_zero(self, static_index, structural_index):
        """CDM: emit with drug_concept_id=0 when no semantic mapping exists."""
        patient = create_patient(PID, "test")
        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Paracetamol"
        concom.start_date = dt.date(2023, 1, 10)
        concom.end_date = dt.date(2023, 1, 20)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        row = rows[0]
        assert row.drug_concept_id == 0
        assert row.drug_source_value == "Paracetamol"
        assert row.drug_exposure_start_date == dt.date(2023, 1, 10)
        assert row.drug_exposure_end_date == dt.date(2023, 1, 20)

    def test_produces_row_with_mapped_concept(self, static_index, structural_index):
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
                leaf_index=0,
                concept_id=1124957,
                name="oxycodone",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")
        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Oxynorm"
        concom.start_date = dt.date(2023, 1, 10)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_concept_id == 1124957

    def test_missing_start_date_skips(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Paracetamol"
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert rows == []

    def test_missing_medication_name_skips(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        concom = ConcomitantMedication(patient_id=PID)
        concom.start_date = dt.date(2023, 1, 1)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert rows == []

    def test_end_date_falls_back_to_start_date(self, static_index, structural_index):
        patient = create_patient(PID, TRIAL)
        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Paracetamol"
        concom.start_date = dt.date(2023, 1, 10)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index)).build(patient, PERSON_ID)

        assert len(rows) == 1
        assert rows[0].drug_exposure_start_date == dt.date(2023, 1, 10)
        assert rows[0].drug_exposure_end_date == dt.date(2023, 1, 10)

    def test_multi_ingredient_produces_multiple_rows(self, static_index, structural_index):
        """Combination drug (e.g. Calcigran Forte) returns multiple concepts
        from a single lookup, one drug_exposure row per ingredient."""
        concom_path = (Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME)
        semantic = create_semantic_index(
            SemanticEntry(patient_id=PID, field_path=concom_path, leaf_index=0, concept_id=19, name="vitamin d", domain="drug", vocab="rxnorm"),
            SemanticEntry(patient_id=PID, field_path=concom_path, leaf_index=0, concept_id=20, name="calcium carbonate", domain="drug", vocab="rxnorm"),
        )
        patient = create_patient(PID, "test")
        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Calcigran Forte"
        concom.start_date = dt.date(2023, 1, 1)
        concom.sequence_id = 1
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 2
        concept_ids = {r.drug_concept_id for r in rows}
        assert concept_ids == {19, 20}
        assert rows[0].drug_exposure_id != rows[1].drug_exposure_id
        assert all(r.drug_source_value == "Calcigran Forte" for r in rows)


class TestIdUniqueness:
    def test_ids_unique_across_sources(self, static_index, structural_index):
        """Rows from treatment_cycles, previous_treatments, and concomitant_medications
        all get unique drug_exposure_ids."""
        semantic = create_semantic_index(
            SemanticEntry(
                patient_id=PID,
                field_path=(Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
                leaf_index=0,
                concept_id=1524674,
                name="zoledronic acid",
                domain="drug",
                vocab="rxnorm",
            )
        )
        patient = create_patient(PID, "test")

        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Drug A"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        prev = PreviousTreatments(patient_id=PID)
        prev.treatment = "Chemotherapy"
        prev.additional_treatment = "Zometa"
        prev.start_date = dt.date(2022, 1, 1)
        patient.previous_treatments = [prev]

        concom = ConcomitantMedication(patient_id=PID)
        concom.medication_name = "Drug C"
        concom.start_date = dt.date(2023, 1, 1)
        patient.concomitant_medications = [concom]

        rows = DrugExposureBuilder(ConceptLookupService(static_index, structural_index, semantic)).build(patient, PERSON_ID)

        assert len(rows) == 3
        ids = [r.drug_exposure_id for r in rows]
        assert len(ids) == len(set(ids)), "All drug_exposure_ids must be unique"

    def test_ids_deterministic(self, static_index, structural_index):
        patient = create_patient(PID, "test")
        cycle = TreatmentCycleComponent(patient_id=PID)
        cycle.source_treatment_name = "Drug A"
        cycle.start_date = dt.date(2023, 1, 1)
        patient.treatment_cycles = [cycle]

        builder = DrugExposureBuilder(ConceptLookupService(static_index, structural_index))
        rows1 = builder.build(patient, PERSON_ID)
        rows2 = builder.build(patient, PERSON_ID)

        assert rows1[0].drug_exposure_id == rows2[0].drug_exposure_id
