from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class DrugExposureBuilder(OmopBuilder[DrugExposureRow]):
    """Builds drug_exposure rows from treatment cycles, previous treatments, and concomitant medications."""

    table_name: ClassVar[str] = "drug_exposure"

    def build(self, patient: Patient, person_id: int) -> list[DrugExposureRow]:
        rows: list[DrugExposureRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        drug_type_concept_id = ecrf.concept_id if ecrf else 0

        for idx, cycle in enumerate(patient.treatment_cycles):
            row = self._build_treatment_cycle_row(patient, person_id, cycle, idx, drug_type_concept_id)
            if row is not None:
                rows.append(row)

        for idx, prev in enumerate(patient.previous_treatments):
            if prev.start_date is None:
                log.warning("Skipping previous treatment %d for %s: missing start_date", idx, patient.patient_id)
                continue
            if prev.treatment:
                row = self._build_previous_treatment_main_row(patient, person_id, prev, idx, drug_type_concept_id)
                if row is not None:
                    rows.append(row)
            if prev.additional_treatment:
                row = self._build_previous_treatment_additional_row(patient, person_id, prev, idx, drug_type_concept_id)
                if row is not None:
                    rows.append(row)

        for idx, concom in enumerate(patient.concomitant_medications):
            row = self._build_concomitant_medication_row(patient, person_id, concom, idx, drug_type_concept_id)
            if row is not None:
                rows.append(row)

        return rows

    def _build_treatment_cycle_row(
        self,
        patient: Patient,
        person_id: int,
        cycle: TreatmentCycleComponent,
        index: int,
        drug_type_concept_id: int,
    ) -> DrugExposureRow | None:
        if cycle.start_date is None:
            log.warning("Skipping treatment cycle %d for %s: missing start_date", index, patient.patient_id)
            return None

        # ingredient_name lookup for parseable names (including combination components),
        # source_treatment_name fallback for plain unparseable names only
        if cycle.ingredient_name:
            mapped = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                index,
                domains={OmopDomain.DRUG},
            )
        else:
            mapped = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                index,
                domains={OmopDomain.DRUG},
            )
        drug_concept_id = int(mapped.concept_id) if mapped else 0

        # dose fields depend on cycle type (IV vs oral)
        quantity: float | None = None
        dose_unit: str | None = None
        route_concept_id: str | None = None
        route_source: str | None = cycle.cycle_type

        if cycle.cycle_type and cycle.cycle_type == "iv":
            quantity = cycle.iv_dose_prescribed
            dose_unit = cycle.iv_dose_prescribed_unit
            iv_route = self.concepts.lookup_structural("iv", domains={"Route"})
            route_concept_id = iv_route.concept_id if iv_route else None
        elif cycle.cycle_type and cycle.cycle_type == "oral":
            quantity = cycle.oral_dose_prescribed_per_day
            dose_unit = cycle.oral_dose_unit
            oral_route = self.concepts.lookup_structural("oral", domains={"Route"})
            route_concept_id = oral_route.concept_id if oral_route else None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.TREATMENT_CYCLES,
            str(cycle.cycle_number),
            str(cycle.treatment_number),
            str(cycle.component_index),
        )

        return DrugExposureRow(
            drug_exposure_id=row_id,
            person_id=person_id,
            drug_concept_id=drug_concept_id,
            drug_exposure_start_date=cycle.start_date,
            drug_exposure_end_date=cycle.end_date or cycle.start_date,
            drug_type_concept_id=drug_type_concept_id,
            quantity=quantity,
            route_source_value=route_source,
            dose_unit_source_value=dose_unit,
            drug_source_value=cycle.source_treatment_name,
            route_concept_id=route_concept_id,
        )

    def _build_previous_treatment_main_row(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatments,
        index: int,
        drug_type_concept_id: int,
    ) -> DrugExposureRow | None:
        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.TREATMENT),
            index,
            domains={OmopDomain.DRUG},
        )
        if not mapped:
            return None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.PREVIOUS_TREATMENTS,
            str(prev.treatment_sequence_number),
            PreviousTreatments.Fields.TREATMENT,
        )

        return DrugExposureRow(
            drug_exposure_id=row_id,
            person_id=person_id,
            drug_concept_id=int(mapped.concept_id),
            drug_exposure_start_date=prev.start_date,
            drug_exposure_end_date=prev.end_date or prev.start_date,
            drug_type_concept_id=drug_type_concept_id,
            drug_source_value=prev.treatment,
        )

    def _build_previous_treatment_additional_row(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatments,
        index: int,
        drug_type_concept_id: int,
    ) -> DrugExposureRow | None:
        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatments.Fields.ADDITIONAL_TREATMENT),
            index,
            domains={OmopDomain.DRUG},
        )
        if not mapped:
            return None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.PREVIOUS_TREATMENTS,
            str(prev.treatment_sequence_number),
            PreviousTreatments.Fields.ADDITIONAL_TREATMENT,
        )

        return DrugExposureRow(
            drug_exposure_id=row_id,
            person_id=person_id,
            drug_concept_id=int(mapped.concept_id),
            drug_exposure_start_date=prev.start_date,
            drug_exposure_end_date=prev.end_date or prev.start_date,
            drug_type_concept_id=drug_type_concept_id,
            drug_source_value=prev.additional_treatment,
        )

    def _build_concomitant_medication_row(
        self,
        patient: Patient,
        person_id: int,
        concom: ConcomitantMedication,
        index: int,
        drug_type_concept_id: int,
    ) -> DrugExposureRow | None:
        if concom.start_date is None:
            log.warning("Skipping concomitant medication %d for %s: missing start_date", index, patient.patient_id)
            return None

        mapped = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
            index,
            domains={OmopDomain.DRUG},
        )
        drug_concept_id = int(mapped.concept_id) if mapped else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.CONCOMITANT_MEDICATIONS,
            str(concom.sequence_id),
        )

        return DrugExposureRow(
            drug_exposure_id=row_id,
            person_id=person_id,
            drug_concept_id=drug_concept_id,
            drug_exposure_start_date=concom.start_date,
            drug_exposure_end_date=concom.end_date or concom.start_date,
            drug_type_concept_id=drug_type_concept_id,
            drug_source_value=concom.medication_name,
        )
