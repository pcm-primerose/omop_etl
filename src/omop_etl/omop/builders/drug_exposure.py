from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.treatment_cycle_component import TreatmentCycleComponent
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatment
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.omop.builders.base import OmopBuilder, BuildContext
from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class DrugExposureBuilder(OmopBuilder[DrugExposureRow]):
    """Builds drug_exposure rows from treatment cycles, previous treatments, and concomitant medications.

    CDM policy: when a drug source value cannot be translated into a standard Drug
    concept, a row is stored with DRUG_CONCEPT_ID of 0 and the source value preserved
    in DRUG_SOURCE_VALUE (CDM 5.4 drug_exposure ETL conventions).

    Note: previous treatment fields map to multiple domains, if no semantic result,
    no row is emitted (breaking the cdm spec). This is because we can't know what domain
    unmapped terms would land in a-priori. See tests for more details.
    """

    table_name: ClassVar[str] = "drug_exposure"

    def build(self, ctx: BuildContext) -> list[DrugExposureRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[DrugExposureRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"type concept"})
        drug_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        for idx, cycle in enumerate(patient.treatment_cycles):
            rows.extend(self._build_treatment_cycle_rows(patient, person_id, cycle, idx, drug_type_concept_id))

        for idx, prev in enumerate(patient.previous_treatments):
            if prev.start_date is None:
                log.warning("Skipping previous treatment %d for %s: missing start_date", idx, patient.patient_id)
                continue
            if prev.treatment:
                rows.extend(self._build_previous_treatment_main_rows(patient, person_id, prev, idx, drug_type_concept_id))
            if prev.additional_treatment:
                rows.extend(self._build_previous_treatment_additional_rows(patient, person_id, prev, idx, drug_type_concept_id))

        for idx, concom in enumerate(patient.concomitant_medications):
            rows.extend(self._build_concomitant_medication_rows(patient, person_id, concom, idx, drug_type_concept_id))

        return rows

    def _build_treatment_cycle_rows(
        self,
        patient: Patient,
        person_id: int,
        cycle: TreatmentCycleComponent,
        index: int,
        drug_type_concept_id: int,
    ) -> list[DrugExposureRow]:
        start_date = cycle.start_date
        end_date = cycle.end_date
        if start_date is None:
            log.warning(f"Skipping treatment cycle {index} for {patient.patient_id}: missing start_date")
            return []

        if cycle.source_treatment_name is None and cycle.ingredient_name is None:
            log.warning(f"Skipping treatment cycle {index} for {patient.patient_id}: missing source_treatment_name and ingredient_name")
            return []

        if cycle.ingredient_name:
            matches = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.INGREDIENT_NAME),
                index,
                domains={OmopDomain.DRUG},
            )
        else:
            matches = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Collections.TREATMENT_CYCLES, TreatmentCycleComponent.Fields.SOURCE_TREATMENT_NAME),
                index,
                domains={OmopDomain.DRUG},
            )

        # dose fields depend on cycle type
        quantity: float | None = None
        dose_unit: str | None = None
        route_concept_id: int | None = None
        route_source: str | None = cycle.cycle_type

        if cycle.cycle_type and cycle.cycle_type == "iv":
            quantity = cycle.iv_dose_prescribed
            dose_unit = cycle.iv_dose_prescribed_unit
            iv_route = self.concepts.lookup_structural("iv", domains={"Route"})
            route_concept_id = int(iv_route.concept_id) if iv_route else None
        elif cycle.cycle_type and cycle.cycle_type == "oral":
            quantity = cycle.oral_dose_prescribed_per_day
            dose_unit = cycle.oral_dose_unit
            oral_route = self.concepts.lookup_structural("oral", domains={"Route"})
            route_concept_id = int(oral_route.concept_id) if oral_route else None

        base_row_id_parts = (
            patient.patient_id,
            Patient.Collections.TREATMENT_CYCLES,
            str(cycle.cycle_number),
            str(cycle.treatment_number),
            str(cycle.component_index),
        )
        end_date_or_start = end_date or start_date
        drug_source_value = cycle.source_treatment_name or cycle.ingredient_name

        # CDM: emit with concept_id=0 if no mapping
        if not matches:
            return [
                DrugExposureRow(
                    drug_exposure_id=self.generate_row_id(*base_row_id_parts),
                    person_id=person_id,
                    drug_concept_id=0,
                    drug_exposure_start_date=start_date,
                    drug_exposure_end_date=end_date_or_start,
                    drug_type_concept_id=drug_type_concept_id,
                    quantity=quantity,
                    route_source_value=route_source,
                    dose_unit_source_value=dose_unit,
                    drug_source_value=drug_source_value,
                    route_concept_id=route_concept_id,
                )
            ]

        return [
            DrugExposureRow(
                drug_exposure_id=self.generate_row_id(*base_row_id_parts, str(concept.concept_id)),
                person_id=person_id,
                drug_concept_id=int(concept.concept_id),
                drug_exposure_start_date=start_date,
                drug_exposure_end_date=end_date_or_start,
                drug_type_concept_id=drug_type_concept_id,
                quantity=quantity,
                route_source_value=route_source,
                dose_unit_source_value=dose_unit,
                drug_source_value=drug_source_value,
                route_concept_id=route_concept_id,
            )
            for concept in matches
        ]

    def _build_previous_treatment_main_rows(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatment,
        index: int,
        drug_type_concept_id: int,
    ) -> list[DrugExposureRow]:
        start_date = prev.start_date
        if start_date is None:
            return []

        end_date = prev.end_date or start_date
        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.TREATMENT),
            index,
            domains={OmopDomain.DRUG},
        )
        if not matches:
            return []

        return [
            DrugExposureRow(
                drug_exposure_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.PREVIOUS_TREATMENTS,
                    str(prev.treatment_sequence_number),
                    PreviousTreatment.Fields.TREATMENT,
                    str(concept.concept_id),
                ),
                person_id=person_id,
                drug_concept_id=int(concept.concept_id),
                drug_exposure_start_date=start_date,
                drug_exposure_end_date=end_date,
                drug_type_concept_id=drug_type_concept_id,
                drug_source_value=prev.treatment,
            )
            for concept in matches
        ]

    def _build_previous_treatment_additional_rows(
        self,
        patient: Patient,
        person_id: int,
        prev: PreviousTreatment,
        index: int,
        drug_type_concept_id: int,
    ) -> list[DrugExposureRow]:
        start_date = prev.start_date
        if start_date is None:
            return []

        end_date = prev.end_date or start_date
        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.PREVIOUS_TREATMENTS, PreviousTreatment.Fields.ADDITIONAL_TREATMENT),
            index,
            domains={OmopDomain.DRUG},
        )
        if not matches:
            return []

        return [
            DrugExposureRow(
                drug_exposure_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.PREVIOUS_TREATMENTS,
                    str(prev.treatment_sequence_number),
                    PreviousTreatment.Fields.ADDITIONAL_TREATMENT,
                    str(concept.concept_id),
                ),
                person_id=person_id,
                drug_concept_id=int(concept.concept_id),
                drug_exposure_start_date=start_date,
                drug_exposure_end_date=end_date,
                drug_type_concept_id=drug_type_concept_id,
                drug_source_value=prev.additional_treatment,
            )
            for concept in matches
        ]

    def _build_concomitant_medication_rows(
        self,
        patient: Patient,
        person_id: int,
        concom: ConcomitantMedication,
        index: int,
        drug_type_concept_id: int,
    ) -> list[DrugExposureRow]:
        start_date = concom.start_date
        end_date = concom.end_date
        if start_date is None:
            log.warning(f"Skipping concomitant medication {index} for {patient.patient_id}: missing start_date")
            return []

        if concom.medication_name is None:
            log.warning(f"Skipping concomitant medication {index} for {patient.patient_id}: missing medication_name")
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.CONCOMITANT_MEDICATIONS, ConcomitantMedication.Fields.MEDICATION_NAME),
            index,
            domains={OmopDomain.DRUG},
        )
        end_date_or_start = end_date or start_date

        # CDM: emit with concept_id=0 if no mapping
        if not matches:
            return [
                DrugExposureRow(
                    drug_exposure_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.CONCOMITANT_MEDICATIONS,
                        str(concom.sequence_id),
                    ),
                    person_id=person_id,
                    drug_concept_id=0,
                    drug_exposure_start_date=start_date,
                    drug_exposure_end_date=end_date_or_start,
                    drug_type_concept_id=drug_type_concept_id,
                    drug_source_value=concom.medication_name,
                )
            ]

        return [
            DrugExposureRow(
                drug_exposure_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.CONCOMITANT_MEDICATIONS,
                    str(concom.sequence_id),
                    str(concept.concept_id),
                ),
                person_id=person_id,
                drug_concept_id=int(concept.concept_id),
                drug_exposure_start_date=start_date,
                drug_exposure_end_date=end_date_or_start,
                drug_type_concept_id=drug_type_concept_id,
                drug_source_value=concom.medication_name,
            )
            for concept in matches
        ]
