from typing import ClassVar, Optional

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.study_drugs import StudyDrugs
from omop_etl.harmonization.models.domain.previous_treatments import PreviousTreatments
from omop_etl.harmonization.models.domain.concomitant_medication import ConcomitantMedication
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import DrugExposureRow
from logging import getLogger


log = getLogger(__name__)


class DrugExposureBuilder(OmopBuilder[DrugExposureRow]):
    """Builds Drug Exposure table rows from patient study drugs, previous treatments, concomitant medication."""

    table_name: ClassVar[str] = "drug_exposure"

    def build(self, patient: Patient, person_id: int) -> list[DrugExposureRow]:
        print(
            f"[DEBUG] DrugExposureBuilder.build called for {patient.patient_id}, study_drugs={patient.study_drugs}, prev_count={len(patient.previous_treatments) if patient.previous_treatments else 0}, concom_count={len(patient.concomitant_medications) if patient.concomitant_medications else 0}"
        )  # debug # To remove after testing
        rows = []

        # loop over patients
        # loop over treatment fcyles
        #

        # Process Study Drugs (primary = drug 1, secondary = drug 2)
        if patient.study_drugs:
            study_rows = self._build_study_drug_rows(patient, person_id, patient.study_drugs)
            print(f"[DEBUG] _build_study_drug_rows returned {len(study_rows)} rows: {[r.drug_source_value for r in study_rows]}")
            rows.extend(study_rows)

        # Process Previous Treatments
        if patient.previous_treatments:
            for idx, prev_treatment in enumerate(patient.previous_treatments):
                prev_rows = self._build_previous_treatment_row(patient=patient, person_id=person_id, prev_treatment=prev_treatment, index=idx)
                if prev_rows:
                    print(f"[DEBUG] _build_previous_treatment_row for {prev_treatment.treatment} -> {len(prev_rows)} rows")
                    rows.extend(prev_rows)

        # Process Concomitant Medications
        if patient.concomitant_medications:
            for idx, concomitant in enumerate(patient.concomitant_medications):
                con_rows = self._build_concomitant_medication_row(patient, person_id, concomitant, idx)
                if con_rows:
                    print(f"[DEBUG] _build_concomitant_medication_row for {concomitant.medication_name} -> {len(con_rows)} rows")
                    rows.extend(con_rows)

        return rows

    def _build_study_drug_rows(self, patient: Patient, person_id: int, study_drugs: StudyDrugs) -> list[DrugExposureRow]:
        """Build rows for study drugs (primary and secondary)."""
        rows: list[DrugExposureRow] = []
        # OMOP requires a start date for drug exposure; if missing we can't build rows
        if patient.treatment_start_date is None:
            log.warning(f"Missing treatment_start_date for {patient.patient_id}; skipping study drug rows")
            return []

        # Primary treatment drug (Study Drug 1)
        if study_drugs.primary_treatment_drug:
            exposure_id = self.generate_row_id(patient.patient_id)
            mapped_concepts = self._concepts.lookup_semantic(patient.patient_id, ("study_drugs", "primary_treatment_drug"), None)
            concept_id = mapped_concepts[0].concept_id if mapped_concepts else 0
            drug_source_value = f"Study Drug 1: {study_drugs.primary_treatment_drug}"

            row = DrugExposureRow(
                drug_exposure_id=exposure_id,
                person_id=person_id,
                drug_concept_id=concept_id,
                drug_exposure_start_date=patient.treatment_start_date,
                drug_exposure_end_date=patient.end_of_treatment_date or patient.treatment_start_date,
                drug_type_concept_id=32809,  # Case Report Form
                drug_exposure_start_datetime=None,
                drug_exposure_end_datetime=None,
                verbatim_end_date=None,
                stop_reason=None,
                refills=None,
                quantity=None,
                days_supply=None,
                sig=None,
                route_concept_id=None,
                lot_number=None,
                provider_id=None,
                visit_ocurrence_id=None,
                visit_detail_id=None,
                drug_source_value=study_drugs.primary_treatment_drug_code,
                drug_source_concept_id=study_drugs.primary_treatment_drug_code,
                route_source_value=None,
                dose_unit_source_value=None,
            )
            rows.append(row)

        # Secondary treatment drug (Study Drug 2)
        if study_drugs.secondary_treatment_drug:
            exposure_id = self.generate_row_id(patient.patient_id)
            mapped_concepts = self._concepts.lookup_semantic(patient.patient_id, ("study_drugs", "secondary_treatment_drug"), None)
            concept_id = mapped_concepts[0].concept_id if mapped_concepts else 0
            drug_source_value = f"Study Drug 2: {study_drugs.secondary_treatment_drug}"

            row = DrugExposureRow(
                drug_exposure_id=exposure_id,
                person_id=person_id,
                drug_concept_id=concept_id,
                drug_exposure_start_date=patient.treatment_start_date,
                drug_exposure_end_date=patient.end_of_treatment_date or patient.treatment_start_date,
                drug_type_concept_id=32809,  # Case report form
                drug_exposure_start_datetime=None,
                drug_exposure_end_datetime=None,
                verbatim_end_date=None,
                stop_reason=None,
                refills=None,
                quantity=None,
                days_supply=None,
                sig=None,
                route_concept_id=None,
                lot_number=None,
                provider_id=None,
                visit_ocurrence_id=None,
                visit_detail_id=None,
                drug_source_value=study_drugs.secondary_treatment_drug_code,
                drug_source_concept_id=study_drugs.secondary_treatment_drug_code,
                route_source_value=None,
                dose_unit_source_value=None,
            )
            rows.append(row)

        return rows

    def _build_previous_treatment_row(
        self, patient: Patient, person_id: int, prev_treatment: PreviousTreatments, index: int, route: Optional[str] = None
    ) -> list[DrugExposureRow]:
        """Build a row for a previous treatment."""
        # need a start date
        if prev_treatment.start_date is None:
            log.warning(f"Missing start_date for previous treatment {patient.patient_id}; skipping")
            return []

        exposure_id = self.generate_row_id(patient.patient_id)
        mapped_concepts = self._concepts.lookup_semantic(patient.patient_id, ("previous_treatments", "treatment"), index)
        concept_id = mapped_concepts[0].concept_id if mapped_concepts else 0

        row = DrugExposureRow(
            drug_exposure_id=exposure_id,
            person_id=person_id,
            drug_concept_id=concept_id,
            drug_exposure_start_date=prev_treatment.start_date,
            drug_exposure_end_date=prev_treatment.end_date or prev_treatment.start_date,
            drug_type_concept_id=32809,  # Case Report Form
            drug_exposure_start_datetime=None,
            drug_exposure_end_datetime=None,
            verbatim_end_date=None,
            stop_reason=None,
            refills=None,
            quantity=None,
            days_supply=None,
            sig=None,
            route_concept_id=None,
            lot_number=None,
            provider_id=None,
            visit_ocurrence_id=None,
            visit_detail_id=None,
            drug_source_value=prev_treatment.treatment_code,
            drug_source_concept_id=None,
            route_source_value=None,
            dose_unit_source_value=None,
        )
        return [row]

    def _build_concomitant_medication_row(self, patient: Patient, person_id: int, concomitant: ConcomitantMedication, index: int) -> list[DrugExposureRow]:
        """Build a row for a concomitant medication."""
        if concomitant.start_date is None:
            log.warning(f"Missing start_date for concomitant medication {patient.patient_id}; skipping")
            return []

        exposure_id = self.generate_row_id(patient.patient_id)
        mapped_concepts = self._concepts.lookup_semantic(patient.patient_id, ("concomitant_medications", "medication_name"), index)
        concept_id = mapped_concepts[0].concept_id if mapped_concepts else 0

        row = DrugExposureRow(
            drug_exposure_id=exposure_id,
            person_id=person_id,
            drug_concept_id=concept_id,
            drug_exposure_start_date=concomitant.start_date,
            drug_exposure_end_date=concomitant.end_date or concomitant.start_date,
            drug_type_concept_id=32809,  # Case Report Form
            drug_exposure_start_datetime=None,
            drug_exposure_end_datetime=None,
            verbatim_end_date=None,
            stop_reason=None,
            refills=None,
            quantity=None,
            days_supply=None,
            sig=None,
            route_concept_id=None,
            lot_number=None,
            provider_id=None,
            visit_ocurrence_id=None,
            visit_detail_id=None,
            drug_source_value=None,
            drug_source_concept_id=None,
            route_source_value=None,
            dose_unit_source_value=None,
        )
        return [row]
