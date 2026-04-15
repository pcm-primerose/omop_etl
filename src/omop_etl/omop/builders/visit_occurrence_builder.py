from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import VisitOccurrenceRow

log = getLogger(__name__)


class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    """Builds visit_occurrence rows from tumor assessment baseline and tumor assessments."""

    table_name: ClassVar[str] = "visit_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
        rows: list[VisitOccurrenceRow] = []
        outpatient = self.concepts.lookup_structural("outpatient_visit")
        ecrf = self.concepts.lookup_structural("ecrf")
        visit_concept_id = outpatient.concept_id if outpatient else 0
        visit_type_concept_id = ecrf.concept_id if ecrf else 0

        # baseline singleton
        if patient.tumor_assessment_baseline is not None:
            row = self._build_baseline_row(patient, person_id, patient.tumor_assessment_baseline, visit_concept_id, visit_type_concept_id)
            if row is not None:
                rows.append(row)

        # assessment collection: dedup by event_id (one visit per physical encounter)
        seen_event_ids: set[str] = set()
        for idx, assessment in enumerate(patient.tumor_assessments):
            if assessment.event_id is not None and assessment.event_id in seen_event_ids:
                continue
            row = self._build_assessment_row(patient, person_id, assessment, visit_concept_id, visit_type_concept_id)
            if row is not None:
                if assessment.event_id is not None:
                    seen_event_ids.add(assessment.event_id)
                rows.append(row)

        return rows

    def _build_baseline_row(
        self,
        patient: Patient,
        person_id: int,
        baseline: TumorAssessmentBaseline,
        visit_concept_id: int,
        visit_type_concept_id: int,
    ) -> VisitOccurrenceRow | None:
        date = baseline.assessment_date or baseline.target_lesion_measurement_date or baseline.off_target_lesion_measurement_date

        if date is None:
            log.warning("Skipping baseline visit for %s: no usable date", patient.patient_id)
            return None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.TUMOR_ASSESSMENT_BASELINE,
            str(date),
        )

        return VisitOccurrenceRow(
            visit_occurrence_id=row_id,
            person_id=person_id,
            visit_concept_id=visit_concept_id,
            visit_start_date=date,
            visit_end_date=date,
            visit_type_concept_id=visit_type_concept_id,
            visit_source_value=baseline.assessment_type,
        )

    def _build_assessment_row(
        self,
        patient: Patient,
        person_id: int,
        assessment: TumorAssessment,
        visit_concept_id: int,
        visit_type_concept_id: int,
    ) -> VisitOccurrenceRow | None:
        if assessment.date is None:
            return None

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.TUMOR_ASSESSMENTS,
            str(assessment.event_id),
            str(assessment.date),
        )

        return VisitOccurrenceRow(
            visit_occurrence_id=row_id,
            person_id=person_id,
            visit_concept_id=visit_concept_id,
            visit_start_date=assessment.date,
            visit_end_date=assessment.date,
            visit_type_concept_id=visit_type_concept_id,
            visit_source_value=assessment.assessment_type,
        )
