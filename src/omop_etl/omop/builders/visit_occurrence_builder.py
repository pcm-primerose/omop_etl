from typing import ClassVar, List

from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import VisitOccurrenceRow
from omop_etl.harmonization.models import (
    Patient,
    TumorAssessmentBaseline,
)


# only thing that goes here is data from VI (visits): tumor assessments
# todo: ensure visit and ecrf concept id
# todo: make none-filtering upfront instead
class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    table_name: ClassVar[str] = "VisitOccurrence"

    def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
        baseline_assessment: TumorAssessmentBaseline = patient.tumor_assessment_baseline
        visits: List[VisitOccurrenceRow] = []

        visit_concept_id = self._concepts.lookup_structural("outpatient_visit")
        ecrf_concept_id = self._concepts.lookup_structural("ecrf")

        if patient.tumor_assessment_baseline and patient.tumor_assessment_baseline.assessment_date:
            visits.append(
                VisitOccurrenceRow(
                    visit_occurrence_id=self.generate_row_id(patient.patient_id),
                    person_id=person_id,
                    visit_concept_id=visit_concept_id.concept_id,
                    visit_start_date=baseline_assessment.assessment_date,
                    visit_end_date=baseline_assessment.assessment_date,
                    visit_type_concept_id=ecrf_concept_id.concept_id,
                    visit_source_value=patient.tumor_assessment_baseline.assessment_type,
                )
            )

        if patient.tumor_assessments:
            for assessment in patient.tumor_assessments:
                if assessment.date:
                    visits.append(
                        VisitOccurrenceRow(
                            visit_occurrence_id=self.generate_row_id(patient.patient_id, assessment.event_id),
                            person_id=person_id,
                            visit_concept_id=ecrf_concept_id.concept_id,
                            visit_start_date=assessment.date,
                            visit_end_date=assessment.date,
                            visit_type_concept_id=visit_concept_id.concept_id,
                            visit_source_value=assessment.assessment_type,
                        )
                    )

        return visits
