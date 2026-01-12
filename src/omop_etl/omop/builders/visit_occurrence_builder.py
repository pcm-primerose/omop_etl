from typing import ClassVar, List

from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import VisitOccurrenceRow
from omop_etl.harmonization.models import (
    Patient,
    TumorAssessmentBaseline,
)


class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    table_name: ClassVar[str] = "visit_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
        baseline_assessment: TumorAssessmentBaseline = patient.tumor_assessment_baseline
        visits: List[VisitOccurrenceRow] = []

        visit_concept_id = self._concepts.lookup_structural("outpatient_visit")
        ecrf_concept_id = self._concepts.lookup_structural("ecrf")

        if baseline_assessment is not None:
            start_date = (
                baseline_assessment.assessment_date
                or baseline_assessment.target_lesion_measurement_date
                or baseline_assessment.off_target_lesion_measurement_date
            )

            # todo: should not create instance if there is not start_date found (i.e. it is None)
            # and baseline_assessment.assessment_date:
            if start_date is not None:
                visits.append(
                    VisitOccurrenceRow(
                        visit_occurrence_id=self.generate_row_id(patient.patient_id),
                        person_id=person_id,
                        visit_concept_id=visit_concept_id.concept_id,
                        visit_start_date=start_date,
                        visit_end_date=start_date,
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
                            visit_concept_id=visit_concept_id.concept_id,
                            visit_start_date=assessment.date,
                            visit_end_date=assessment.date,
                            visit_type_concept_id=ecrf_concept_id.concept_id,
                            visit_source_value=assessment.assessment_type,
                        )
                    )

        return visits
