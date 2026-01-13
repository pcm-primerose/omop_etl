from typing import ClassVar

from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import VisitOccurrenceRow
from omop_etl.harmonization.models.patient import Patient


class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    table_name: ClassVar[str] = "visit_occurrence"

    def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
        outpatient = self._concepts.lookup_structural("outpatient_visit")
        ecrf = self._concepts.lookup_structural("ecrf")

        rows: list[VisitOccurrenceRow] = []

        if (baseline := patient.tumor_assessment_baseline) is not None:
            start_date = baseline.assessment_date or baseline.target_lesion_measurement_date or baseline.off_target_lesion_measurement_date
            if start_date is not None:
                rows.append(
                    VisitOccurrenceRow(
                        visit_occurrence_id=self.generate_row_id(patient.patient_id),
                        person_id=person_id,
                        visit_concept_id=outpatient.concept_id,
                        visit_start_date=start_date,
                        visit_end_date=start_date,
                        visit_type_concept_id=ecrf.concept_id,
                        visit_source_value=baseline.assessment_type,
                    )
                )

        seen_event_ids: set[str] = set()
        for a in patient.tumor_assessments:
            if a.date is None:
                continue
            if a.event_id in seen_event_ids:
                continue
            seen_event_ids.add(a.event_id)
            rows.append(
                VisitOccurrenceRow(
                    visit_occurrence_id=self.generate_row_id(patient.patient_id, a.event_id),
                    person_id=person_id,
                    visit_concept_id=outpatient.concept_id,
                    visit_start_date=a.date,
                    visit_end_date=a.date,
                    visit_type_concept_id=ecrf.concept_id,
                    visit_source_value=a.assessment_type,
                )
            )

        return rows


# class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
#     table_name: ClassVar[str] = "visit_occurrence"
#
#     def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
#         baseline_assessment: TumorAssessmentBaseline = patient.tumor_assessment_baseline
#         visits: List[VisitOccurrenceRow] = []
#
#         visit_concept_id = self._concepts.lookup_structural("outpatient_visit")
#         ecrf_concept_id = self._concepts.lookup_structural("ecrf")
#
#         if baseline_assessment is not None:
#             start_date = (
#                 baseline_assessment.assessment_date
#                 or baseline_assessment.target_lesion_measurement_date
#                 or baseline_assessment.off_target_lesion_measurement_date
#             )
#
#             if start_date is not None:
#                 visits.append(
#                     VisitOccurrenceRow(
#                         visit_occurrence_id=self.generate_row_id(patient.patient_id),
#                         person_id=person_id,
#                         visit_concept_id=visit_concept_id.concept_id,
#                         visit_start_date=start_date,
#                         visit_end_date=start_date,
#                         visit_type_concept_id=ecrf_concept_id.concept_id,
#                         visit_source_value=patient.tumor_assessment_baseline.assessment_type,
#                     )
#                 )
#
#         # todo: dedupe on (person, date, event_id)?
#         # can there be multiple assessments on the same date,
#         # that have the same event id?
#         # check logic in harmonizer
#
#         seen_dates: set[dt.date] = set()
#
#         if patient.tumor_assessments:
#             for assessment in patient.tumor_assessments:
#                 if assessment.date:
#                     seen_dates.add(assessment.date)
#                     if assessment.date not in seen_dates:
#                     visits.append(
#                         VisitOccurrenceRow(
#                             visit_occurrence_id=self.generate_row_id(
#                                 patient.patient_id,
#                                 assessment.event_id,
#                                 assessment.date.strftime(format="%d/%m/%Y")
#                             ),
#                             person_id=person_id,
#                             visit_concept_id=visit_concept_id.concept_id,
#                             visit_start_date=assessment.date,
#                             visit_end_date=assessment.date,
#                             visit_type_concept_id=ecrf_concept_id.concept_id,
#                             visit_source_value=assessment.assessment_type,
#                         )
#                     )
#
#         return visits
