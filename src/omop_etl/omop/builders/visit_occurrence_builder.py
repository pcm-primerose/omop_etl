from typing import ClassVar, List
import datetime as dt

from omop_etl.harmonization.models import Patient
from omop_etl.omop.models.rows import VisitOccurrenceRow


class VisitOccurrenceBuilder:
    table_name: ClassVar[str] = "VisitOccurrence"

    def build(self, patient: Patient, person_id: int) -> list[VisitOccurrenceRow]:
        baseline_assessment = patient.tumor_assessment_baseline.assessment_date
        assessment_dates: List[dt.date] = []

        for assessment in patient.tumor_assessments:
            assessment_dates.append(assessment.date)
