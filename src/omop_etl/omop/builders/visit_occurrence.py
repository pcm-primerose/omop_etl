import datetime as dt
from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.rows import VisitOccurrenceRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import (
    BuildResult,
    OmopRowReference,
    RowPublication,
    visit_source_ref,
)

log = getLogger(__name__)


class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    """
    Builds visit_occurrence rows from the tumor assessment collection.

    Publishes each emitted row under `SourceRef(patient_id, SourceAnchors.VISIT_DATE,
    (date,))` so downstream builders can resolve visit_occurrence_id by date via
    `ctx.resolve_visit_id(date)`.
    """

    # todo: after adding Visits domain model, refactor to not use SourceAnchor
    #   (as then visit dates are part of domain model)

    table_name: ClassVar[str] = OmopTables.VISIT_OCCURRENCE

    def build(self, ctx: BuildContext) -> BuildResult[VisitOccurrenceRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[VisitOccurrenceRow] = []
        publications: list[RowPublication] = []
        outpatient = self.concepts.lookup_structural("outpatient_visit", domains={"Visit"})
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        visit_concept_id = int(outpatient.concept_id) if outpatient else 0
        visit_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        # track previous visit for preceding_visit_occurrence_id FK
        prev_visit_id: int | None = None

        # One visit_occurrence per unique date: a visit IS a date in this model.
        # The baseline is the was_baseline assessment (dated at its V00 visit) and
        # is ordered first in the collection, so it anchors the visit chain like
        # any other assessment. seen_dates folds genuinely same-date assessments
        # (e.g. target + non-target measurements, or a visit recorded under two
        # event_ids like "V04"/"W00") into one visit_occurrence row.
        # (Future: model Visits as a first-class Patient domain so the builder
        # consumes deduped visit dates directly instead of reconstructing)
        seen_dates: set[dt.date] = set()

        for assessment in patient.tumor_assessments:
            assessment_date = assessment.date
            if assessment_date is None or assessment_date in seen_dates:
                continue
            row = self._build_assessment_row(
                patient,
                person_id,
                assessment,
                assessment_date,
                visit_concept_id,
                visit_type_concept_id,
                prev_visit_id,
            )
            seen_dates.add(assessment_date)
            prev_visit_id = row.visit_occurrence_id
            rows.append(row)
            publications.append(self._publish(patient, row))

        return BuildResult(rows=tuple(rows), publications=tuple(publications))

    @staticmethod
    def _publish(patient: Patient, row: VisitOccurrenceRow) -> RowPublication:
        # todo: refactor after adding Visits domain model
        return RowPublication(
            target_table=OmopTables.VISIT_OCCURRENCE,
            source_ref=visit_source_ref(patient.patient_id, row.visit_start_date),
            rows=(
                OmopRowReference(
                    table=OmopTables.VISIT_OCCURRENCE,
                    row_id=row.visit_occurrence_id,
                    primary_concept_id=row.visit_concept_id,
                ),
            ),
        )

    def _build_assessment_row(
        self,
        patient: Patient,
        person_id: int,
        assessment: TumorAssessment,
        date: dt.date,
        visit_concept_id: int,
        visit_type_concept_id: int,
        preceding_visit_id: int | None,
    ) -> VisitOccurrenceRow:
        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.TUMOR_ASSESSMENTS,
            *assessment.natural_key(),
        )

        return VisitOccurrenceRow(
            visit_occurrence_id=row_id,
            person_id=person_id,
            visit_concept_id=visit_concept_id,
            visit_start_date=date,
            visit_end_date=date,
            visit_type_concept_id=visit_type_concept_id,
            visit_source_value=assessment.assessment_type,
            preceding_visit_occurrence_id=preceding_visit_id,
        )
