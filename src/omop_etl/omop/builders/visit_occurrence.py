from typing import ClassVar
from logging import getLogger
import datetime as dt

from omop_etl.harmonization.models.domain.visit import Visit
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.rows import VisitOccurrenceRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import (
    BuildResult,
    OmopRowReference,
    RowPublication,
    SourceReference,
)

log = getLogger(__name__)


class VisitOccurrenceBuilder(OmopBuilder[VisitOccurrenceRow]):
    """
    Builds visit_occurrence rows from the patient's Visit collection,
    one visit_occurrence per Visit.

    Publishes each row under `SourceReference(patient_id, Collections.VISITS,
    visit.natural_key())`. The Visit NK is `(date,)`), so downstream builders
    resolve visit_occurrence_id by date via `ctx.resolve_visit_id(date)`.
    """

    table_name: ClassVar[str] = OmopTables.VISIT_OCCURRENCE

    def build(self, ctx: BuildContext) -> BuildResult[VisitOccurrenceRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[VisitOccurrenceRow] = []
        publications: list[RowPublication] = []
        outpatient = self.concepts.lookup_structural("outpatient_visit", domains={"Visit"})
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        if outpatient is None:
            raise ValueError(f"Outpatient not found in structural lookup: {outpatient}")
        if ecrf is None:
            raise ValueError(f"eCRF concept not found in structural lookup: {ecrf}")

        visit_concept_id = outpatient.concept_id
        visit_type_concept_id = ecrf.concept_id

        # Visits are date-ordered by natural key, so iterating yields the visit
        # chain: track the previous visit for preceding_visit_occurrence_id
        prev_visit_id: int | None = None

        for visit in patient.visits:
            date = visit.date
            if date is None:
                continue
            row = self._build_visit_row(
                patient,
                person_id,
                visit,
                date,
                visit_concept_id,
                visit_type_concept_id,
                prev_visit_id,
            )
            prev_visit_id = row.visit_occurrence_id
            rows.append(row)
            publications.append(self._publish(patient, visit, row))

        return BuildResult(rows=tuple(rows), publications=tuple(publications))

    @staticmethod
    def _publish(patient: Patient, visit: Visit, row: VisitOccurrenceRow) -> RowPublication:
        return RowPublication(
            target_table=OmopTables.VISIT_OCCURRENCE,
            source_ref=SourceReference(
                patient.patient_id,
                Patient.Collections.VISITS,
                visit.natural_key(),
            ),
            rows=(
                OmopRowReference(
                    table=OmopTables.VISIT_OCCURRENCE,
                    row_id=row.visit_occurrence_id,
                    primary_concept_id=row.visit_concept_id,
                ),
            ),
        )

    def _build_visit_row(
        self,
        patient: Patient,
        person_id: int,
        visit: Visit,
        date: dt.date,
        visit_concept_id: int,
        visit_type_concept_id: int,
        preceding_visit_id: int | None,
    ) -> VisitOccurrenceRow:
        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Collections.VISITS,
            *visit.natural_key(),
        )

        return VisitOccurrenceRow(
            visit_occurrence_id=row_id,
            person_id=person_id,
            visit_concept_id=visit_concept_id,
            visit_start_date=date,
            visit_end_date=date,
            visit_type_concept_id=visit_type_concept_id,
            visit_source_value=visit.event_id,
            preceding_visit_occurrence_id=preceding_visit_id,
        )
