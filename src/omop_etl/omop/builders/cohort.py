import datetime as dt
from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import BuildContext, BuildResult, OmopBuilder
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import CohortRow
from omop_etl.omop.models.tables import OmopTables

log = getLogger(__name__)


class CohortBuilder(OmopBuilder[CohortRow]):
    """
    One cohort membership row per patient: the cohort the patient was placed in.

    - subject_id = person_id.
    - cohort_definition_id = the content-addressed cohort_definition id
      (row_id over the normalized cohort name), which CohortDefinitionBuilder
      derives identically, so the row joins to its definition without resolving it.
      Not trial-scoped: the normalized name is cross-trial canonical, so the same
      arm in different trials shares one definition.
    - cohort_start_date = treatment_start_date, falling back to the earliest
      treatment-cycle start.
    - cohort_end_date = end_of_treatment date, then date_of_death, then
      treatment_start_last_cycle (the observation_period end, since that period
      ends at eot.date or treatment_start_last_cycle).

    Skipped when the cohort did not normalize, or when a start or
    end date cannot be derived.
    """

    table_name: ClassVar[str] = OmopTables.COHORT

    def build(self, ctx: BuildContext) -> BuildResult[CohortRow]:
        patient = ctx.patient
        cohort = patient.cohort
        if cohort is None or cohort.normalized_name is None:
            return BuildResult(rows=())

        start_date = self._cohort_start(patient)
        if start_date is None:
            log.warning("Skipping cohort for %s: no usable start date", patient.patient_id)
            return BuildResult(rows=())

        end_date = self._cohort_end(patient)
        if end_date is None:
            log.warning("Skipping cohort for %s: no usable end date", patient.patient_id)
            return BuildResult(rows=())

        row = CohortRow(
            cohort_definition_id=row_id(OmopTables.COHORT_DEFINITION, cohort.normalized_name),
            subject_id=ctx.person_id,
            cohort_start_date=start_date,
            cohort_end_date=end_date,
        )
        return BuildResult(rows=(row,))

    @staticmethod
    def _cohort_start(patient: Patient) -> dt.date | None:
        if patient.treatment_start_date is not None:
            return patient.treatment_start_date
        cycle_starts = [start for c in patient.treatment_cycles if (start := c.start_date) is not None]
        return min(cycle_starts) if cycle_starts else None

    @staticmethod
    def _cohort_end(patient: Patient) -> dt.date | None:
        eot = patient.end_of_treatment
        if eot is not None and eot.date is not None:
            return eot.date
        if patient.date_of_death is not None:
            return patient.date_of_death
        return patient.treatment_start_last_cycle
