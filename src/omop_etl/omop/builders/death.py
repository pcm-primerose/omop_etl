from typing import ClassVar
from logging import getLogger
import datetime as dt

from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import BuildResult
from omop_etl.omop.models.rows import DeathRow
from omop_etl.omop.models.tables import OmopTables

log = getLogger(__name__)


class DeathBuilder(OmopBuilder[DeathRow]):
    table_name: ClassVar[str] = OmopTables.DEATH

    def build(self, ctx: BuildContext) -> BuildResult[DeathRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        ecrf_concept_candidate = self.concepts.lookup_structural(value_set="ecrf", domains={"type concept"})
        ecrf_concept = ecrf_concept_candidate.concept_id if ecrf_concept_candidate else 0

        date_of_death = patient.date_of_death
        if date_of_death is None:
            return BuildResult(rows=(), publications=())

        datetime_of_death = dt.datetime(
            date_of_death.year,
            date_of_death.month,
            date_of_death.day,
        )

        death_row = DeathRow(
            person_id=person_id,
            death_date=date_of_death,
            death_datetime=datetime_of_death,
            death_type_concept_id=ecrf_concept,
        )

        return BuildResult(rows=(death_row,), publications=())
