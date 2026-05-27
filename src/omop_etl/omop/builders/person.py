import datetime as dt
from typing import ClassVar

from omop_etl.omop.builders.base import BuildContext, BuildResult, OmopBuilder
from omop_etl.omop.models.rows import PersonRow
from omop_etl.omop.models.tables import OmopTables


class PersonBuilder(OmopBuilder[PersonRow]):
    """Builds Person table rows from patient demographics."""

    table_name: ClassVar[str] = OmopTables.PERSON

    def build(self, ctx: BuildContext) -> BuildResult[PersonRow]:
        patient = ctx.patient
        sex_raw = patient.sex

        mapped = None
        if sex_raw is not None:
            mapped = self.concepts.lookup_static("sex", sex_raw, domains={"Gender"})

        gender_concept_id = int(mapped.concept_id) if mapped else 0

        dob = patient.date_of_birth
        if dob is None:
            return BuildResult(rows=())

        row = PersonRow(
            person_id=ctx.person_id,
            gender_concept_id=gender_concept_id,
            year_of_birth=dob.year,
            month_of_birth=dob.month,
            day_of_birth=dob.day,
            birth_datetime=dt.datetime(dob.year, dob.month, dob.day),
            race_concept_id=0,
            ethnicity_concept_id=0,
            person_source_value=patient.patient_id,
            gender_source_value=sex_raw,
            gender_source_concept_id=0,
            race_source_value=None,
            race_source_concept_id=0,
            ethnicity_source_value=None,
            ethnicity_source_concept_id=0,
        )
        return BuildResult(rows=(row,))
