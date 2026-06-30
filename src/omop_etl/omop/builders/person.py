import datetime as dt
from typing import ClassVar

from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import PersonRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.omop.builders.base import (
    BuildContext,
    BuildResult,
    OmopBuilder,
)


class PersonBuilder(OmopBuilder[PersonRow]):
    """Builds Person table rows from patient demographics."""

    table_name: ClassVar[str] = OmopTables.PERSON

    def build(self, ctx: BuildContext) -> BuildResult[PersonRow]:
        patient = ctx.patient
        sex_raw = patient.sex

        mapped = ()
        if sex_raw is not None:
            mapped = self.concepts.resolve("sex", sex_raw, domains={"Gender"})

        gender_concept_id = mapped[0].concept_id if mapped else 0

        dob = patient.date_of_birth
        if dob is None:
            return BuildResult(rows=())

        # country-level location from the trial; content-addressed to LocationBuilder's row
        country = self.concepts.resolve("trial_country", patient.trial_id, domains={"Geography"})
        location_id = row_id(OmopTables.LOCATION, country[0].concept_id) if country else None

        row = PersonRow(
            person_id=ctx.person_id,
            gender_concept_id=gender_concept_id,
            location_id=location_id,
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
