from typing import ClassVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.models.rows import PersonRow


class PersonBuilder(OmopBuilder[PersonRow]):
    """Builds Person table rows from patient demographics."""

    table_name: ClassVar[str] = "person"

    def build(self, patient: Patient, person_id: int) -> list[PersonRow]:
        sex_raw = patient.sex

        mapped = None
        if sex_raw is not None:
            mapped = self._concepts.lookup_static("sex", sex_raw)

        if sex_raw is not None and mapped is None:
            raise ValueError(f"Unknown sex mapping for value={sex_raw!r} on patient={patient.patient_id}")

        gender_concept_id = mapped.concept_id if mapped else 0

        dob = patient.date_of_birth
        # dob is required by CDM
        if dob is None:
            return []

        row = PersonRow(
            person_id=person_id,
            gender_concept_id=gender_concept_id,
            year_of_birth=dob.year,
            month_of_birth=dob.month,
            day_of_birth=dob.day,
            birth_datetime=None,
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

        return [row]
