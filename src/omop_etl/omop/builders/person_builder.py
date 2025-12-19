from omop_etl.harmonization.datamodels import Patient
from omop_etl.omop.models.rows import PersonRow
from omop_etl.mapping.concept_service import ConceptMappingService


class PersonRowBuilder:
    def __init__(self, concept_service: ConceptMappingService):
        self._concepts = concept_service

    def build(self, patient: Patient, person_id: int) -> PersonRow | None:
        sex_raw = patient.sex

        mapped = None
        if sex_raw is not None:
            mapped = self._concepts.lookup_static("sex", sex_raw)

        if sex_raw is not None and mapped is None:
            raise ValueError(f"Unknown sex mapping for value={sex_raw!r} on patient={patient.patient_id}")

        gender_concept_id = mapped.concept_id if mapped else 0

        dob = patient.date_of_birth
        # dob is req by cdm
        if dob is None:
            return None

        return PersonRow(
            person_id=person_id,
            gender_concept_id=gender_concept_id,
            year_of_birth=dob.year if dob is not None else None,
            month_of_birth=dob.month if dob is not None else None,
            day_of_birth=dob.day if dob is not None else None,
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
