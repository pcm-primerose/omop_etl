import datetime as dt
from logging import getLogger
from typing import ClassVar

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.omop.models.rows import MeasurementRow
from omop_etl.omop.builders.base import (
    OmopBuilder,
    BuildContext,
)

log = getLogger(__name__)

# what Patient data?
# what branches are needed, how to structure/group etc
# define all states/branches for test

# what is inlcuded?
# measurements are stored as attribute value pairs, where the value is either
# a number or a concept.

# all measurements and orders of measurements:
#   - labs, questionnaires (?), biomarkers, medical history ongoing/past etc, tumor assessments (size, number of lesions),
#     adverse events with measurmenet domain, ecog, responce (recist,irecist,rano),

# c30, eq5d, ecog, biomarkers, response, tumor assessments / baseline (size, number of lesions),
# medical history (ongoing/past/etc) &

# so: any standardized intrument/test means data goes into measurement


class MeasurementBuilder(OmopBuilder[MeasurementRow]):
    table_name: ClassVar[str] = "measurement"

    def build(self, ctx: BuildContext):
        patient = ctx.patient
        person_id = ctx.person_id

        rows: list[MeasurementRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"}).concept_id

        if patient.ecog_baseline is not None:
            rows.extend(self._build_ecog_rows(patient, person_id, ecrf, patient.ecog_baseline))

        return rows

    def _build_ecog_rows(self, patient: Patient, person_id: int, ecrf_concept: int, ecog_baseline: EcogBaseline) -> list[MeasurementRow]:
        if ecog_baseline.date is None:
            log.warning(f"Skipping EcogBaseline for {patient.patient_id} in {self.table_name}: missing date")
            return []

        ecog_performance_field_concept = self.concepts.lookup_structural("ecog").concept_id
        if ecog_performance_field_concept is None:
            log.warning("No ECOG field concept found in structural mapping file")
            return []

        if ecog_baseline.grade:
            matches = self.concepts.lookup_semantic(
                patient_id=patient.patient_id,
                field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.GRADE),
                domains={"Meas Value"},
                leaf_index=None,
            )
        else:
            matches = self.concepts.lookup_semantic(
                patient_id=patient.patient_id,
                field_path=(Patient.Singletons.ECOG_BASELINE, EcogBaseline.Fields.DESCRIPTION),
                domains={"Meas Value"},
                leaf_index=None,
            )

        if matches is None:
            return []

        row_id = self.generate_row_id(
            ecog_baseline.patient_id,
            str(ecog_baseline.date),
        )

        return [
            MeasurementRow(
                measurement_id=row_id,
                person_id=person_id,
                measurement_concept_id=concept.concept_id,
                measurement_date=ecog_baseline.date,
                measurement_type_concept_id=ecrf_concept,
                measurement_datetime=dt.datetime(
                    ecog_baseline.date.year,
                    ecog_baseline.date.month,
                    ecog_baseline.date.day,
                ),
                operator_concept_id="",
                value_as_number="",
                value_as_concept_id="",
                unit_concept_id="",
                range_low="",
                range_high="",
                provider_id="",
                visit_occurrence_id="",
                visit_detail_id="",
                measurement_source_value="",
                measurement_source_concept_id="",
                unit_source_value="",
                unit_source_concept_id="",
                value_source_value="",
                measurement_event_id="",
                meas_event_field_concept_id="",
            )
            for concept in matches
        ]
