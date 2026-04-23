import datetime as dt
from logging import getLogger
from typing import ClassVar

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
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
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        measurement_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        if patient.ecog_baseline is not None:
            rows.extend(self._build_ecog_rows(patient, person_id, measurement_type_concept_id, patient.ecog_baseline, ctx))

        if patient.biomarkers is not None:
            rows.extend(self._build_biomarker_rows(patient, person_id, measurement_type_concept_id, patient.biomarkers, ctx))

        return rows

    def _build_ecog_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        ecog_baseline: EcogBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        if ecog_baseline.date is None:
            log.warning("Skipping ECOG for %s: missing date", patient.patient_id)
            return []

        if ecog_baseline.grade is None:
            log.warning("Skipping ECOG for %s: missing grade", patient.patient_id)
            return []

        # ECOG performance status score
        ecog_test = self.concepts.lookup_structural("ecog", domains={"Measurement"})
        if ecog_test is None:
            log.warning("No ECOG structural concept found")
            return []

        # specific grade answer concept
        ecog_answer = self.concepts.lookup_static(
            "ecog_code",
            str(ecog_baseline.grade),
            domains={"Meas Value"},
        )
        value_as_concept_id = int(ecog_answer.concept_id) if ecog_answer else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.ECOG_BASELINE,
            str(ecog_baseline.date),
        )
        visit_occurrence_id = ctx.visit_id_by_date.get(ecog_baseline.date)

        return [
            MeasurementRow(
                measurement_id=row_id,
                person_id=person_id,
                measurement_concept_id=int(ecog_test.concept_id),
                measurement_date=ecog_baseline.date,
                measurement_type_concept_id=ecrf_concept,
                measurement_datetime=dt.datetime(
                    ecog_baseline.date.year,
                    ecog_baseline.date.month,
                    ecog_baseline.date.day,
                ),
                value_as_number=float(ecog_baseline.grade),
                value_as_concept_id=value_as_concept_id,
                visit_occurrence_id=visit_occurrence_id,
                measurement_source_value=str(ecog_baseline.grade),
            )
        ]

    def build_biomarker_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        biomarkers: Biomarkers,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_response_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        responses: TumorAssessment,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_adverse_event_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        adverse_events: AdverseEvent,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_tumor_assessment_baseline_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        tumor_assessment_baseline: TumorAssessmentBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_tumor_assessment_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        tumor_assessments: TumorAssessment,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_c30_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        c30: C30,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def build_eq5d_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        eq5d: EQ5D,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass
