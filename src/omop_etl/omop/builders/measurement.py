import datetime as dt
from logging import getLogger
from typing import ClassVar

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


# todo:
#   [ ] include adverse events or not?
#       - AEs are free-text but also terms from ctcaet and graded,
#         so belongs in observations, but some terms are measurements,
#         e.g. labs like "decreased platelet count".
#       - would then need to expoand semantic config to measurement,
#         filter on measurement only and emit one row per mapped concept for AEs
#   [x] are biomarkers semantically mapped to measurement or observations?
#   [x] one method per mapped concept, or one method per domain model -> rows?
#       - tumor assessments would emit multiple concepts/rows: leaning one per domain
#   [x] cohort target name or gene and mutation as main biomarker row?
#       - use the one with highest prevalance, fallback to secondary field?

# tumor assessment baseline:
# emit two rows per instance basically:
# target_lesion_size, off_target_lesions_number, target_lesions_nadir
# value as number/str separates these?
# also: number_of_lesions are currently in observation,
# but can't find any concept for this in measurement domain: defer to observation!

# biomarkers
# use cohort_target_mutation, fallback to cohort_target_name
#           - COHTMN/cohort target mutation name is:
#             e.g. Activating mutations of SSH pathway or MMR-genes incl res
#           - COHCTN/cohort target name is:
#             e.g. SSH pathway or MSI-high incl. res
#       --> cohort_target_mutation_name (COHTMN) seems most specific,
#           wheras gene_and_mutation is more unspecific in some cases (e.g. BRAF non-v600 is here BRAF activating mutations)

# tumor assessments
# just check what response fields have data per instance,
# emit resoponses as value_as_concept_id, tumor assessment types as attr (measurement_concept_id),
# row id from (patient_id, TUMOR_ASSESSMENTS, date, response_type)
# todo: need to map change in lesion size (or just lesion size) per instance!
# we need to track the change in lesion size from baseline, this is very important.
# this is stored as a float, denoting the percentage change from baseline lesion size (e.g. -0.5) iirc.
# so this means we can see over time the response in the tumor size to treatment.
# each instance of tumor assessments has data here.
# what concept can i use for this?
# maybe just map the absolute size per instance (would ideally need to update the tumor assessment domain model),
# instead of calculating this in the builder (should be harmonizer job)
# so, as long as we can store the lesion size per instance (date linked), we can
# calculate change from baseline and nadir (these are derived), after extraction from the database
# so either 1) change the domain model to add a field that calculates the total size,
# or 2) store the values as the relative change floats per instance in:
# lesion_size,4084390,246116008,Lesion size,Observable Entity,Standard,Valid,Measurement,SNOMED
# maybe? think it's better to update the patient model, but then i'd need to update:
# _process_tumor_assessments() method and tests, adding a method to calculate the absolute size,
# but this then requires knowledge of the absolute size at baseline, which is processed in the _process_tumor_assessment_baseline()
# method. I could also do the same column selection and processing as in the baseline method, and
# then match to the correct patient and tumor assessment type etc, not a basic change and need more robust testing.
# regardless, tracking the absolute size of the target lesion per instance allows for calculating the percentage change
# over time and the change from nadir over time.

# end of treatment
# just access patient scalars directly (end_of_treatment_reason, end_of_treatment_date):
# one concept:
# eot_reason,Normal completion according to cohort-specific manual,45884335,LA4511-7,Treatment Completed,Answer,Standard,Valid,Meas Value,LOINC
# todo: find observation concept for this instead: conceptually EOT reason belongs in observation?
# at least all other EOT reasons are in observation...
# conclusion: just defer this to when I implement the observation builder!

# c30
# quite simple with the loop,
# but shouldn't value_as_concept_id be the static mappings for c30 in the static_mapping.csv file?
# other than that I agree.

# eq5d
# same as for c30, shouldn't the static mappings for answers be used as value_as_concept_id?
#


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

    def _build_biomarker_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        biomarkers: Biomarkers,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def _build_tumor_assessment_baseline_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        tumor_assessment_baseline: TumorAssessmentBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def _build_tumor_assessment_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        tumor_assessments: TumorAssessment,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def _build_c30_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        c30: C30,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass

    def _build_eq5d_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        eq5d: EQ5D,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        pass
