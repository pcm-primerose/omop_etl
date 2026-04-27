import datetime as dt
from logging import getLogger
from typing import ClassVar

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.models.rows import MeasurementRow
from omop_etl.semantic_mapping.core.models import OmopDomain
from omop_etl.omop.builders.base import (
    BuildContext,
    OmopBuilder,
)

log = getLogger(__name__)

# todo:
# [ ] verify logic/mapping & clean up code
# [ ] clean up tests


class MeasurementBuilder(OmopBuilder[MeasurementRow]):
    """Builds measurement rows from ECOG, tumor-assessment baseline + per-instance
    assessments, C30/EQ5D PROs, biomarkers, and adverse-event terms that map to the
    Measurement domain.

    CDM 5.4 policy:
    - `measurement_concept_id` must be in measurement domain. Rows are skipped if no
      Measurement concept is available.
    - `value_as_concept_id` is Meas Value domain: NULL when the source has no
      categorical result; 0 when there is a categorical result but no mapping;
      otherwise the mapped concept id.
    - `value_as_number` is the numeric result where the source provides one.
    - `visit_occurrence_id` is linked by date via `ctx.visit_id_by_date` (populated
      by the visit_occurrence builder, which must run before this one).
    """

    table_name: ClassVar[str] = "measurement"

    def build(self, ctx: BuildContext) -> list[MeasurementRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        rows: list[MeasurementRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        measurement_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        ecog_baseline = patient.ecog_baseline
        if ecog_baseline is not None:
            rows.extend(
                self._build_ecog_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    ecog_baseline,
                    ctx,
                )
            )

        ta_baseline = patient.tumor_assessment_baseline
        if ta_baseline is not None:
            rows.extend(
                self._build_tumor_assessment_baseline_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    ta_baseline,
                    ctx,
                )
            )

        for idx, ta in enumerate(patient.tumor_assessments):
            rows.extend(
                self._build_tumor_assessment_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    ta,
                    idx,
                    ctx,
                )
            )

        for idx, c30 in enumerate(patient.c30_collection):
            rows.extend(
                self._build_c30_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    c30,
                    idx,
                    ctx,
                )
            )

        for idx, eq5d in enumerate(patient.eq5d_collection):
            rows.extend(
                self._build_eq5d_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    eq5d,
                    idx,
                    ctx,
                )
            )

        biomarkers = patient.biomarkers
        if biomarkers is not None:
            rows.extend(
                self._build_biomarker_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    biomarkers,
                    ctx,
                )
            )

        for idx, ae in enumerate(patient.adverse_events):
            rows.extend(
                self._build_adverse_event_rows(
                    patient,
                    person_id,
                    measurement_type_concept_id,
                    ae,
                    idx,
                    ctx,
                )
            )

        return rows

    def _build_ecog_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        ecog_baseline: EcogBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        Emits 0 or 1 row from EcogBaseline.
        Skipped if date, grade or ecog structural concept is missing.
        measurement_concept_id is the structural ecog concept,
        with the value_as_concept_id containing the static mapping per grade (0 if grade has no static mapping)
        and the raw score in value_as_number.

        Skipped if date, grade, or ecog structural concept is missing.
        """
        date = ecog_baseline.date
        grade = ecog_baseline.grade
        if date is None:
            log.warning("Skipping ECOG for %s: missing date", patient.patient_id)
            return []
        if grade is None:
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
            str(grade),
            domains={"Meas Value"},
        )
        value_as_concept_id = int(ecog_answer.concept_id) if ecog_answer else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.ECOG_BASELINE,
            str(date),
        )
        return [
            MeasurementRow(
                measurement_id=row_id,
                person_id=person_id,
                measurement_concept_id=int(ecog_test.concept_id),
                measurement_date=date,
                measurement_type_concept_id=ecrf_concept,
                measurement_datetime=dt.datetime(date.year, date.month, date.day),
                value_as_number=float(grade),
                value_as_concept_id=value_as_concept_id,
                visit_occurrence_id=ctx.visit_id_by_date.get(date),
                measurement_source_value=str(grade)[0:50],
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
        """
        Emits 0-N rows from Biomarkers, one row per matched concept for selected
        biomarker field in preference chain.
        Cohort fields are inclusion criteria and are preferred when mapping.

        measurement_concept_id is each mapped semantic concept (one row each),
        can return multiple (meaning multiple rows, decided by semantic mapper).
        measurement_source_value is the raw biomarker value.

        value_as_number and value_as_concept_id is not set.

        Skipped if date is missing, or if no field in chain maps to a measurement concept.
        """
        date = biomarkers.date
        if date is None:
            log.warning("Skipping biomarkers for %s: missing date", patient.patient_id)
            return []

        chain: list[tuple[str, str | None]] = [
            (Biomarkers.Fields.COHORT_TARGET_MUTATION, biomarkers.cohort_target_mutation),
            (Biomarkers.Fields.COHORT_TARGET_NAME, biomarkers.cohort_target_name),
            (Biomarkers.Fields.GENE_AND_MUTATION, biomarkers.gene_and_mutation),
        ]
        for field_name, source_value in chain:
            if source_value is None:
                continue
            matches = self.concepts.lookup_semantic(
                patient.patient_id,
                (Patient.Singletons.BIOMARKERS, field_name),
                None,
                domains={OmopDomain.MEASUREMENTS},
            )
            if not matches:
                continue

            datetime_value = dt.datetime(date.year, date.month, date.day)
            visit_occurrence_id = ctx.visit_id_by_date.get(date)
            return [
                MeasurementRow(
                    measurement_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Singletons.BIOMARKERS,
                        field_name,
                        str(concept.concept_id),
                    ),
                    person_id=person_id,
                    measurement_concept_id=int(concept.concept_id),
                    measurement_date=date,
                    measurement_datetime=datetime_value,
                    measurement_type_concept_id=ecrf_concept,
                    visit_occurrence_id=visit_occurrence_id,
                    measurement_source_value=source_value[:50],
                )
                for concept in matches
            ]

        return []

    def _build_tumor_assessment_baseline_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        baseline: TumorAssessmentBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        Emits 0 or 1 row from TumorAssessmentBaseline.
        measurment_concept_id is the structural lookup for 'lesion_size',
        value_as_number is the actual size at baseline.

        Skipped if target_lesion_size, target_lesion_measurement_date, or
        lesion_size structural concept is missing.
        """
        size = baseline.target_lesion_size
        date = baseline.target_lesion_measurement_date
        if size is None:
            log.warning("No size for tumor assessment baseline for %s", patient.patient_id)
            return []
        if date is None:
            log.warning("No date for tumor assessment baseline for %s", patient.patient_id)
            return []

        lesion = self.concepts.lookup_structural("lesion_size", domains={OmopDomain.MEASUREMENTS})
        if lesion is None:
            log.warning("No lesion_size structural concept for %s", patient.patient_id)
            return []

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.TUMOR_ASSESSMENT_BASELINE,
            TumorAssessmentBaseline.Fields.TARGET_LESION_SIZE,
        )
        return [
            MeasurementRow(
                measurement_id=row_id,
                person_id=person_id,
                measurement_concept_id=int(lesion.concept_id),
                measurement_date=date,
                measurement_datetime=dt.datetime(date.year, date.month, date.day),
                measurement_type_concept_id=ecrf_concept,
                value_as_number=float(size),
                visit_occurrence_id=ctx.visit_id_by_date.get(date),
                measurement_source_value=str(size)[0:50],
            )
        ]

    def _build_tumor_assessment_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        tumor_assessments: TumorAssessment,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        Emits 0-4 rows per TumorAssessment instance.
        1 row for each mapped target_lesion_size field when set,
        using the strutural 'lesion_size' lookup as the measurement_concept_id,
        storing the lesion size in the measurement_as_number.

        1-3 rows as emitted per populated response scale (RECIST, iRECIST, RANO).
        The responses use precooordinated pairs, so the measurement_concept_id stores
        both the question and value (same as EQ5D) as on static concept.

        If date is missing the instance is skipped entirely and no rows are emitted.
        """
        date = tumor_assessments.date
        if date is None:
            log.warning("Skipping tumor assessment %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.visit_id_by_date.get(date)
        datetime_value = dt.datetime(date.year, date.month, date.day)

        # absolute target-lesion size
        size = tumor_assessments.target_lesion_size
        if size is not None:
            lesion = self.concepts.lookup_structural("lesion_size", domains={OmopDomain.MEASUREMENTS})
            if lesion is not None:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            str(tumor_assessments.event_id),
                            str(date),
                            TumorAssessment.Fields.TARGET_LESION_SIZE,
                        ),
                        person_id=person_id,
                        measurement_concept_id=int(lesion.concept_id),
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        value_as_number=float(size),
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=str(size)[:50],
                    )
                )

        # tumor assessment response rows
        recist = tumor_assessments.recist_response
        if recist is not None:
            concept = self.concepts.lookup_static("response_recist", recist, domains={OmopDomain.MEASUREMENTS})
            if concept is None:
                log.warning("No response_recist mapping for %r (patient %s)", recist, patient.patient_id)
            else:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            str(tumor_assessments.event_id),
                            str(date),
                            TumorAssessment.Fields.RECIST_RESPONSE,
                        ),
                        person_id=person_id,
                        measurement_concept_id=int(concept.concept_id),
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=recist[:50],
                    )
                )

        irecist = tumor_assessments.irecist_response
        if irecist is not None:
            concept = self.concepts.lookup_static("response_irecist", irecist, domains={OmopDomain.MEASUREMENTS})
            if concept is None:
                log.warning("No response_irecist mapping for %r (patient %s)", irecist, patient.patient_id)
            else:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            str(tumor_assessments.event_id),
                            str(date),
                            TumorAssessment.Fields.IRECIST_RESPONSE,
                        ),
                        person_id=person_id,
                        measurement_concept_id=int(concept.concept_id),
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=irecist[:50],
                    )
                )

        rano = tumor_assessments.rano_response
        if rano is not None:
            concept = self.concepts.lookup_static("response_rano", rano, domains={OmopDomain.MEASUREMENTS})
            if concept is None:
                log.warning("No response_rano mapping for %r (patient %s)", rano, patient.patient_id)
            else:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            str(tumor_assessments.event_id),
                            str(date),
                            TumorAssessment.Fields.RANO_RESPONSE,
                        ),
                        person_id=person_id,
                        measurement_concept_id=int(concept.concept_id),
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=rano[:50],
                    )
                )

        return rows

    def _build_c30_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        c30: C30,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        QLQ-C30 has 30 'question' fields. For each C30 instance the builder emits one row per answered question,
        where either the answer or code is populated. These fields combine to a single row.

        measurement_concept_id is populated by the structural question code for that field.

        value_as_concept_id:
        For Q1-Q28 the 4-level static lookups are used.
        Q29-30 uses specific static concepts unique to those fields.

        If a level is missing, sets the value to NULL, 0 when level is set but unmapped.
        If date is missing, the entire instance is skipped and no rows are emitted.
        """
        date = c30.date
        if date is None:
            log.warning("Skipping C30 instance %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.visit_id_by_date.get(date)
        datetime_value = dt.datetime(date.year, date.month, date.day)

        for n in range(1, C30.Q_COUNT + 1):
            answer_text: str | None = getattr(c30, f"q{n}")
            answer_level: int | None = getattr(c30, f"q{n}_code")
            if answer_text is None and answer_level is None:
                continue

            test_concept = self.concepts.lookup_structural(f"c30_q{n}", domains={OmopDomain.MEASUREMENTS})
            if test_concept is None:
                log.warning("No measurement domain concept for C30 test found for patient %s", patient.patient_id)
                continue

            # Q29 (overall health) and Q30 (overall QoL) use the 1–7 global scale
            value_as_concept_id: int | None = None
            if answer_level is not None:
                answer_set = "c30_global_answer_code" if n in (29, 30) else "c30_answer_code"
                answer_concept = self.concepts.lookup_static(answer_set, str(answer_level), domains={"Meas Value"})
                value_as_concept_id = int(answer_concept.concept_id) if answer_concept else 0

            source_value = answer_text if answer_text is not None else str(answer_level)
            rows.append(
                MeasurementRow(
                    measurement_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.C30_COLLECTION,
                        str(c30.event_name),
                        str(date),
                        f"q{n}",
                    ),
                    person_id=person_id,
                    measurement_concept_id=int(test_concept.concept_id),
                    measurement_date=date,
                    measurement_datetime=datetime_value,
                    measurement_type_concept_id=ecrf_concept,
                    value_as_number=float(answer_level) if answer_level is not None else None,
                    value_as_concept_id=value_as_concept_id,
                    visit_occurrence_id=visit_occurrence_id,
                    measurement_source_value=source_value[:50],
                )
            )

        return rows

    def _build_eq5d_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        eq5d: EQ5D,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        EQ5D: 5 question dimension and the VAS QoL score.
        Per EQ5D instance, emits 0–6 rows.

        Each question dimension uses a precoordinated static concept
        that encodes the question and answer together in measurement_concept_id.
        The actual score is in value_as_number.

        The VAS score uses the rq5d_qol_core structural concept for measurement_concept_id,
        storing the score in value_as_number.

        If VAS score or question code field is missing, skips that field and emits no row.
        If date is missing, skips the entire instance.
        """
        date = eq5d.date
        if date is None:
            log.warning("Skipping EQ5D instance %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.visit_id_by_date.get(date)
        datetime_value = dt.datetime(date.year, date.month, date.day)

        for n in range(1, EQ5D.Q_COUNT + 1):
            level: int | None = getattr(eq5d, f"q{n}_code")
            if level is None:
                continue

            answer_concept = self.concepts.lookup_static(f"eq5d_q{n}_answer_code", str(level), domains={OmopDomain.MEASUREMENTS})
            if answer_concept is None:
                continue

            text: str | None = getattr(eq5d, f"q{n}")
            source_value = text if text is not None else str(level)
            rows.append(
                MeasurementRow(
                    measurement_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.EQ5D_COLLECTION,
                        str(eq5d.event_name),
                        str(date),
                        f"q{n}",
                    ),
                    person_id=person_id,
                    measurement_concept_id=int(answer_concept.concept_id),
                    measurement_date=date,
                    measurement_datetime=datetime_value,
                    measurement_type_concept_id=ecrf_concept,
                    value_as_number=float(level),
                    visit_occurrence_id=visit_occurrence_id,
                    measurement_source_value=source_value[:50],
                )
            )

        # VAS row
        vas = eq5d.qol_metric
        if vas is not None:
            vas_concept = self.concepts.lookup_structural("eq5d_qol_score", domains={OmopDomain.MEASUREMENTS})
            if vas_concept is not None:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.EQ5D_COLLECTION,
                            str(eq5d.event_name),
                            str(date),
                            EQ5D.Fields.QOL_METRIC,
                        ),
                        person_id=person_id,
                        measurement_concept_id=int(vas_concept.concept_id),
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        value_as_number=float(vas),
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=str(vas)[:50],
                    )
                )

        return rows

    def _build_adverse_event_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        return []
