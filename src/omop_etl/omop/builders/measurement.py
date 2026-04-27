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
    """Builds measurement rows from ECOG, biomarkers, tumor assessments, C30/EQ5D PROs,
    and adverse-event terms that map to the Measurement domain.

    CDM 5.4 policy:
    - `measurement_concept_id` must be in measurement domain. Rows are skipped if no
      Measurement concept is available.
    - `value_as_concept_id` is Meas Value domain: NULL when the source has no
      categorical result; 0 when there is a categorical result but no mapping;
      otherwise the mapped concept id.
    - `value_as_number` is the numeric result where the source provides one.
    """

    table_name: ClassVar[str] = "measurement"

    def build(self, ctx: BuildContext) -> list[MeasurementRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        rows: list[MeasurementRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        measurement_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        # bind property accessors to locals so the type checker can narrow `None`
        # across the call site and the inner method.
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
        return []

    def _build_tumor_assessment_baseline_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        baseline: TumorAssessmentBaseline,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        # baseline nadir is not emitted, at baseline nadir is the starting size
        # off-target lesion count belongs in observation, has no measurement concept for lesion count
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
        date = tumor_assessments.date
        if date is None:
            log.warning("Skipping tumor assessment %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.visit_id_by_date.get(date)
        datetime_value = dt.datetime(date.year, date.month, date.day)

        # absolute target-lesion size set by harmonizer: baseline * (1 + change_from_baseline)
        # skipped when size or structural concept is unavailable,
        # response rows on the same instance emits rows independantly (below)
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

        # response rows: each scale has its own precoordinated Measurement concept that
        # encodes scale + answer (e.g. "RECIST 1.1: stable disease"),
        # the static concept is the measurement_concept_id and value_as_concept_id stays NULL
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
        # EORTC QLQ-C30: 30 questions, one row per question.
        # measurement_concept_id = structural `c30_q{N}` (precoordinated, names the question)
        # value_as_concept_id = static answer concept; Q1–Q28 share `c30_answer_code` (1–4),
        #   Q29–Q30 use `c30_global_answer_code` (1–7) — different scales.
        # value_as_number = the raw level code; measurement_source_value = the answer text.
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
                # CDM: skip when no Measurement-domain concept is available.
                continue

            # Q29 (overall health) and Q30 (overall QoL) use the 1–7 global scale; Q1–Q28 use 1–4.
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
        return []

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
