import datetime as dt
from logging import getLogger
from typing import ClassVar

from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.harmonization.models.domain.biomarkers import Biomarkers
from omop_etl.harmonization.models.domain.c30 import C30
from omop_etl.harmonization.models.domain.ecog_baseline import EcogBaseline
from omop_etl.harmonization.models.domain.eq5d import EQ5D
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.tumor_assessment import TumorAssessment
from omop_etl.harmonization.models.domain.tumor_assessment_baseline import TumorAssessmentBaseline
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import (
    BuildResult,
    LinkTarget,
    SourceReference,
)
from omop_etl.omop.models.rows import MeasurementRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class MeasurementBuilder(OmopBuilder[MeasurementRow]):
    """
    Builds measurement rows from ECOG, tumor-assessment baseline, tumor assessment
    instances, C30/EQ5D, biomarkers, and free-text terms (medical history, AE)
    that map to the Measurement domain.

    CDM 5.4 policy:
    - measurement_concept_id: must be in measurement domain. Rows are skipped if no
      Measurement concept is available.
    - value_as_concept_id: in Meas Value domain. NULL when the source has no
      categorical result, 0 when there is a categorical result but no mapping,
      otherwise the mapped concept id.
    - value_as_number: the numeric result where the source provides one.
    - visit_occurrence_id: resolved by date via `ctx.resolve_visit_id(date)`
      (visit_occurrence builder must run before this).
    - measurement_event_id: linked to the patient's primary cancer
      condition_occurrence row(s) for lesion-size and biomarker rows, per
      oncology CDM guideline. Resolved via `_resolve_link_targets` over the
      tumor_type SourceReference; multi-concept tumor expands lesion/biomarker
      rows 1:N with target row_id in the PK hash. Emit-anyway policy enforced
      at the callsite: rows are emitted even when no primary cancer row is
      published.
    - meas_event_field_concept_id: cdm_field concept for the FK column
      (`condition_occurrence.condition_occurrence_id` by default).
    """

    table_name: ClassVar[str] = OmopTables.MEASUREMENT

    def build(self, ctx: BuildContext) -> BuildResult[MeasurementRow]:
        patient = ctx.patient
        person_id = ctx.person_id

        rows: list[MeasurementRow] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        measurement_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        ecog_baseline = patient.ecog_baseline
        if ecog_baseline is not None:
            rows.extend(self._build_ecog_rows(patient, person_id, measurement_type_concept_id, ecog_baseline, ctx))

        ta_baseline = patient.tumor_assessment_baseline
        if ta_baseline is not None:
            rows.extend(self._build_tumor_assessment_baseline_rows(patient, person_id, measurement_type_concept_id, ta_baseline, ctx))

        for idx, ta in enumerate(patient.tumor_assessments):
            rows.extend(self._build_tumor_assessment_rows(patient, person_id, measurement_type_concept_id, ta, idx, ctx))

        for idx, c30 in enumerate(patient.c30_collection):
            rows.extend(self._build_c30_rows(patient, person_id, measurement_type_concept_id, c30, idx, ctx))

        for idx, eq5d in enumerate(patient.eq5d_collection):
            rows.extend(self._build_eq5d_rows(patient, person_id, measurement_type_concept_id, eq5d, idx, ctx))

        biomarkers = patient.biomarkers
        if biomarkers is not None:
            rows.extend(self._build_biomarker_rows(patient, person_id, measurement_type_concept_id, biomarkers, ctx))

        for idx, mh in enumerate(patient.medical_histories):
            rows.extend(self._build_medical_history_rows(patient, person_id, measurement_type_concept_id, mh, idx, ctx))

        for idx, ae in enumerate(patient.adverse_events):
            rows.extend(self._build_adverse_event_rows(patient, person_id, measurement_type_concept_id, ae, idx, ctx))

        return BuildResult(rows=tuple(rows))

    def _primary_cancer_link_targets(self, patient: Patient, ctx: BuildContext) -> tuple[LinkTarget, ...]:
        """Resolve LinkTargets for the patient's primary cancer
        condition_occurrence row(s). Returns () when no tumor singleton or no
        condition row was published; callers decide whether to emit an
        unlinked row or skip."""
        tumor = patient.tumor_type
        if tumor is None:
            return ()
        return self._resolve_link_targets(
            ctx,
            target_table=OmopTables.CONDITION_OCCURRENCE,
            source_ref=SourceReference(
                patient.patient_id,
                Patient.Singletons.TUMOR_TYPE,
                tumor.natural_key(),
            ),
        )

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
        Skipped if date, grade, or the ecog structural concept is missing.

        - measurement_concept_id: structural `ecog` concept (Measurement domain).
        - value_as_concept_id: static `ecog_code` mapping for the grade. Per CDM 5.4,
          set to 0 when the grade has no static mapping (categorical result present
          but unmapped).
        - value_as_number: raw grade.
        """
        date = ecog_baseline.date
        grade = ecog_baseline.grade
        if date is None:
            log.warning("Skipping ECOG for %s: missing date", patient.patient_id)
            return []
        if grade is None:
            log.warning("Skipping ECOG for %s: missing grade", patient.patient_id)
            return []

        ecog_test = self.concepts.lookup_structural("ecog", domains={OmopDomain.MEASUREMENTS})
        if ecog_test is None:
            log.warning("No ECOG structural concept found")
            return []

        ecog_answer = self.concepts.lookup_static(
            "ecog_code",
            str(grade),
            domains={OmopDomain.MEAS_VALUE},
        )
        value_as_concept_id = int(ecog_answer.concept_id) if ecog_answer else 0

        row_id = self.generate_row_id(
            patient.patient_id,
            Patient.Singletons.ECOG_BASELINE,
            *ecog_baseline.natural_key(),
        )
        return [
            MeasurementRow(
                measurement_id=row_id,
                person_id=person_id,
                measurement_concept_id=ecog_test.concept_id,
                measurement_date=date,
                measurement_type_concept_id=ecrf_concept,
                measurement_datetime=dt.datetime(date.year, date.month, date.day),
                value_as_number=float(grade),
                value_as_concept_id=value_as_concept_id,
                visit_occurrence_id=ctx.resolve_visit_id(date),
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
        biomarker field in preference chain. Cohort fields are inclusion criteria
        and are preferred when mapping.

        measurement_concept_id is each mapped semantic concept; can return
        multiple rows. measurement_source_value is the raw biomarker value.
        value_as_number and value_as_concept_id are not set.

        FK-linked to the primary cancer condition_occurrence row(s); when the
        tumor produces N condition rows, each biomarker concept expands to N
        rows (cross-product) with the target row_id in the PK. Emit-anyway:
        when no linkage, biomarker rows are still emitted unlinked.

        Skipped if date is missing or if no field in the chain maps to a
        measurement concept.
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
            visit_occurrence_id = ctx.resolve_visit_id(date)
            targets = self._primary_cancer_link_targets(patient, ctx)

            def row(
                concept_id: int,
                source_value_local: str,
                event_id: int | None,
                field_concept_id: int | None,
            ) -> MeasurementRow:
                return MeasurementRow(
                    measurement_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Singletons.BIOMARKERS,
                        field_name,
                        *biomarkers.natural_key(),
                        concept_id,
                        event_id,
                    ),
                    person_id=person_id,
                    measurement_concept_id=concept_id,
                    measurement_date=date,
                    measurement_datetime=datetime_value,
                    measurement_type_concept_id=ecrf_concept,
                    visit_occurrence_id=visit_occurrence_id,
                    measurement_source_value=source_value_local[:50],
                    measurement_event_id=event_id,
                    meas_event_field_concept_id=field_concept_id,
                )

            rows: list[MeasurementRow] = []
            for concept in matches:
                if not targets:
                    rows.append(row(concept.concept_id, source_value, None, None))
                else:
                    rows.extend(row(concept.concept_id, source_value, t.event_id, t.field_concept_id) for t in targets)
            return rows

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
        Emits 0 to N rows from TumorAssessmentBaseline (N = number of primary
        cancer condition rows, 1 in the common case). measurement_concept_id is
        the structural lookup for 'lesion_size', value_as_number is the size at
        baseline. FK-linked to primary cancer condition_occurrence row(s);
        multi-concept tumor expands 1:N with target row_id in the PK.
        Emit-anyway when no linkage.

        Skipped if target_lesion_size, target_lesion_measurement_date, or the
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

        unit = self.concepts.lookup_structural("millimeter", domains={"Unit"})
        unit_concept_id = unit.concept_id if unit else None
        visit_occurrence_id = ctx.resolve_visit_id(date)

        def row(event_id: int | None, field_concept_id: int | None) -> MeasurementRow:
            return MeasurementRow(
                measurement_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.TUMOR_ASSESSMENT_BASELINE,
                    TumorAssessmentBaseline.Fields.TARGET_LESION_SIZE,
                    *baseline.natural_key(),
                    event_id,
                ),
                person_id=person_id,
                measurement_concept_id=lesion.concept_id,
                measurement_date=date,
                measurement_datetime=dt.datetime(date.year, date.month, date.day),
                measurement_type_concept_id=ecrf_concept,
                value_as_number=float(size),
                unit_concept_id=unit_concept_id,
                visit_occurrence_id=visit_occurrence_id,
                measurement_source_value=str(size)[0:50],
                measurement_event_id=event_id,
                meas_event_field_concept_id=field_concept_id,
            )

        targets = self._primary_cancer_link_targets(patient, ctx)
        if not targets:
            return [row(event_id=None, field_concept_id=None)]
        return [row(t.event_id, t.field_concept_id) for t in targets]

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
        Emits 0 to (N + 3*2) rows per TumorAssessment instance.
        Up to N rows for the absolute target_lesion_size when set (N = number
        of primary cancer condition rows, 1 in the common case), using the
        structural 'lesion_size' lookup as the measurement_concept_id and
        storing the lesion size in value_as_number. FK-linked to primary
        cancer condition_occurrence row(s); emit-anyway when no linkage.

        Up to 3 additional rows, one per populated response scale (RECIST,
        iRECIST, RANO). The responses use precoordinated pair concepts, so the
        measurement_concept_id stores both scale and answer (same pattern as
        EQ5D), value_as_concept_id stays NULL. No FK linkage on response rows.

        If response scale is Not Evaluable, use separate branch with structural
        lookup for measurement concept id and value as concept id is then the
        NE Meas Value response concept, so any Meas Value concept for this
        lookup key means assessment was Not Evaluable.

        If date is missing the instance is skipped entirely and no rows are
        emitted.
        """
        date = tumor_assessments.date
        if date is None:
            log.warning("Skipping tumor assessment %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.resolve_visit_id(date)
        datetime_value = dt.datetime(date.year, date.month, date.day)

        # absolute target-lesion size, FK-linked to primary cancer
        size = tumor_assessments.target_lesion_size
        if size is not None:
            lesion = self.concepts.lookup_structural("lesion_size", domains={OmopDomain.MEASUREMENTS})
            if lesion is not None:
                unit = self.concepts.lookup_structural("millimeter", domains={"Unit"})
                unit_concept_id = unit.concept_id if unit else None

                def lesion_row(event_id: int | None, field_concept_id: int | None) -> MeasurementRow:
                    return MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            TumorAssessment.Fields.TARGET_LESION_SIZE,
                            *tumor_assessments.natural_key(),
                            event_id,
                        ),
                        person_id=person_id,
                        measurement_concept_id=lesion.concept_id,
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        value_as_number=size,
                        unit_concept_id=unit_concept_id,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=str(size)[:50],
                        measurement_event_id=event_id,
                        meas_event_field_concept_id=field_concept_id,
                    )

                targets = self._primary_cancer_link_targets(patient, ctx)
                if not targets:
                    rows.append(lesion_row(event_id=None, field_concept_id=None))
                else:
                    rows.extend(lesion_row(t.event_id, t.field_concept_id) for t in targets)

        # tumor assessment response rows (no FK linkage)
        recist = tumor_assessments.recist_response
        if recist is not None:
            recist_response_concept = self.concepts.lookup_static("response_recist", recist, domains={OmopDomain.MEASUREMENTS})
            recist_not_evaluable_concept = self.concepts.lookup_static("response_recist", recist, domains={OmopDomain.MEAS_VALUE})

            if recist_response_concept is None and recist_not_evaluable_concept is None:
                log.warning("No response_recist mapping for %r (patient %s)", recist, patient.patient_id)

            if recist_response_concept:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            TumorAssessment.Fields.RECIST_RESPONSE,
                            *tumor_assessments.natural_key(),
                        ),
                        person_id=person_id,
                        measurement_concept_id=recist_response_concept.concept_id,
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=recist[:50],
                    )
                )

            if recist_not_evaluable_concept:
                recist_concept = self.concepts.lookup_structural("response_recist")
                if recist_concept is None:
                    log.warning("No structural concept found for response_recist")
                else:
                    rows.append(
                        MeasurementRow(
                            measurement_id=self.generate_row_id(
                                patient.patient_id,
                                Patient.Collections.TUMOR_ASSESSMENTS,
                                TumorAssessment.Fields.RECIST_RESPONSE,
                                *tumor_assessments.natural_key(),
                            ),
                            person_id=person_id,
                            measurement_concept_id=recist_concept.concept_id,
                            value_as_concept_id=recist_not_evaluable_concept.concept_id,
                            measurement_date=date,
                            measurement_datetime=datetime_value,
                            measurement_type_concept_id=ecrf_concept,
                            visit_occurrence_id=visit_occurrence_id,
                            measurement_source_value=recist[:50],
                        )
                    )

        irecist = tumor_assessments.irecist_response
        if irecist is not None:
            irecist_response_concept = self.concepts.lookup_static("response_irecist", irecist, domains={OmopDomain.MEASUREMENTS})
            irecist_not_evaluable_concept = self.concepts.lookup_static("response_irecist", irecist, domains={OmopDomain.MEAS_VALUE})

            if irecist_response_concept is None and irecist_not_evaluable_concept is None:
                log.warning("No response_irecist mapping for %r (patient %s)", irecist, patient.patient_id)

            if irecist_response_concept:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            TumorAssessment.Fields.IRECIST_RESPONSE,
                            *tumor_assessments.natural_key(),
                        ),
                        person_id=person_id,
                        measurement_concept_id=irecist_response_concept.concept_id,
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=irecist[:50],
                    )
                )

            if irecist_not_evaluable_concept:
                irecist_concept = self.concepts.lookup_structural("response_irecist")
                if irecist_concept is None:
                    log.warning("No structural concept found for response_irecist")
                else:
                    rows.append(
                        MeasurementRow(
                            measurement_id=self.generate_row_id(
                                patient.patient_id,
                                Patient.Collections.TUMOR_ASSESSMENTS,
                                TumorAssessment.Fields.IRECIST_RESPONSE,
                                *tumor_assessments.natural_key(),
                            ),
                            person_id=person_id,
                            measurement_concept_id=irecist_concept.concept_id,
                            value_as_concept_id=irecist_not_evaluable_concept.concept_id,
                            measurement_date=date,
                            measurement_datetime=datetime_value,
                            measurement_type_concept_id=ecrf_concept,
                            visit_occurrence_id=visit_occurrence_id,
                            measurement_source_value=irecist[:50],
                        )
                    )

        rano = tumor_assessments.rano_response
        if rano is not None:
            rano_response_concept = self.concepts.lookup_static("response_rano", rano, domains={OmopDomain.MEASUREMENTS})
            rano_not_evaluable_concept = self.concepts.lookup_static("response_rano", rano, domains={OmopDomain.MEAS_VALUE})

            if rano_response_concept is None and rano_not_evaluable_concept is None:
                log.warning("No response_rano mapping for %r (patient %s)", rano, patient.patient_id)

            if rano_response_concept:
                rows.append(
                    MeasurementRow(
                        measurement_id=self.generate_row_id(
                            patient.patient_id,
                            Patient.Collections.TUMOR_ASSESSMENTS,
                            TumorAssessment.Fields.RANO_RESPONSE,
                            *tumor_assessments.natural_key(),
                        ),
                        person_id=person_id,
                        measurement_concept_id=rano_response_concept.concept_id,
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=rano[:50],
                    )
                )

            if rano_not_evaluable_concept:
                rano_concept = self.concepts.lookup_structural("response_rano")
                if rano_concept is None:
                    log.warning("No structural concept found for response_rano")
                else:
                    rows.append(
                        MeasurementRow(
                            measurement_id=self.generate_row_id(
                                patient.patient_id,
                                Patient.Collections.TUMOR_ASSESSMENTS,
                                TumorAssessment.Fields.RANO_RESPONSE,
                                *tumor_assessments.natural_key(),
                            ),
                            person_id=person_id,
                            measurement_concept_id=rano_concept.concept_id,
                            value_as_concept_id=rano_not_evaluable_concept.concept_id,
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
        visit_occurrence_id = ctx.resolve_visit_id(date)
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
                answer_concept = self.concepts.lookup_static(answer_set, str(answer_level), domains={OmopDomain.MEAS_VALUE})
                value_as_concept_id = int(answer_concept.concept_id) if answer_concept else 0

            source_value = answer_text if answer_text is not None else str(answer_level)
            rows.append(
                MeasurementRow(
                    measurement_id=self.generate_row_id(
                        patient.patient_id,
                        Patient.Collections.C30_COLLECTION,
                        test_concept.concept_id,
                        *c30.natural_key(),
                        f"q{n}",
                    ),
                    person_id=person_id,
                    measurement_concept_id=test_concept.concept_id,
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
        EQ5D: 5 dimension questions + VAS QoL score. Per EQ5D instance, emits 0–6 rows.

        Each dimension uses a precoordinated static concept (encodes question and
        answer together) as measurement_concept_id, value_as_concept_id stays NULL,
        value_as_number is the level. A dimension is skipped if its level is unset,
        there is no generic structural fallback for an EQ5D dimension.

        The VAS uses the eq5d_qol_score structural concept as measurement_concept_id
        with the raw VAS in value_as_number.

        If date is missing, the entire instance is skipped.
        """
        date = eq5d.date
        if date is None:
            log.warning("Skipping EQ5D instance %d for %s: missing date", index, patient.patient_id)
            return []

        rows: list[MeasurementRow] = []
        visit_occurrence_id = ctx.resolve_visit_id(date)
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
                        *eq5d.natural_key(),
                        f"q{n}",
                    ),
                    person_id=person_id,
                    measurement_concept_id=answer_concept.concept_id,
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
                            *eq5d.natural_key(),
                            vas_concept.concept_id,
                            EQ5D.Fields.QOL_METRIC,
                        ),
                        person_id=person_id,
                        measurement_concept_id=vas_concept.concept_id,
                        measurement_date=date,
                        measurement_datetime=datetime_value,
                        measurement_type_concept_id=ecrf_concept,
                        value_as_number=float(vas),
                        visit_occurrence_id=visit_occurrence_id,
                        measurement_source_value=str(vas)[:50],
                    )
                )

        return rows

    def _build_medical_history_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        mh: MedicalHistory,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        Emits 0+ rows from a single MedicalHistory. Mirrors the adverse-event pattern:
        the term is free text and the semantic mapping is configured to query both
        Measurement and Meas Value domains. A past finding like "decreased hemoglobin"
        maps to a Measurement attribute ("Hemoglobin measurement") plus a Meas Value
        qualifier ("Decreased").

        Behavior:
        - Skipped if `start_date` is missing.
        - Lookup is constrained to {Measurement, Meas Value} domains.
        - If no Measurement-domain concept is matched, no rows are emitted.
        - One row per (Measurement, Meas Value) pair. When no Meas Value matches,
          emits one row per Measurement with `value_as_concept_id=0`.
        - `measurement_source_value` is the raw term.

        Multi-Meas-Value cardinality is logged so a real mapping mistake doesn't
        silently inflate the table.
        """
        date = mh.start_date
        if date is None:
            log.warning("Skipping medical history %d for %s: missing start_date", index, patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            index,
            domains={OmopDomain.MEASUREMENTS, OmopDomain.MEAS_VALUE},
        )
        if not matches:
            return []

        measurement_concepts = [c for c in matches if c.domain_id == OmopDomain.MEASUREMENTS.value]
        meas_value_concepts = [c for c in matches if c.domain_id == OmopDomain.MEAS_VALUE.value]

        if not measurement_concepts:
            return []

        if len(meas_value_concepts) > 1:
            log.warning(
                "Medical history %d for %s mapped to %d Meas Value concepts (typical: 1). Emitting cross-product. concepts=%s",
                index,
                patient.patient_id,
                len(meas_value_concepts),
                [c.concept_id for c in meas_value_concepts],
            )

        term = mh.term
        datetime_value = dt.datetime(date.year, date.month, date.day)
        visit_occurrence_id = ctx.resolve_visit_id(date)
        source_value = term[:50] if term is not None else None

        qualifier_ids: list[int] = [int(c.concept_id) for c in meas_value_concepts] if meas_value_concepts else [0]

        return [
            MeasurementRow(
                # same concept id produces multiple rows, so need concept_id and q_id in UID
                measurement_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.MEDICAL_HISTORIES,
                    *mh.natural_key(),
                    q_id,
                    m_concept.concept_id,
                ),
                person_id=person_id,
                measurement_concept_id=m_concept.concept_id,
                measurement_date=date,
                measurement_datetime=datetime_value,
                measurement_type_concept_id=ecrf_concept,
                value_as_concept_id=q_id,
                visit_occurrence_id=visit_occurrence_id,
                measurement_source_value=source_value,
            )
            for m_concept in measurement_concepts
            for q_id in qualifier_ids
        ]

    def _build_adverse_event_rows(
        self,
        patient: Patient,
        person_id: int,
        ecrf_concept: int,
        ae: AdverseEvent,
        index: int,
        ctx: BuildContext,
    ) -> list[MeasurementRow]:
        """
        Emits 0-N rows from AdverseEvent.
        Some values in AdverseEvent maps to the measurement domain,
        e.g. "decreased platelet count".

        measurement_concept_id is here the attribute mapping from the semantic mapping,
        e.g. "platelet count", where value_as_concept_id is the meas-value qualifier,
        e.g. "decreased". Both are in semantic mapping as these fields are free-text.
        value_as_number is the adverse event grade, measurement_source_value is the adverse event term.

        Skipped if start_date is missing or if no measurement domain found from semantic query.
        If measurement concept is found but no meas value qualifier, measurement_concept_id is
        populated but value_as_concept_id is set to 0.
        """
        date = ae.start_date
        if date is None:
            log.warning("Skipping AE %d for %s: missing start_date", index, patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            patient.patient_id,
            (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
            index,
            domains={OmopDomain.MEASUREMENTS, OmopDomain.MEAS_VALUE},
        )
        if not matches:
            return []

        measurement_concepts = [c for c in matches if c.domain_id == OmopDomain.MEASUREMENTS.value]
        meas_value_concepts = [c for c in matches if c.domain_id == OmopDomain.MEAS_VALUE.value]

        if not measurement_concepts:
            return []

        # mapping policy: one measurement attribute paired with one meas value
        # qualifier per AE term. If mapper returns multiple qualifiers (or multiple
        # attributes), trust it and emit the full product, semantic mapping is singular source of truth.
        if len(meas_value_concepts) > 1:
            log.warning(
                "AE %d for %s mapped to %d Meas Value concepts (typical: 1). Emitting cross-product. concepts=%s",
                index,
                patient.patient_id,
                len(meas_value_concepts),
                [c.concept_id for c in meas_value_concepts],
            )

        grade = ae.grade
        term = ae.term
        datetime_value = dt.datetime(date.year, date.month, date.day)
        visit_occurrence_id = ctx.resolve_visit_id(date)
        value_as_number = float(grade) if grade is not None else None
        source_value = term[:50] if term is not None else None

        # no meas value match yields single qualifier slot of 0 (categorical present in source
        # but unmapped, see CDM 5.4 measurement value_as_concept_id rules).
        qualifier_ids: list[int] = [int(c.concept_id) for c in meas_value_concepts] if meas_value_concepts else [0]

        return [
            MeasurementRow(
                # same concept id produces multiple rows, so need concept_id and q_id in UID
                measurement_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    q_id,
                    m_concept.concept_id,
                ),
                person_id=person_id,
                measurement_concept_id=m_concept.concept_id,
                measurement_date=date,
                measurement_datetime=datetime_value,
                measurement_type_concept_id=ecrf_concept,
                value_as_number=value_as_number,
                value_as_concept_id=q_id,
                visit_occurrence_id=visit_occurrence_id,
                measurement_source_value=source_value,
            )
            for m_concept in measurement_concepts
            for q_id in qualifier_ids
        ]
