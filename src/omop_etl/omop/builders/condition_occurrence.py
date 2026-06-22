from typing import ClassVar
from logging import getLogger

from omop_etl.harmonization.models.patient import Patient
from omop_etl.harmonization.models.domain.tumor_type import TumorType
from omop_etl.harmonization.models.domain.medical_history import MedicalHistory
from omop_etl.harmonization.models.domain.adverse_event import AdverseEvent
from omop_etl.omop.models.rows import ConditionOccurrenceRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.core.linkage import (
    BuildResult,
    OmopRowReference,
    RowPublication,
    SourceReference,
)

log = getLogger(__name__)


class ConditionOccurrenceBuilder(OmopBuilder[ConditionOccurrenceRow]):
    """Builds condition_occurrence rows from tumor type, medical histories, and adverse events.

    CDM policy: only records whose source values map to concepts with a domain of
    "Condition" should go in this table (CDM 5.4 condition_occurrence ETL conventions).
    No rows emitted for unmapped source values.

    Publishes emitted rows via `BuildResult.publications` so downstream builders
    (e.g. Observation AE modifiers) can resolve FK targets by `SourceRef`.
    Multi-concept source instances publish all emitted rows.
    """

    table_name: ClassVar[str] = OmopTables.CONDITION_OCCURRENCE

    def build(self, ctx: BuildContext) -> BuildResult[ConditionOccurrenceRow]:
        patient = ctx.patient
        person_id = ctx.person_id
        rows: list[ConditionOccurrenceRow] = []
        publications: list[RowPublication] = []
        ecrf = self.concepts.lookup_structural("ecrf", domains={"Type Concept"})
        condition_type_concept_id = int(ecrf.concept_id) if ecrf else 0

        tumor_type = patient.tumor_type
        if tumor_type is not None:
            tumor_rows = self._build_tumor_type_rows(patient, person_id, tumor_type, condition_type_concept_id)
            if tumor_rows:
                publications.append(
                    self._row_publication(
                        patient.patient_id,
                        Patient.Singletons.TUMOR_TYPE,
                        tumor_type.natural_key(),
                        tumor_rows,
                    )
                )
            rows.extend(tumor_rows)

        for idx, mh in enumerate(patient.medical_histories):
            rows.extend(self._build_medical_history_rows(patient, person_id, mh, idx, condition_type_concept_id))

        for idx, ae in enumerate(patient.adverse_events):
            ae_rows = self._build_adverse_event_rows(patient, person_id, ae, idx, condition_type_concept_id)
            if ae_rows:
                publications.append(
                    self._row_publication(
                        patient.patient_id,
                        Patient.Collections.ADVERSE_EVENTS,
                        ae.natural_key(),
                        ae_rows,
                    )
                )
            rows.extend(ae_rows)

        return BuildResult(rows=tuple(rows), publications=tuple(publications))

    @staticmethod
    def _row_publication(
        patient_id: str,
        source_kind: str,
        natural_key: tuple,
        condition_rows: list[ConditionOccurrenceRow],
    ) -> RowPublication:
        """
        Wrap a list of condition_occurrence rows as a RowPublication anchored
        on (patient_id, source_kind, natural_key). publish_rows sorts internally
        by (primary_concept_id, row_id) for deterministic modifier expansion.
        """
        return RowPublication(
            target_table=OmopTables.CONDITION_OCCURRENCE,
            source_ref=SourceReference(patient_id, source_kind, natural_key),
            rows=tuple(
                OmopRowReference(
                    table=OmopTables.CONDITION_OCCURRENCE,
                    row_id=row.condition_occurrence_id,
                    primary_concept_id=row.condition_concept_id,
                )
                for row in condition_rows
            ),
        )

    def _build_tumor_type_rows(
        self,
        patient: Patient,
        person_id: int,
        tumor: TumorType | None,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        if tumor is None:
            return []

        if tumor.icd10_code:
            matches = self.concepts.lookup_semantic(
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.ICD10_CODE),
                tumor.icd10_code,
                domains={OmopDomain.CONDITION},
            )
            source_value = tumor.icd10_code

        elif tumor.main_tumor_type:
            matches = self.concepts.lookup_semantic(
                (Patient.Singletons.TUMOR_TYPE, TumorType.Fields.MAIN_TUMOR_TYPE),
                tumor.main_tumor_type,
                domains={OmopDomain.CONDITION},
            )
            source_value = tumor.main_tumor_type

        else:
            log.warning("Skipping tumor type for %s: no icd10_code or main_tumor_type", patient.patient_id)
            return []

        if not matches:
            return []

        date = tumor.date or patient.treatment_start_date
        if date is None:
            log.warning("Skipping tumor type for %s: no usable date", patient.patient_id)
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Singletons.TUMOR_TYPE,
                    *tumor.natural_key(),
                    concept.concept_id,
                ),
                person_id=person_id,
                condition_concept_id=concept.concept_id,
                condition_start_date=date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=source_value,
            )
            for concept in matches
        ]

    def _build_medical_history_rows(
        self,
        patient: Patient,
        person_id: int,
        mh: MedicalHistory,
        index: int,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        start_date = mh.start_date
        if start_date is None:
            log.warning("Skipping medical history %d for %s: missing start_date", index, patient.patient_id)
            return []

        sequence_id = mh.sequence_id if mh.sequence_id else None
        if not sequence_id:
            log.warning("Skipping medical history for %s: missing sequence_id", patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            (Patient.Collections.MEDICAL_HISTORIES, MedicalHistory.Fields.TERM),
            mh.term,
            domains={OmopDomain.CONDITION},
        )
        if not matches:
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.MEDICAL_HISTORIES,
                    *mh.natural_key(),
                    concept.concept_id,
                ),
                person_id=person_id,
                condition_concept_id=concept.concept_id,
                condition_start_date=start_date,
                condition_end_date=mh.end_date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=mh.term,
            )
            for concept in matches
        ]

    def _build_adverse_event_rows(
        self,
        patient: Patient,
        person_id: int,
        ae: AdverseEvent,
        index: int,
        condition_type_concept_id: int,
    ) -> list[ConditionOccurrenceRow]:
        start_date = ae.start_date
        term = ae.term

        if start_date is None:
            log.warning("Skipping adverse event %d for %s: missing start_date", index, patient.patient_id)
            return []

        matches = self.concepts.lookup_semantic(
            (Patient.Collections.ADVERSE_EVENTS, AdverseEvent.Fields.TERM),
            ae.term,
            domains={OmopDomain.CONDITION},
        )
        if not matches:
            return []

        return [
            ConditionOccurrenceRow(
                condition_occurrence_id=self.generate_row_id(
                    patient.patient_id,
                    Patient.Collections.ADVERSE_EVENTS,
                    *ae.natural_key(),
                    concept.concept_id,
                ),
                person_id=person_id,
                condition_concept_id=concept.concept_id,
                condition_start_date=start_date,
                condition_end_date=ae.end_date,
                condition_type_concept_id=condition_type_concept_id,
                condition_source_value=term[:50] if term is not None else None,
            )
            for concept in matches
        ]
