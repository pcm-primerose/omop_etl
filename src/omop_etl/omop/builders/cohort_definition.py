from collections.abc import Sequence
from logging import getLogger

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import CohortDefinitionRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.semantic_mapping.core.models import OmopDomain

log = getLogger(__name__)


class CohortDefinitionBuilder:
    """
    Builds the cohort_definition reference rows (OMOP RESULTS schema): one row
    per distinct normalized cohort name observed across all patients.

    Cross-patient (one patient does not know every arm), so it runs once over the
    full patient set after the per-patient loop, like CdmSourceBuilder but
    data-derived rather than a fixed singleton.

    Patients whose cohort did not normalize (normalized_name is None) contribute
    no definition, mirroring CohortBuilder which skips them.
    """

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    def build(self, patients: Sequence[Patient]) -> list[CohortDefinitionRow]:
        ecrf = self.concepts.resolve("ecrf", domains={OmopDomain.TYPE_CONCEPT})
        if not ecrf:
            log.warning("No ecrf concept for cohort definition type, setting definition_type_concept_id to 0")
        definition_type_concept_id = ecrf[0].concept_id if ecrf else 0

        subject = self.concepts.resolve("cohort_subject")
        if not subject:
            log.warning("No cohort_subject concept, setting subject_concept_id to 0")
        subject_concept_id = subject[0].concept_id if subject else 0

        # distinct cohort names keyed by cohort_definition_id,
        # first patient supplies the description fields
        rows: dict[int, CohortDefinitionRow] = {}
        for patient in patients:
            cohort = patient.cohort
            if cohort is None:
                continue
            normalized_name = cohort.normalized_name
            if normalized_name is None:
                continue
            definition_id = row_id(OmopTables.COHORT_DEFINITION, normalized_name)
            if definition_id in rows:
                continue
            rows[definition_id] = CohortDefinitionRow(
                cohort_definition_id=definition_id,
                cohort_definition_name=normalized_name[:255],
                definition_type_concept_id=definition_type_concept_id,
                subject_concept_id=subject_concept_id,
                # the raw source cohort string as provenance
                cohort_definition_description=cohort.raw_name,
            )

        return list(rows.values())
