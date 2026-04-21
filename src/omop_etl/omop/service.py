from collections.abc import Sequence

from omop_etl.harmonization.models.patient import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.base import OmopBuilder, BuildContext
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.builders.person import PersonBuilder
from omop_etl.omop.builders.observation_period import ObservationPeriodBuilder
from omop_etl.omop.builders.cdm_source import CdmSourceBuilder
from omop_etl.omop.builders.procedure_occurrence import ProcedureOccurrenceBuilder
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.builders.drug_exposure import DrugExposureBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.models.tables import OmopTables


class OmopService:
    """
    Entry point for building OMOP CDM rows from patient data.

    Takes mapped patient data and constructs OMOP-compliant rows using
    table-centric builders. Builders emitting context consumed downstream
    (like visit_occurrence) are built first.
    """

    def __init__(self, concepts: ConceptLookupService):
        self._concepts = concepts
        self._visit_builder = VisitOccurrenceBuilder(concepts)
        self._builders: list[OmopBuilder] = [
            PersonBuilder(concepts),
            ObservationPeriodBuilder(concepts),
            DrugExposureBuilder(concepts),
            ConditionOccurrenceBuilder(concepts),
            ProcedureOccurrenceBuilder(concepts),
        ]

    def build(self, patients: Sequence[Patient]) -> OmopTables:
        """
        Build all OMOP tables from patient data.
        """
        tables = OmopTables()

        for patient in patients:
            person_id = sha1_bigint("person", patient.patient_id)
            ctx = BuildContext(
                patient=patient,
                person_id=person_id,
            )

            # build visits first to populate ctx.visit_id_by_date for downstream builders
            visit_rows = self._visit_builder.build(ctx)
            for row in visit_rows:
                ctx.visit_id_by_date[row.visit_start_date] = row.visit_occurrence_id
            tables.extend(self._visit_builder.table_name, visit_rows)

            # build remaining tables
            for builder in self._builders:
                rows = builder.build(ctx)
                tables.extend(builder.table_name, rows)

        # singleton metadata row
        tables.add("cdm_source", CdmSourceBuilder(self._concepts).build())

        return tables
