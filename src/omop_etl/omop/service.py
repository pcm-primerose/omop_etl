from collections.abc import Sequence

from omop_etl.harmonization.models.patient import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.context import BuildContext
from omop_etl.omop.builders.condition_occurrence import ConditionOccurrenceBuilder
from omop_etl.omop.builders.measurement import MeasurementBuilder
from omop_etl.omop.builders.observation import ObservationBuilder
from omop_etl.omop.builders.person import PersonBuilder
from omop_etl.omop.builders.observation_period import ObservationPeriodBuilder
from omop_etl.omop.builders.cdm_source import CdmSourceBuilder
from omop_etl.omop.builders.procedure_occurrence import ProcedureOccurrenceBuilder
from omop_etl.omop.builders.visit_occurrence import VisitOccurrenceBuilder
from omop_etl.omop.builders.drug_exposure import DrugExposureBuilder
from omop_etl.omop.core.id_generator import sha256_bigint
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
        # Builder order matters:
        # builders whose publications are consumed downstream must run first.
        # VisitOccurrenceBuilder publishes the date-anchored visit map.
        # ConditionOccurrenceBuilder publishes AE and primary-cancer rows,
        # consumed by MeasurementBuilder and ObservationBuilder.
        self._builders: list[OmopBuilder] = [
            VisitOccurrenceBuilder(concepts),
            PersonBuilder(concepts),
            ObservationPeriodBuilder(concepts),
            DrugExposureBuilder(concepts),
            ConditionOccurrenceBuilder(concepts),
            ProcedureOccurrenceBuilder(concepts),
            MeasurementBuilder(concepts),
            ObservationBuilder(concepts),
        ]

    def build(self, patients: Sequence[Patient]) -> OmopTables:
        """
        Build all OMOP tables from patient data.
        """
        tables = OmopTables()

        for patient in patients:
            person_id = sha256_bigint("person", patient.patient_id)
            ctx = BuildContext(patient=patient, person_id=person_id)

            for builder in self._builders:
                rows = builder.build_and_populate(ctx)
                tables.extend(builder.table_name, list(rows))

        # singleton metadata row
        tables.add(OmopTables.CDM_SOURCE, CdmSourceBuilder(self._concepts).build())

        return tables
