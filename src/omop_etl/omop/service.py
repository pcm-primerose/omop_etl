from collections.abc import Sequence

from omop_etl.harmonization.models.patient import Patient
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.base import OmopBuilder
from omop_etl.omop.builders.drug_exposure_builder import DrugExposureBuilder
from omop_etl.omop.builders.person_builder import PersonBuilder
from omop_etl.omop.builders.observation_period_builder import ObservationPeriodBuilder
from omop_etl.omop.builders.cdm_source_builder import CdmSourceBuilder
from omop_etl.omop.builders.visit_occurrence_builder import VisitOccurrenceBuilder
from omop_etl.omop.core.id_generator import sha1_bigint
from omop_etl.omop.models.tables import OmopTables


class OmopService:
    """
    Entry point for building OMOP CDM rows from patient data.

    Takes mapped patient data and constructs OMOP-compliant rows using
    table-centric builders.
    """

    def __init__(self, concepts: ConceptLookupService):
        self._concepts = concepts
        self._builders: list[OmopBuilder] = [
            PersonBuilder(concepts),
            ObservationPeriodBuilder(concepts),
            VisitOccurrenceBuilder(concepts),
            DrugExposureBuilder(concepts),
        ]

    def build(self, patients: Sequence[Patient]) -> OmopTables:
        """
        Build all OMOP tables from patient data.
        """
        tables = OmopTables()

        # temporarily limit the number of patients processed for debug # remove once testing is finished
        for patient in patients[:5]:  # remove once testing is finished
            # for patient in patients: # uncomment when testing is finished
            person_id = sha1_bigint("person", patient.patient_id)
            for builder in self._builders:
                rows = builder.build(patient, person_id)
                # debug: print builder name and row count # To remove once my testing is finished
                print(
                    f"[DEBUG] {builder.__class__.__name__} produced {len(rows)} rows for patient {patient.patient_id}"
                )  # To remove once my testing is finished
                if builder.__class__.__name__ == "DrugExposureBuilder":  # To remove once my testing is finished
                    print(f"[DEBUG] sample drug rows: {[getattr(r, 'drug_source_value', None) for r in rows][:5]}")  # To remove once my testing is finished
                tables.extend(builder.table_name, rows)

        # singleton metadata row
        tables.add("cdm_source", CdmSourceBuilder(self._concepts).build())

        return tables
