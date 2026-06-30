from collections.abc import Sequence
from logging import getLogger

from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.harmonization.models.patient import Patient
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import LocationRow
from omop_etl.omop.models.tables import OmopTables

log = getLogger(__name__)


class LocationBuilder:
    """
    Builds the location reference rows: one row per distinct
    country a patient's trial maps to, using the `trial_country` static map
    (trial_id to Geography country concept).

    Cross-patient (a country is shared across a trial's patients), so it runs once
    over the full patient set after the per-patient loop, like CohortDefinitionBuilder.
    `location_id` is content-addressed (`row_id(LOCATION, country_concept_id)`), the
    same hash PersonBuilder computes for `person.location_id`, so they join.

    Trials with no `trial_country` mapping contribute no location (and their
    patients get `location_id = None`).
    """

    def __init__(self, concepts: ConceptLookupService):
        self.concepts = concepts

    def build(self, patients: Sequence[Patient]) -> list[LocationRow]:
        rows: dict[int, LocationRow] = {}
        for patient in patients:
            country = self.concepts.resolve("trial_country", patient.trial_id, domains={"Geography"})
            if not country:
                continue
            concept = country[0]
            location_id = row_id(OmopTables.LOCATION, concept.concept_id)
            if location_id in rows:
                continue
            rows[location_id] = LocationRow(
                location_id=location_id,
                country_concept_id=concept.concept_id,
                country_source_value=concept.concept_name[:80] if concept.concept_name else None,
            )
        return list(rows.values())
