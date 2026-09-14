import datetime as dt

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.dose_era import DerivedDose, derive_dose
from omop_etl.omop.models.rows import DrugExposureRow

D = dt.date

MILLIGRAM_CONCEPT_ID = 8576


def _milligram() -> MappedConcept:
    return MappedConcept(
        concept_id=MILLIGRAM_CONCEPT_ID,
        concept_code="mg",
        concept_name="milligram",
        domain_id="Unit",
        vocabulary_id="UCUM",
        validity="valid",
    )


def _concepts_with_unit_mapping() -> ConceptLookupService:
    static_index = {("unit", "mg (milligram)"): _milligram()}
    return ConceptLookupService(static_index, structural_index={})


def _exposure(
    quantity: float | None = 4350.0,
    days_supply: int | None = 29,
    dose_unit_source_value: str | None = "mg (milligram)",
) -> DrugExposureRow:
    return DrugExposureRow(
        drug_exposure_id=0,
        person_id=1,
        drug_concept_id=100,
        drug_exposure_start_date=D(2023, 1, 1),
        drug_exposure_end_date=D(2023, 1, 29),
        drug_type_concept_id=0,
        quantity=quantity,
        days_supply=days_supply,
        dose_unit_source_value=dose_unit_source_value,
    )


class TestDeriveDose:
    def test_valid_exposure_derives_daily_dose(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(quantity=4350.0, days_supply=29)

        dose = derive_dose(exposure, concepts)

        assert dose == DerivedDose(dose_value=150.0, unit_concept_id=MILLIGRAM_CONCEPT_ID)

    def test_missing_quantity_returns_none(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(quantity=None)

        assert derive_dose(exposure, concepts) is None

    def test_missing_days_supply_returns_none(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(days_supply=None)

        assert derive_dose(exposure, concepts) is None

    def test_zero_days_supply_returns_none(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(days_supply=0)

        assert derive_dose(exposure, concepts) is None

    def test_negative_days_supply_returns_none(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(days_supply=-5)

        assert derive_dose(exposure, concepts) is None

    def test_missing_dose_unit_returns_none(self):
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(dose_unit_source_value=None)

        assert derive_dose(exposure, concepts) is None

    def test_unresolvable_dose_unit_returns_none(self):
        # "unit" value_set exists but doesn't have this particular source text
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(dose_unit_source_value="some unrecognized unit string")

        assert derive_dose(exposure, concepts) is None

    def test_never_fabricates_a_value_for_ineligible_rows(self):
        # just None
        concepts = _concepts_with_unit_mapping()
        exposure = _exposure(quantity=None, days_supply=None, dose_unit_source_value=None)

        assert derive_dose(exposure, concepts) is None
