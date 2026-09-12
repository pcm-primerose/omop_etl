import datetime as dt
import polars as pl

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.concept_mapping.service import ConceptLookupService
from omop_etl.omop.builders.dose_era import DoseEraBuilder
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.models import AthenaConcept
from omop_etl.vocabulary.core.vocabulary import Vocabulary

D = dt.date

MILLIGRAM_CONCEPT_ID = 8576


def _exposure(
    exposure_id: int,
    person_id: int,
    concept_id: int,
    start: dt.date,
    end: dt.date,
    *,
    quantity: float | None,
    days_supply: int | None,
    dose_unit: str | None = "mg (milligram)",
) -> DrugExposureRow:
    return DrugExposureRow(
        drug_exposure_id=exposure_id,
        person_id=person_id,
        drug_concept_id=concept_id,
        drug_exposure_start_date=start,
        drug_exposure_end_date=end,
        drug_type_concept_id=0,
        quantity=quantity,
        days_supply=days_supply,
        dose_unit_source_value=dose_unit,
    )


def _ingredient(concept_id: int, name: str = "x") -> AthenaConcept:
    return AthenaConcept(
        concept_id=concept_id,
        concept_code="",
        concept_name=name,
        domain_id="Drug",
        vocabulary_id="RxNorm",
        concept_class_id="Ingredient",
        validity="valid",
    )


def _ancestor_links(*pairs: tuple[int, int]) -> pl.DataFrame:
    ancestors, descendants = zip(*pairs) if pairs else ((), ())
    return pl.DataFrame(
        {
            "ancestor_concept_id": [str(a) for a in ancestors],
            "descendant_concept_id": [str(d) for d in descendants],
            "min_levels_of_separation": ["1"] * len(pairs),
            "max_levels_of_separation": ["1"] * len(pairs),
        }
    )


def _concepts_with_unit_mapping() -> ConceptLookupService:
    milligram = MappedConcept(
        concept_id=MILLIGRAM_CONCEPT_ID,
        concept_code="mg",
        concept_name="milligram",
        domain_id="Unit",
        vocabulary_id="UCUM",
        validity="valid",
    )
    return ConceptLookupService({("unit", "mg (milligram)"): milligram}, structural_index={})


class TestDoseEraBuilder:
    def test_single_dosed_exposure_produces_one_era(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [_exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 29), quantity=4350.0, days_supply=29)]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 1
        era = eras[0]
        assert era.person_id == 1
        assert era.drug_concept_id == 200
        assert era.unit_concept_id == MILLIGRAM_CONCEPT_ID
        assert era.dose_value == 150.0
        assert era.dose_era_start_date == D(2023, 1, 1)
        assert era.dose_era_end_date == D(2023, 1, 29)

    def test_same_dose_across_two_exposures_merges_into_one_era(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [
            _exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 10), quantity=150.0, days_supply=1),
            _exposure(2, 1, 100, D(2023, 1, 15), D(2023, 1, 20), quantity=150.0, days_supply=1),
        ]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 1
        assert eras[0].dose_value == 150.0
        assert eras[0].dose_era_start_date == D(2023, 1, 1)
        assert eras[0].dose_era_end_date == D(2023, 1, 20)

    def test_different_dose_values_form_separate_eras(self):
        # a patient could have two concurrent/overlapping dosage records at
        # different doses for the same ingredient, tracked as two independent
        # dose_value groups, not merged and not averaged
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [
            _exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 20), quantity=150.0, days_supply=1),
            _exposure(2, 1, 100, D(2023, 1, 15), D(2023, 1, 20), quantity=300.0, days_supply=1),
        ]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 2
        assert {era.dose_value for era in eras} == {150.0, 300.0}

    def test_gap_beyond_persistence_window_yields_separate_eras(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [
            _exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 1), quantity=150.0, days_supply=1),
            _exposure(2, 1, 100, D(2023, 3, 1), D(2023, 3, 1), quantity=150.0, days_supply=1),
        ]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 2

    def test_combination_drug_derives_the_same_dose_for_each_ingredient(self):
        # one exposure record, two ingredients, both get their own era, both
        # sourced from the SAME underlying quantity/days_supply/unit
        vocab = Vocabulary({200: _ingredient(200, "A"), 201: _ingredient(201, "B")})
        ancestor = _ancestor_links((200, 100), (201, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [_exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 10), quantity=150.0, days_supply=1)]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 2
        assert {era.drug_concept_id for era in eras} == {200, 201}
        assert all(era.dose_value == 150.0 for era in eras)

    def test_exposure_without_derivable_dose_is_excluded(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [_exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 10), quantity=None, days_supply=None)]

        assert DoseEraBuilder().build(exposures, ancestor, vocab, concepts) == []

    def test_only_dose_eligible_exposures_contribute(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [
            _exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 10), quantity=150.0, days_supply=1),
            _exposure(2, 1, 100, D(2023, 6, 1), D(2023, 6, 10), quantity=None, days_supply=None),
        ]

        eras = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)

        assert len(eras) == 1
        assert eras[0].dose_era_start_date == D(2023, 1, 1)

    def test_no_drug_exposure_rows_yields_no_eras(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()

        assert DoseEraBuilder().build([], ancestor, vocab, concepts) == []

    def test_dose_era_id_is_deterministic_and_matches_row_id_convention(self):
        vocab = Vocabulary({200: _ingredient(200)})
        ancestor = _ancestor_links((200, 100))
        concepts = _concepts_with_unit_mapping()
        exposures = [_exposure(1, 1, 100, D(2023, 1, 1), D(2023, 1, 29), quantity=4350.0, days_supply=29)]

        era = DoseEraBuilder().build(exposures, ancestor, vocab, concepts)[0]

        assert era.dose_era_id == row_id(OmopTables.DOSE_ERA, 1, 200, MILLIGRAM_CONCEPT_ID, 150.0, D(2023, 1, 1))
