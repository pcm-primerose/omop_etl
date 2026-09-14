import datetime as dt
import polars as pl

from omop_etl.omop.builders.drug_era import DrugEraBuilder
from omop_etl.omop.core.id_generator import row_id
from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.omop.models.tables import OmopTables
from omop_etl.vocabulary.core.models import AthenaConcept
from omop_etl.vocabulary.core.vocabulary import Vocabulary

D = dt.date


def _exposure(person_id: int, concept_id: int, start: dt.date, end: dt.date) -> DrugExposureRow:
    return DrugExposureRow(
        drug_exposure_id=0,
        person_id=person_id,
        drug_concept_id=concept_id,
        drug_exposure_start_date=start,
        drug_exposure_end_date=end,
        drug_type_concept_id=0,
    )


def _concept(concept_id: int, domain_id: str, concept_class_id: str, name: str = "x") -> AthenaConcept:
    return AthenaConcept(
        concept_id=concept_id,
        concept_code="",
        concept_name=name,
        domain_id=domain_id,
        vocabulary_id="RxNorm",
        concept_class_id=concept_class_id,
        validity="valid",
    )


def _ancestor_links(*pairs: tuple[int, int]) -> pl.DataFrame:
    """(ancestor_concept_id, descendant_concept_id) pairs, Athena-shaped (all str)."""
    ancestors, descendants = zip(*pairs) if pairs else ((), ())
    return pl.DataFrame(
        {
            "ancestor_concept_id": [str(a) for a in ancestors],
            "descendant_concept_id": [str(d) for d in descendants],
            "min_levels_of_separation": ["1"] * len(pairs),
            "max_levels_of_separation": ["1"] * len(pairs),
        }
    )


class TestDrugEraBuilder:
    def test_single_ingredient_drug_collapses_into_one_era(self, row_id_generator):
        vocab = Vocabulary({200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 15),
                end=D(2023, 1, 20),
            ),
        ]

        eras = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)

        assert len(eras) == 1
        era = eras[0]
        assert era.person_id == 1
        assert era.drug_concept_id == 200
        assert era.drug_era_start_date == D(2023, 1, 1)
        assert era.drug_era_end_date == D(2023, 1, 20)
        assert era.drug_exposure_count == 2

    def test_gap_days_reflects_genuinely_uncovered_time(self, row_id_generator):
        # A: Jan1-Jan10 (9 exposed days), gap, B: Jan20-Jan25 (5 exposed days)
        # era span Jan1-Jan25 = 24 days, gap_days = 24 - (9+5) = 10
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 20),
                end=D(2023, 1, 25),
            ),
        ]

        era = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)[0]

        assert era.drug_exposure_count == 2
        assert era.gap_days == 10

    def test_overlapping_exposures_have_zero_gap_days(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 15),
            ),
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 10),
                end=D(2023, 1, 20),
            ),
        ]

        era = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)[0]

        assert era.gap_days == 0

    def test_gap_beyond_persistence_window_yields_separate_eras(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 1),
            ),
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 3, 1),
                end=D(2023, 3, 1),
            ),
        ]

        eras = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)

        assert len(eras) == 2

    def test_combination_drug_expands_to_one_era_per_ingredient(self, row_id_generator):
        vocab = Vocabulary(
            {
                200: _concept(concept_id=201, domain_id="Drug", concept_class_id="Ingredient", name="IngredientA"),
                201: _concept(concept_id=201, domain_id="Drug", concept_class_id="Ingredient", name="IngredientB"),
            }
        )
        ancestor = _ancestor_links((200, 100), (201, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        eras = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)

        assert {era.drug_concept_id for era in eras} == {200, 201}
        assert len(eras) == 2

    def test_non_ingredient_ancestor_is_not_used(self, row_id_generator):
        # 300 is an ancestor of 100 but isn't Ingredient-class: should be ignored
        vocab = Vocabulary(concepts={300: _concept(concept_id=300, domain_id="Drug", concept_class_id="Brand Name")})
        ancestor = _ancestor_links((300, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab) == []

    def test_unmapped_drug_concept_id_zero_is_excluded(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=0,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab) == []

    def test_no_drug_exposure_rows_yields_no_eras(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))

        assert DrugEraBuilder(row_id_generator).build([], ancestor, vocab) == []

    def test_empty_concept_ancestor_yields_no_eras(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert DrugEraBuilder(row_id_generator).build(exposures, _ancestor_links(), vocab) == []

    def test_era_id_is_deterministic_and_matches_row_id_convention(self, row_id_generator):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        era = DrugEraBuilder(row_id_generator).build(exposures, ancestor, vocab)[0]

        assert era.drug_era_id == row_id(OmopTables.DRUG_ERA, 1, 200, D(2023, 1, 1))
