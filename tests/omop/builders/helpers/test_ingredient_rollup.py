import datetime as dt
import polars as pl

from omop_etl.omop.builders.helpers.ingredient_rollup import ingredient_exposures
from omop_etl.omop.models.rows import DrugExposureRow
from omop_etl.vocabulary.core.models import AthenaConcept
from omop_etl.vocabulary.core.vocabulary import Vocabulary

D = dt.date


def _exposure(exposure_id: int, person_id: int, concept_id: int, start: dt.date, end: dt.date) -> DrugExposureRow:
    return DrugExposureRow(
        drug_exposure_id=exposure_id,
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


class TestIngredientExposures:
    def test_single_ingredient_drug_maps_to_one_row(self):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        result = ingredient_exposures(exposures, ancestor, vocab)

        assert result.height == 1
        row = result.row(0, named=True)
        assert row["person_id"] == 1
        assert row["ingredient_concept_id"] == 200
        assert row["drug_exposure_id"] == 1
        assert row["start"] == D(2023, 1, 1)
        assert row["end"] == D(2023, 1, 10)

    def test_combination_drug_expands_to_one_row_per_ingredient(self):
        vocab = Vocabulary(
            {
                200: _concept(
                    concept_id=200,
                    domain_id="Drug",
                    concept_class_id="Ingredient",
                    name="IngredientA",
                ),
                201: _concept(
                    concept_id=201,
                    domain_id="Drug",
                    concept_class_id="Ingredient",
                    name="IngredientB",
                ),
            }
        )
        ancestor = _ancestor_links((200, 100), (201, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        result = ingredient_exposures(exposures, ancestor, vocab)

        assert result.height == 2
        assert set(result.get_column("ingredient_concept_id").to_list()) == {200, 201}
        # both rows trace back to the same original exposure
        assert set(result.get_column("drug_exposure_id").to_list()) == {1}

    def test_non_ingredient_ancestor_is_excluded(self):
        vocab = Vocabulary(concepts={300: _concept(concept_id=300, domain_id="Drug", concept_class_id="Brand Name")})
        ancestor = _ancestor_links((300, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert ingredient_exposures(exposures, ancestor, vocab).is_empty()

    def test_non_drug_domain_ancestor_is_excluded(self):
        # Ingredient-class but wrong domain, shouldn't happen in real Athena data,
        # but the filter checks both, not just concept_class_id
        vocab = Vocabulary(concepts={300: _concept(concept_id=300, domain_id="Condition", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((300, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert ingredient_exposures(exposures, ancestor, vocab).is_empty()

    def test_unmapped_drug_concept_id_zero_is_excluded(self):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=0,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert ingredient_exposures(exposures, ancestor, vocab).is_empty()

    def test_no_exposures_returns_empty(self):
        vocab = Vocabulary(concepts={200: _concept(concept_id=200, domain_id="Drug", concept_class_id="Ingredient")})
        ancestor = _ancestor_links((200, 100))

        assert ingredient_exposures([], ancestor, vocab).is_empty()

    def test_empty_concept_ancestor_returns_empty(self):
        vocab = Vocabulary({200: _concept(200, "Drug", "Ingredient")})
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
        ]

        assert ingredient_exposures(exposures, _ancestor_links(), vocab).is_empty()

    def test_different_exposures_of_the_same_ingredient_all_appear(self):
        vocab = Vocabulary({200: _concept(200, "Drug", "Ingredient")})
        ancestor = _ancestor_links((200, 100))
        exposures = [
            _exposure(
                exposure_id=1,
                person_id=1,
                concept_id=100,
                start=D(2023, 1, 1),
                end=D(2023, 1, 10),
            ),
            _exposure(
                exposure_id=2,
                person_id=1,
                concept_id=100,
                start=D(2023, 2, 1),
                end=D(2023, 2, 10),
            ),
        ]

        result = ingredient_exposures(exposures, ancestor, vocab)

        assert result.height == 2
        assert set(result.get_column("drug_exposure_id").to_list()) == {1, 2}
