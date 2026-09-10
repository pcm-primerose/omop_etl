import logging

from omop_etl.vocabulary.core.models import AthenaConcept
from omop_etl.vocabulary.core.vocabulary import Vocabulary
from tests.vocabulary.conftest import ConceptRow


def _vocab(*rows: ConceptRow) -> Vocabulary:
    return Vocabulary.from_concept_rows(row.as_row() for row in rows)


def _hydrated(vocab: Vocabulary, concept_id: int) -> AthenaConcept:
    """hydrate() + presence assert to avoid None type warnings."""
    concept = vocab.hydrate(concept_id)
    assert concept is not None, f"expected concept {concept_id} in vocabulary"
    return concept


class TestVocabulary:
    def test_hydrates_omop_concept_attributes(self):
        vocab = _vocab(ConceptRow(4112853))

        assert vocab.hydrate(4112853) == AthenaConcept(
            concept_id=4112853,
            concept_code="93655004",
            concept_name="Malignant melanoma",
            domain_id="Condition",
            vocabulary_id="SNOMED",
            validity="valid",
        )

    def test_validity_derived_from_invalid_reason(self):
        # OMOP invalid_reason: empty = valid, else (D deprecated / U updated) = invalid
        vocab = _vocab(ConceptRow(1, invalid_reason=""), ConceptRow(2, invalid_reason="D"), ConceptRow(3, invalid_reason="U"))

        assert _hydrated(vocab, 1).validity == "valid"
        assert _hydrated(vocab, 2).validity == "invalid"
        assert _hydrated(vocab, 3).validity == "invalid"

    def test_hydrate_miss_returns_none(self):
        vocab = _vocab(ConceptRow(4112853))

        assert 999 not in vocab
        assert vocab.hydrate(999) is None

    def test_identical_duplicate_deduped_silently(self, caplog):
        with caplog.at_level(logging.WARNING, logger="omop_etl.vocabulary.core.vocabulary"):
            vocab = _vocab(ConceptRow(4112853), ConceptRow(4112853))

        assert len(vocab) == 1
        assert caplog.records == []  # identical attributes: no warning

    def test_conflicting_duplicate_warns_and_keeps_first(self, caplog):
        with caplog.at_level(logging.WARNING, logger="omop_etl.vocabulary.core.vocabulary"):
            vocab = _vocab(
                ConceptRow(4112853, concept_name="Malignant melanoma"),
                ConceptRow(4112853, concept_name="Something else"),
            )

        assert len(vocab) == 1
        assert _hydrated(vocab, 4112853).concept_name == "Malignant melanoma"  # first wins
        assert any("Duplicate concept_id 4112853" in record.message for record in caplog.records)
