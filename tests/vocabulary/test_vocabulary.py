import logging
import pytest

from omop_etl.concept_mapping.core.models import MappedConcept
from omop_etl.vocabulary.vocabulary import Vocabulary
from tests.vocabulary.conftest import (
    ConceptRow,
    write_concept_tsv,
)


def _vocab(*rows: ConceptRow) -> Vocabulary:
    return Vocabulary.from_concept_rows(row.as_row() for row in rows)


def _hydrated(vocab: Vocabulary, concept_id: int) -> MappedConcept:
    """hydrate() + presence assert to avoid None type warnings."""
    concept = vocab.hydrate(concept_id)
    assert concept is not None, f"expected concept {concept_id} in vocabulary"
    return concept


class TestVocabulary:
    def test_hydrates_omop_concept_attributes(self):
        vocab = _vocab(ConceptRow(4112853))

        assert vocab.hydrate(4112853) == MappedConcept(
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
        with caplog.at_level(logging.WARNING, logger="omop_etl.vocabulary.vocabulary"):
            vocab = _vocab(ConceptRow(4112853), ConceptRow(4112853))

        assert len(vocab) == 1
        assert caplog.records == []  # identical attributes: no warning

    def test_conflicting_duplicate_warns_and_keeps_first(self, caplog):
        with caplog.at_level(logging.WARNING, logger="omop_etl.vocabulary.vocabulary"):
            vocab = _vocab(
                ConceptRow(4112853, concept_name="Malignant melanoma"),
                ConceptRow(4112853, concept_name="Something else"),
            )

        assert len(vocab) == 1
        assert _hydrated(vocab, 4112853).concept_name == "Malignant melanoma"  # first wins
        assert any("Duplicate concept_id 4112853" in record.message for record in caplog.records)


class TestFromCsv:
    def test_reads_tab_delimited_concept_file(self, tmp_path):
        path = write_concept_tsv(
            tmp_path / "concept_subset.tsv",
            ConceptRow(4112853),
            ConceptRow(1633368, concept_name="Partial Response", domain_id="Measurement", concept_code="441513008", invalid_reason="D"),
        )

        vocab = Vocabulary.from_csv(path)

        assert len(vocab) == 2
        assert _hydrated(vocab, 4112853).domain_id == "Condition"
        assert _hydrated(vocab, 1633368).validity == "invalid"

    def test_missing_required_columns_raises(self, tmp_path):
        # header validation rejects a file lacking the OMOP CONCEPT columns
        path = tmp_path / "bad.tsv"
        path.write_text("concept_id\tconcept_name\n4112853\tMelanoma\n")

        with pytest.raises(ValueError, match="missing required columns"):
            Vocabulary.from_csv(path)
