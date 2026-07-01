import logging

from omop_etl.concept_mapping.core.vocabulary import (
    Vocabulary,
    _concept_row_to_mapped,  # noqa
)


def _row(concept_id: int, *, name="Malignant melanoma", domain="Condition", vocab="SNOMED", code="93655004", invalid_reason="") -> dict[str, str]:
    """One OMOP CONCEPT row (Athena export shape)."""
    return {
        "concept_id": str(concept_id),
        "concept_name": name,
        "domain_id": domain,
        "vocabulary_id": vocab,
        "concept_class_id": "Clinical Finding",
        "standard_concept": "S",
        "concept_code": code,
        "valid_start_date": "20020131",
        "valid_end_date": "20991231",
        "invalid_reason": invalid_reason,
    }


class TestConceptRowToMapped:
    def test_maps_omop_concept_columns(self):
        mapped = _concept_row_to_mapped(_row(4112853))
        assert mapped.concept_id == 4112853  # parsed to int
        assert mapped.concept_name == "Malignant melanoma"  # verbatim case
        assert mapped.domain_id == "Condition"
        assert mapped.vocabulary_id == "SNOMED"
        assert mapped.concept_code == "93655004"
        assert mapped.validity == "valid"  # empty invalid_reason

    def test_invalid_reason_collapses_to_validity(self):
        assert _concept_row_to_mapped(_row(1, invalid_reason="D")).validity == "invalid"
        assert _concept_row_to_mapped(_row(2, invalid_reason="U")).validity == "invalid"
        assert _concept_row_to_mapped(_row(3, invalid_reason="")).validity == "valid"


class TestVocabulary:
    def test_hydrate_returns_concept(self):
        vocab = Vocabulary.from_concept_rows([_row(4112853), _row(4266809, name="Fever", code="386661006")])

        assert len(vocab) == 2
        assert 4112853 in vocab
        assert (
            vocab.hydrate(4112853).concept_name == "Malignant melanoma"
        )  # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'concept_name'
        assert vocab.hydrate(4266809).concept_name == "Fever"  # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'concept_name'

    def test_hydrate_miss_returns_none(self):
        vocab = Vocabulary.from_concept_rows([_row(4112853)])

        assert 999 not in vocab
        assert vocab.hydrate(999) is None

    def test_identical_duplicate_deduped_silently(self, caplog):
        with caplog.at_level(logging.WARNING, logger="omop_etl.concept_mapping.core.vocabulary"):
            vocab = Vocabulary.from_concept_rows([_row(4112853), _row(4112853)])

        assert len(vocab) == 1
        assert caplog.records == []  # same attributes -> no warning

    def test_conflicting_duplicate_warns_and_keeps_first(self, caplog):
        with caplog.at_level(logging.WARNING, logger="omop_etl.concept_mapping.core.vocabulary"):
            vocab = Vocabulary.from_concept_rows([_row(4112853, name="Malignant melanoma"), _row(4112853, name="Something else")])

        assert len(vocab) == 1
        assert (
            vocab.hydrate(4112853).concept_name == "Malignant melanoma"
        )  # first wins # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'concept_name'
        assert any("Duplicate concept_id 4112853" in r.message for r in caplog.records)

    def test_from_csv_reads_tab_delimited(self, tmp_path):
        path = tmp_path / "concept_subset.csv"
        cols = "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason"
        path.write_text(
            cols + "\n"
            "4112853\tMalignant melanoma\tCondition\tSNOMED\tClinical Finding\tS\t93655004\t20020131\t20991231\t\n"
            "1633368\tPartial Response\tMeasurement\tSNOMED\tStaging / Scales\tS\t441513008\t20020131\t20991231\tD\n"
        )

        vocab = Vocabulary.from_csv(path)

        assert len(vocab) == 2
        assert vocab.hydrate(4112853).domain_id == "Condition"  # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'domain_id'
        assert vocab.hydrate(4112853).validity == "valid"  # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'validity'
        assert (
            vocab.hydrate(1633368).validity == "invalid"
        )  # invalid_reason=D  # fixme: Member 'None' of 'MappedConcept | None' does not have attribute 'validity'
